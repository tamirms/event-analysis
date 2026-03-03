package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/zstd"
)

const defaultRecordSize = 128

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	// RecordSize is the number of items per record. 0 defaults to 128.
	RecordSize int

	// Format controls record encoding. Default (zero value) is Compressed.
	// Compressed: zstd with built-in integrity.
	// Uncompressed: raw records with CRC32C integrity.
	// Raw: raw records with no integrity wrapper.
	Format RecordFormat

	// Concurrency sets the number of compression goroutines.
	// 0 or 1 means serial. Ignored when Format is not Compressed.
	Concurrency int

	// ContentHash enables SHA-256 content hashing over the logical item stream.
	ContentHash bool

	// BytesPerSync initiates background writeback of dirty pages every N bytes
	// written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
	// is non-blocking — it tells the kernel to start flushing without waiting.
	// This spreads I/O across the write phase so the final fdatasync in Finish()
	// has less data to flush. 0 disables (default).
	BytesPerSync int
}

type blockResult struct {
	blockID   uint32
	data      []byte   // payload → compressed payload
	forIndex  []byte   // FOR index: [packed][1B W][4B min][4B CRC32C]; nil when recordSize==1
	hashSizes []uint32 // entry sizes for hash goroutine
	digest    [sha256.Size]byte
	hasHash   bool
	err       error
}

type hashWork struct {
	data      []byte
	hashSizes []uint32
}

// Writer creates a new packfile with item-level semantics.
// Items are accumulated into records of recordSize items each,
// compressed (optionally), and written with an offset index.
type Writer struct {
	// File I/O
	file         *os.File
	path         string // final path
	tmpPath      string // {path}.tmp.{random}
	pos          int64
	offsets      []int64
	bytesPerSync int64
	lastSyncPos  int64

	// Record accumulation
	buf        []byte
	sizes      []uint32
	total      int
	recordSize int
	format     RecordFormat
	compressor *zstd.Compressor

	// Content hash
	contentHash  bool           // whether content hashing is enabled
	serialHasher *contentHasher // serial path: streams entries through contentHasher
	digests      []byte         // concurrent path: accumulated 32-byte chunk digests
	sizesPool    sync.Pool      // concurrent path: pooled []uint32 for hash goroutines

	// Pipeline (concurrency > 1)
	concurrency int
	nextBlockID uint32
	workCh      chan blockResult
	resultCh    chan blockResult
	writerDone  chan error

	err        error
	closed     bool
	fileClosed bool
}

func (w *Writer) getSizes() []uint32 {
	if p := w.sizesPool.Get(); p != nil {
		return p.([]uint32)[:w.recordSize]
	}
	return make([]uint32, w.recordSize)
}

func (w *Writer) putSizes(s []uint32) { w.sizesPool.Put(s) }

// Create starts writing a new packfile at path.
// The file is not visible at path until Finish is called.
func Create(path string, opts WriterOptions) (*Writer, error) {
	recordSize := opts.RecordSize
	if recordSize == 0 {
		recordSize = defaultRecordSize
	}
	if recordSize < 0 {
		return nil, errors.New("packfile: RecordSize must be > 0")
	}
	conc := opts.Concurrency
	if opts.Format != Compressed {
		conc = 0 // no pipeline when compression is disabled
	}

	tmpPath := path + ".tmp." + strconv.FormatInt(rand.Int63(), 10)
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		file:         f,
		path:         path,
		tmpPath:      tmpPath,
		recordSize:   recordSize,
		concurrency:  conc,
		format:       opts.Format,
		contentHash:  opts.ContentHash,
		bytesPerSync: int64(opts.BytesPerSync),
	}

	if opts.ContentHash && w.concurrency <= 1 {
		w.serialHasher = newContentHasher(recordSize)
	}

	if w.concurrency > 1 {
		w.workCh = make(chan blockResult, w.concurrency)
		w.resultCh = make(chan blockResult, w.concurrency)
		w.writerDone = make(chan error, 1)

		var compressWg sync.WaitGroup
		for range w.concurrency {
			compressWg.Go(w.compressWorker)
		}
		go func() {
			compressWg.Wait()
			close(w.resultCh)
		}()
		go w.runWriter()
	}

	return w, nil
}

// compressWorker reads uncompressed blocks from workCh and sends compressed
// results to resultCh, preserving the blockID for reordering.
func (w *Writer) compressWorker() {
	c := zstd.NewCompressor()
	defer c.Close()

	var hashIn chan hashWork
	var hashOut chan [sha256.Size]byte
	if w.contentHash {
		hashIn = make(chan hashWork, 1)
		hashOut = make(chan [sha256.Size]byte, 1)
		go func() {
			var lenBuf [4]byte
			var hashBuf []byte
			for hw := range hashIn {
				hashBuf = hashBuf[:0]
				offset := 0
				for _, size := range hw.hashSizes {
					binary.LittleEndian.PutUint32(lenBuf[:], size)
					hashBuf = append(hashBuf, lenBuf[:]...)
					hashBuf = append(hashBuf, hw.data[offset:offset+int(size)]...)
					offset += int(size)
				}
				w.putSizes(hw.hashSizes)
				hashOut <- sha256.Sum256(hashBuf)
			}
		}()
		defer close(hashIn)
	}

	for work := range w.workCh {
		if work.hashSizes != nil {
			hashIn <- hashWork{data: work.data, hashSizes: work.hashSizes}
		}

		// Compress payload only; the FOR index is never compressed.
		compressed, err := c.Encode(work.data)
		if err != nil {
			if work.hashSizes != nil {
				<-hashOut
			}
			w.resultCh <- blockResult{blockID: work.blockID, err: fmt.Errorf("packfile: compress block %d: %w", work.blockID, err)}
			return
		}

		if work.hashSizes != nil {
			work.digest = <-hashOut
			work.hasHash = true
			work.hashSizes = nil
		}
		// Assemble: compressed payload + uncompressed FOR index.
		work.data = append(append(work.data[:0], compressed...), work.forIndex...)
		work.forIndex = nil
		w.resultCh <- work
	}
}

// runWriter receives compressed blocks and writes them in blockID order.
func (w *Writer) runWriter() {
	defer close(w.writerDone)

	pending := make(map[uint32]blockResult)
	nextBlockID := uint32(0)

	for result := range w.resultCh {
		if result.err != nil {
			w.writerDone <- result.err
			for range w.resultCh {
			}
			return
		}
		pending[result.blockID] = result

		for br, ok := pending[nextBlockID]; ok; br, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)
			if err := w.writeBlock(br.data); err != nil {
				w.writerDone <- err
				for range w.resultCh {
				}
				return
			}
			if br.hasHash {
				w.digests = append(w.digests, br.digest[:]...)
			}
			nextBlockID++
		}
	}
}

// Append adds a single logical item. Parts are concatenated as one entry.
// Flushes a record when recordSize items accumulate.
func (w *Writer) Append(parts ...[]byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("packfile: writer is closed")
	}

	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if total == 0 && len(parts) == 0 {
		return nil
	}

	if w.serialHasher != nil {
		w.serialHasher.Add(parts...)
	}

	for _, p := range parts {
		w.buf = append(w.buf, p...)
	}
	w.sizes = append(w.sizes, uint32(total))

	if len(w.sizes) == w.recordSize {
		if err := w.flush(); err != nil {
			w.err = err
			return err
		}
	}
	w.total++
	return nil
}

// writeBlock appends a compressed (or uncompressed) record to the file,
// updates offsets/pos, and initiates background writeback if configured.
func (w *Writer) writeBlock(data []byte) error {
	w.offsets = append(w.offsets, w.pos)
	n, err := w.file.Write(data)
	w.pos += int64(n)
	if err != nil {
		w.err = err
		return err
	}
	if w.bytesPerSync > 0 && w.pos-w.lastSyncPos >= w.bytesPerSync {
		initiateWriteback(w.file, w.lastSyncPos, w.pos-w.lastSyncPos)
		w.lastSyncPos = w.pos
	}
	return nil
}

// closeCompressor releases the serial-path compressor, if allocated.
func (w *Writer) closeCompressor() {
	if w.compressor != nil {
		w.compressor.Close()
		w.compressor = nil
	}
}

// buildBlock extracts the current payload buffer and (for recordSize>1) encodes
// the FOR index. Returns them separately: the FOR index is never compressed and
// always carries its own CRC32C.
func (w *Writer) buildBlock() (payload []byte, forIndex []byte) {
	payload = make([]byte, len(w.buf))
	copy(payload, w.buf)
	w.buf = w.buf[:0]
	if w.recordSize > 1 {
		encoded := intpack.EncodeGroup(w.sizes)
		forIndex = binary.LittleEndian.AppendUint32(encoded, CRC32C(encoded))
	}
	w.sizes = w.sizes[:0]
	return
}

func (w *Writer) flush() error {
	// Serial path: compress inline. Content hash is handled by serialHasher in Append.
	if w.concurrency <= 1 {
		payload, forIndex := w.buildBlock()
		var block []byte
		switch w.format {
		case Compressed:
			if w.compressor == nil {
				w.compressor = zstd.NewCompressor()
			}
			compressed, err := w.compressor.Encode(payload)
			if err != nil {
				return err
			}
			block = append(compressed, forIndex...)
		case Uncompressed:
			// CRC_items covers payload only; FOR index has its own CRC.
			payload = binary.LittleEndian.AppendUint32(payload, CRC32C(payload))
			block = append(payload, forIndex...)
		case Raw:
			block = append(payload, forIndex...)
		}
		return w.writeBlock(block)
	}

	// Concurrent path (Compressed-only: Create sets conc=0 for other formats).
	var hashSizes []uint32
	if w.contentHash {
		hashSizes = w.getSizes()[:len(w.sizes)]
		copy(hashSizes, w.sizes)
	}

	payload, forIndex := w.buildBlock()

	w.workCh <- blockResult{
		blockID:   w.nextBlockID,
		data:      payload,
		forIndex:  forIndex,
		hashSizes: hashSizes,
	}
	w.nextBlockID++
	return nil
}

// Finish flushes any partial record, drains the pipeline, writes the index,
// optional app data, and 64-byte trailer, and finalizes the packfile.
// appData is optional caller-injected data stored between the index and
// trailer; pass nil for no app data.
func (w *Writer) Finish(appData []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("packfile: writer is closed")
	}
	if len(w.sizes) > 0 {
		if err := w.flush(); err != nil {
			w.err = err
			return err
		}
	}

	// Drain the streaming compression pipeline.
	if w.workCh != nil {
		close(w.workCh)
		err := <-w.writerDone
		w.workCh = nil
		if err != nil {
			w.err = err
			return err
		}
	}

	if w.total > math.MaxUint32 {
		w.err = fmt.Errorf("packfile: item count %d exceeds uint32 max", w.total)
		return w.err
	}

	w.offsets = append(w.offsets, w.pos) // end-of-data offset

	// Compute content hash if enabled.
	var hash [32]byte
	if w.contentHash {
		if w.serialHasher != nil {
			hash = w.serialHasher.Sum()
		} else {
			hash = sha256.Sum256(w.digests)
		}
	}

	// Encode index using FOR-128.
	indexBytes, err := encodeIndex(w.offsets)
	if err != nil {
		w.err = err
		return err
	}
	indexSize := uint32(len(indexBytes))

	// Write index section.
	if _, err := w.file.Write(indexBytes); err != nil {
		w.err = err
		return err
	}

	// Write app data (if any).
	appDataSize := uint32(len(appData))
	if appDataSize > 0 {
		if _, err := w.file.Write(appData); err != nil {
			w.err = err
			return err
		}
	}

	// Build 64-byte trailer.
	var flags uint8
	if w.format != Compressed {
		flags |= flagNoCompression
	}
	if w.format == Raw {
		flags |= flagNoCRC
	}
	if w.contentHash {
		flags |= flagContentHash
	}

	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:], magic)
	trailer[4] = version
	trailer[5] = flags
	binary.LittleEndian.PutUint32(trailer[6:], uint32(len(w.offsets)-1))  // recordCount
	binary.LittleEndian.PutUint32(trailer[10:], uint32(w.total))          // totalItems
	binary.LittleEndian.PutUint32(trailer[14:], uint32(w.recordSize))     // recordSize
	binary.LittleEndian.PutUint32(trailer[18:], indexSize)                // indexSize
	binary.LittleEndian.PutUint32(trailer[22:], appDataSize)              // appDataSize
	copy(trailer[26:58], hash[:])                                         // contentHash (zeroed if unused)
	// trailer[58:60] reserved (zero)

	// CRC32C over trailer[0:60] only. App data integrity is the caller's responsibility.
	binary.LittleEndian.PutUint32(trailer[60:], CRC32C(trailer[:60]))

	if _, err := w.file.Write(trailer[:]); err != nil {
		w.err = err
		return err
	}

	// Fsync and atomic rename.
	if err := w.file.Sync(); err != nil {
		w.err = err
		return err
	}
	if err := w.file.Close(); err != nil {
		w.err = err
		return err
	}
	w.fileClosed = true
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		w.err = err
		return err
	}

	// Fsync parent directory to ensure the rename is durable.
	if dir, err := os.Open(filepath.Dir(w.path)); err == nil {
		syncErr := dir.Sync()
		closeErr := dir.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			w.err = err
			return err
		}
	}

	w.closeCompressor()

	w.closed = true
	return nil
}

// Abort discards the in-progress packfile and removes the temp file.
// Safe to call after a failed Finish to clean up.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.closeCompressor()
	if w.workCh != nil {
		close(w.workCh)
		<-w.writerDone
	}
	var closeErr error
	if !w.fileClosed {
		closeErr = w.file.Close()
	}
	removeErr := os.Remove(w.tmpPath)
	return errors.Join(closeErr, removeErr)
}
