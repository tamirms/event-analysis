package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/zstd"
)

const defaultItemsPerRecord = 128

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	// ItemsPerRecord is the number of items per record. 0 defaults to 128.
	ItemsPerRecord int

	// Format controls record encoding. Default (zero value) is Compressed.
	// Compressed: zstd with built-in integrity.
	// Uncompressed: raw records with CRC32C integrity.
	// Raw: raw records with no integrity wrapper.
	Format RecordFormat

	// Concurrency sets the number of parallel compression goroutines.
	// 0 or 1 means serial.
	Concurrency int

	// ContentHash enables SHA-256 content hashing over the logical item stream.
	ContentHash bool

	// BytesPerSync initiates background writeback of dirty pages every N bytes
	// written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
	// is non-blocking — it tells the kernel to start flushing without waiting.
	// This spreads I/O across the write phase so the final fdatasync in Finish()
	// has less data to flush. 0 disables (default).
	BytesPerSync int

	// Overwrite allows Create to replace an existing file at the path.
	// When false (default), Create fails if the file already exists.
	// When true, any existing file is removed before creation.
	Overwrite bool
}

type blockResult struct {
	blockID   uint32
	data      []byte   // payload → format-processed payload
	forIndex  []byte   // FOR index: [packed][1B W][4B min][4B CRC32C]; nil when itemsPerRecord==1
	hashSizes []uint32 // item sizes for hash goroutine
	digest    [sha256.Size]byte
	hasHash   bool
	err       error
}

type hashWork struct {
	data      []byte
	hashSizes []uint32
}

// Writer creates a new packfile with item-level semantics.
// Items are accumulated into records of itemsPerRecord items each,
// format-processed (compressed/CRC/raw), and written with an offset index.
type Writer struct {
	// File I/O
	file *os.File
	path string
	pos  int64
	offsets      []int64
	bytesPerSync int64
	lastSyncPos  int64

	// Record accumulation
	buf        []byte
	sizes      []uint32
	total      int
	itemsPerRecord int
	format     RecordFormat
	compressor *zstd.Compressor

	// Content hash
	contentHash  bool           // whether content hashing is enabled
	serialHasher *contentHasher // serial path: streams items through contentHasher
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
		return p.([]uint32)[:w.itemsPerRecord]
	}
	return make([]uint32, w.itemsPerRecord)
}

func (w *Writer) putSizes(s []uint32) { w.sizesPool.Put(s) }

// resolveItemsPerRecord returns the effective record size from opts, defaulting
// to 128 if zero. Returns an error if negative.
func resolveItemsPerRecord(opts WriterOptions) (int, error) {
	rs := opts.ItemsPerRecord
	if rs == 0 {
		return defaultItemsPerRecord, nil
	}
	if rs < 0 {
		return 0, errors.New("packfile: ItemsPerRecord must be > 0")
	}
	return rs, nil
}

// Create starts writing a new packfile at path. By default, fails if the
// file already exists. Set Overwrite to replace an existing file.
func Create(path string, opts WriterOptions) (*Writer, error) {
	itemsPerRecord, err := resolveItemsPerRecord(opts)
	if err != nil {
		return nil, err
	}
	conc := opts.Concurrency
	if opts.Format != Compressed && !opts.ContentHash {
		conc = 0 // no pipeline when nothing to parallelize
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if opts.Overwrite {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		file:         f,
		path:         path,
		itemsPerRecord:   itemsPerRecord,
		concurrency:  conc,
		format:       opts.Format,
		contentHash:  opts.ContentHash,
		bytesPerSync: int64(opts.BytesPerSync),
	}

	if opts.ContentHash && w.concurrency <= 1 {
		w.serialHasher = newContentHasher(itemsPerRecord)
	}

	if w.concurrency > 1 {
		w.workCh = make(chan blockResult, w.concurrency)
		w.resultCh = make(chan blockResult, w.concurrency)
		w.writerDone = make(chan error, 1)

		var blockWg sync.WaitGroup
		for range w.concurrency {
			blockWg.Go(w.blockWorker)
		}
		go func() {
			blockWg.Wait()
			close(w.resultCh)
		}()
		go w.runWriter()
	}

	return w, nil
}

// blockWorker reads blocks from workCh, performs format-specific processing
// (compression/CRC) and optional content hashing, then sends results to
// resultCh preserving blockID for reordering.
func (w *Writer) blockWorker() {
	var c *zstd.Compressor
	if w.format == Compressed {
		c = zstd.NewCompressor()
		defer c.Close()
	}

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
		// Phase 1: queue hash work (hash goroutine reads work.data concurrently).
		if work.hashSizes != nil {
			hashIn <- hashWork{data: work.data, hashSizes: work.hashSizes}
		}

		// Phase 2: format processing (read-only on work.data, concurrent with hash).
		var compressed []byte
		var crc uint32
		var fmtErr error
		switch w.format {
		case Compressed:
			compressed, fmtErr = c.Encode(work.data)
		case Uncompressed:
			crc = CRC32C(work.data)
		}

		// Phase 3: collect hash (hash goroutine done reading work.data).
		if work.hashSizes != nil {
			if fmtErr != nil {
				<-hashOut
			} else {
				work.digest = <-hashOut
				work.hasHash = true
			}
			work.hashSizes = nil
		}
		if fmtErr != nil {
			w.resultCh <- blockResult{blockID: work.blockID, err: fmt.Errorf("packfile: block %d: %w", work.blockID, fmtErr)}
			return
		}

		// Assemble: format-specific payload + uncompressed FOR index.
		switch w.format {
		case Compressed:
			work.data = append(append(work.data[:0], compressed...), work.forIndex...)
		case Uncompressed:
			work.data = binary.LittleEndian.AppendUint32(work.data, crc)
			work.data = append(work.data, work.forIndex...)
		default: // Raw
			work.data = append(work.data, work.forIndex...)
		}
		work.forIndex = nil
		w.resultCh <- work
	}
}

// runWriter receives processed blocks and writes them in blockID order.
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

// AppendItem adds a single item. If multiple byte slices are passed,
// they are concatenated into one item.
// Flushes a record when ItemsPerRecord items accumulate.
func (w *Writer) AppendItem(parts ...[]byte) error {
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

	if len(w.sizes) == w.itemsPerRecord {
		if err := w.flush(); err != nil {
			w.err = err
			return err
		}
	}
	w.total++
	return nil
}

// writeBlock appends a format-processed record to the file,
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

// buildBlock extracts the current payload buffer and (for itemsPerRecord>1) encodes
// the FOR index. Returns them separately: the FOR index is never compressed and
// always carries its own CRC32C. Payload is allocated with spare capacity for
// CRC (4B) + forIndex so callers can append without reallocation.
func (w *Writer) buildBlock() (payload []byte, forIndex []byte) {
	if w.itemsPerRecord > 1 {
		encoded := intpack.EncodeGroup(w.sizes)
		forIndex = binary.LittleEndian.AppendUint32(encoded, CRC32C(encoded))
	}
	payload = make([]byte, len(w.buf), len(w.buf)+4+len(forIndex))
	copy(payload, w.buf)
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return
}

func (w *Writer) flush() error {
	// Serial path: format-process inline. Content hash is handled by serialHasher in AppendItem.
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
			block = append(append(payload[:0], compressed...), forIndex...)
		case Uncompressed:
			// CRC_items covers payload only; FOR index has its own CRC.
			payload = binary.LittleEndian.AppendUint32(payload, CRC32C(payload))
			block = append(payload, forIndex...)
		case Raw:
			block = append(payload, forIndex...)
		}
		return w.writeBlock(block)
	}

	// Concurrent path: blockWorker handles all formats.
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

	// Drain the block-processing pipeline.
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
	binary.LittleEndian.PutUint32(trailer[14:], uint32(w.itemsPerRecord))     // itemsPerRecord
	binary.LittleEndian.PutUint32(trailer[18:], indexSize)                // indexSize
	binary.LittleEndian.PutUint32(trailer[22:], appDataSize)              // appDataSize
	copy(trailer[26:58], hash[:])                                         // contentHash (zeroed if unused)
	binary.LittleEndian.PutUint16(trailer[58:], uint16(groupSize))        // indexForGroupSize

	// CRC32C over trailer[0:60] only. App data integrity is the caller's responsibility.
	binary.LittleEndian.PutUint32(trailer[60:], CRC32C(trailer[:60]))

	if _, err := w.file.Write(trailer[:]); err != nil {
		w.err = err
		return err
	}

	if err := w.file.Sync(); err != nil {
		w.err = err
		return err
	}
	if err := w.file.Close(); err != nil {
		w.err = err
		return err
	}
	w.fileClosed = true

	w.closeCompressor()

	w.closed = true
	return nil
}

// Close releases resources. If Finish was not called, the incomplete file
// is removed. If Finish was called, Close is a no-op (the file remains as
// a valid packfile). Safe to call multiple times. Idiomatic usage:
//
//	w, _ := Create(path, opts)
//	defer w.Close()
//	// ... AppendItem ...
//	return w.Finish(nil)
func (w *Writer) Close() error {
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
	// Finish was never called — remove the incomplete file.
	removeErr := os.Remove(w.path)
	return errors.Join(closeErr, removeErr)
}
