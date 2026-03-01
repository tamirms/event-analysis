package eventstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"sync"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/record"
	"github.com/tamir/events-analysis/zstd"
)

const DefaultBlockSize = 128

// WriterOptions configures how the eventstore is written.
type WriterOptions struct {
	BlockSize     int  // events per block; 0 defaults to DefaultBlockSize (128)
	Concurrency   int  // compression goroutines; 0 or 1 = serial
	NoCompression bool // skip zstd; use CRC32C integrity instead (for benchmarking raw I/O)
	ContentHash   bool // compute SHA-256 content hash over logical event stream
}

type blockResult struct {
	blockID   uint32
	data      []byte   // uncompressed block → compressed block
	hashSizes []uint32 // event sizes for hash goroutine (reads events from data)
	digest    [sha256.Size]byte
	hasHash   bool
	err       error
}

type hashWork struct {
	data      []byte
	hashSizes []uint32
}

type Writer struct {
	pw         *packfile.Writer
	buf        []byte           // accumulates raw events for current block
	sizes      []uint32         // event sizes in current block
	total      int              // total events written
	blockN     int              // events per block
	closed     bool             // set by Finish or Abort
	err        error            // sticky — once set, all subsequent ops fail
	noCompress bool             // skip zstd compression
	compressor *zstd.Compressor // for serial flush path

	// Content hash (opt-in).
	hasher        hash.Hash // final aggregation hasher; nil when disabled
	serialHashBuf []byte    // reusable buffer for serial path hash chunk
	sizesPool     sync.Pool // recycled []uint32 buffers for hashSizes

	// Streaming compression pipeline (concurrency > 1).
	concurrency int
	nextBlockID uint32
	workCh      chan blockResult  // blockID + uncompressed → compress workers
	resultCh    chan blockResult  // blockID + compressed → writer goroutine
	writerDone  chan error       // writer signals completion (buffered, size 1)
}

func (w *Writer) getSizes() []uint32 {
	if p := w.sizesPool.Get(); p != nil {
		return p.([]uint32)[:w.blockN]
	}
	return make([]uint32, w.blockN)
}

func (w *Writer) putSizes(s []uint32) { w.sizesPool.Put(s) }

// Create starts writing a new eventstore at path.
func Create(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.BlockSize < 0 {
		return nil, errors.New("eventstore: BlockSize must be > 0")
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		BytesPerSync: 1 << 20, // 1MB — initiate background writeback to overlap I/O with compression
	})
	if err != nil {
		return nil, err
	}
	conc := opts.Concurrency
	if opts.NoCompression {
		conc = 0 // no pipeline when compression is disabled
	}
	w := &Writer{
		pw:          pw,
		blockN:      opts.BlockSize,
		concurrency: conc,
		noCompress:  opts.NoCompression,
	}
	if opts.ContentHash {
		w.hasher = sha256.New()
	}
	if w.concurrency > 1 {
		w.workCh = make(chan blockResult, w.concurrency)
		w.resultCh = make(chan blockResult, w.concurrency)
		w.writerDone = make(chan error, 1)

		var compressWg sync.WaitGroup
		for range w.concurrency {
			compressWg.Go(func() {
				w.compressWorker()
			})
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
// When content hashing is enabled, a persistent goroutine builds a
// length-prefixed buffer from block data and computes a single SHA-256
// digest per chunk, overlapping with CGo zstd compression.
func (w *Writer) compressWorker() {
	c := zstd.NewCompressor()
	defer c.Close()

	var hashIn chan hashWork
	var hashOut chan [sha256.Size]byte
	if w.hasher != nil {
		hashIn = make(chan hashWork, 1)
		hashOut = make(chan [sha256.Size]byte, 1)
		go func() {
			var lenBuf [4]byte
			var hashBuf []byte
			for hw := range hashIn {
				// Build [4B len][ev₀]...[4B len][evₙ] from block data + sizes.
			// Same hash format as record.ContentHasher, operating on the flat
			// buffer directly. Cross-validated by TestContentHashWithConcurrency.
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
	}

	for work := range w.workCh {
		// Send to persistent hash goroutine. It reads work.data
		// concurrently with c.Encode — both are reads, no race.
		if work.hashSizes != nil {
			hashIn <- hashWork{data: work.data, hashSizes: work.hashSizes}
		}

		compressed, err := c.Encode(work.data)
		if err != nil {
			if work.hashSizes != nil {
				<-hashOut
			}
			w.resultCh <- blockResult{blockID: work.blockID, err: fmt.Errorf("eventstore: compress block %d: %w", work.blockID, err)}
			return
		}

		// Wait for hash goroutine before overwriting work.data.
		if work.hashSizes != nil {
			work.digest = <-hashOut
			work.hasHash = true
			work.hashSizes = nil
		}
		work.data = append(work.data[:0], compressed...)
		w.resultCh <- work
	}
	if hashIn != nil {
		close(hashIn)
	}
}

// runWriter receives compressed blocks and writes them in blockID order
// using a reorder buffer. Chunk digests are forwarded to a dedicated
// aggregation goroutine to keep the writer's critical path free.
func (w *Writer) runWriter() {
	defer close(w.writerDone)

	var hashCh chan [sha256.Size]byte
	var hashDone chan struct{}
	if w.hasher != nil {
		hashCh = make(chan [sha256.Size]byte, w.concurrency)
		hashDone = make(chan struct{})
		go func() {
			defer close(hashDone)
			for d := range hashCh {
				w.hasher.Write(d[:])
			}
		}()
	}

	pending := make(map[uint32]blockResult)
	nextBlockID := uint32(0)

	for result := range w.resultCh {
		if result.err != nil {
			if hashCh != nil {
				close(hashCh)
				<-hashDone
			}
			w.writerDone <- result.err
			for range w.resultCh {
			}
			return
		}
		pending[result.blockID] = result

		for br, ok := pending[nextBlockID]; ok; br, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)
			if err := w.pw.Append(br.data); err != nil {
				if hashCh != nil {
					close(hashCh)
					<-hashDone
				}
				w.writerDone <- err
				for range w.resultCh {
				}
				return
			}
			if br.hasHash {
				hashCh <- br.digest
			}
			nextBlockID++
		}
	}

	if hashCh != nil {
		close(hashCh)
		<-hashDone
	}
}

// Append adds a single event. Flushes a block when blockN events accumulate.
func (w *Writer) Append(event []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("eventstore: writer is closed")
	}
	w.buf = append(w.buf, event...)
	w.sizes = append(w.sizes, uint32(len(event)))

	if len(w.sizes) == w.blockN {
		if err := w.flush(); err != nil {
			w.err = err
			return err
		}
	}
	w.total++
	return nil
}

// buildBlock assembles the current event buffer into an uncompressed block.
func (w *Writer) buildBlock() []byte {
	trailing := intpack.EncodeTrailingGroup(w.sizes)
	block := make([]byte, len(w.buf)+len(trailing))
	copy(block, w.buf)
	copy(block[len(w.buf):], trailing)
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return block
}

func (w *Writer) flush() error {
	// Serial path: hash + compress inline.
	if w.concurrency <= 1 {
		if w.hasher != nil {
			// Compute chunk digest over events in w.buf. Same [4B len][entry]...
			// hash format as record.ContentHasher, operating on the flat buffer
			// directly. Cross-validated by TestContentHashWithConcurrency.
			hashBuf := w.serialHashBuf[:0]
			var lenBuf [4]byte
			offset := 0
			for _, size := range w.sizes {
				binary.LittleEndian.PutUint32(lenBuf[:], size)
				hashBuf = append(hashBuf, lenBuf[:]...)
				hashBuf = append(hashBuf, w.buf[offset:offset+int(size)]...)
				offset += int(size)
			}
			digest := sha256.Sum256(hashBuf)
			w.hasher.Write(digest[:])
			w.serialHashBuf = hashBuf
		}

		block := w.buildBlock()
		if w.noCompress {
			block = binary.LittleEndian.AppendUint32(block, packfile.CRC32C(block))
		} else {
			if w.compressor == nil {
				w.compressor = zstd.NewCompressor()
			}
			var err error
			block, err = w.compressor.Encode(block)
			if err != nil {
				return err
			}
		}
		return w.pw.Append(block)
	}

	// Concurrent path: copy sizes (small), hash goroutine reads events
	// directly from block data.
	var hashSizes []uint32
	if w.hasher != nil {
		hashSizes = w.getSizes()[:len(w.sizes)]
		copy(hashSizes, w.sizes)
	}

	block := w.buildBlock()

	w.workCh <- blockResult{
		blockID:   w.nextBlockID,
		data:      block,
		hashSizes: hashSizes,
	}
	w.nextBlockID++
	return nil
}

// Finish flushes any partial block, drains the pipeline, writes metadata,
// and finalizes the packfile.
func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return errors.New("eventstore: writer is closed")
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
		w.err = fmt.Errorf("eventstore: event count %d exceeds uint32 max", w.total)
		return w.err
	}

	var flags uint32
	if w.noCompress {
		flags |= record.FlagNoCompression
	}
	if w.hasher != nil {
		flags |= record.FlagContentHash
	}
	meta := record.EncodeMetadata(w.total, w.blockN, flags)
	if w.hasher != nil {
		meta = append(meta, w.hasher.Sum(nil)...)
	}

	if w.compressor != nil {
		w.compressor.Close()
		w.compressor = nil
	}

	_, err := w.pw.Finish(meta)
	if err != nil {
		w.err = errors.Join(err, w.pw.Abort())
		return w.err
	}
	w.closed = true
	return nil
}

// Abort discards the in-progress eventstore.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.compressor != nil {
		w.compressor.Close()
		w.compressor = nil
	}
	if w.workCh != nil {
		close(w.workCh)
		<-w.writerDone
	}
	return w.pw.Abort()
}
