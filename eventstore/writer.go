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
	blockID uint32
	data    []byte
	err     error // set by compressWorker on failure
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

	// Content hash (opt-in). For concurrent writes, a dedicated goroutine
	// hashes in parallel with compression.
	hasher   hash.Hash   // nil when content hashing is disabled; SHA-256
	hashBuf  []byte      // [u32 len][event]... for current block
	hashCh   chan []byte  // hash goroutine input (concurrent path only)
	hashDone chan struct{} // signals hash goroutine completion

	// Streaming compression pipeline (concurrency > 1).
	concurrency int
	nextBlockID uint32
	workCh      chan blockResult // blockID + uncompressed → compress workers
	resultCh    chan blockResult // blockID + compressed → writer goroutine
	writerDone  chan error       // writer signals completion (buffered, size 1)
}

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
		if w.concurrency > 1 {
			w.hashCh = make(chan []byte, 256)
			w.hashDone = make(chan struct{})
			go w.hashWorker()
		}
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
		// Close resultCh when all compressors finish, signaling the writer to drain and exit.
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
	for work := range w.workCh {
		compressed, err := c.Encode(work.data)
		if err != nil {
			w.resultCh <- blockResult{blockID: work.blockID, err: fmt.Errorf("eventstore: compress block %d: %w", work.blockID, err)}
			return
		}
		// Copy: compressed aliases c's scratch buffer, but must outlive
		// until the writer goroutine consumes it.
		work.data = append(work.data[:0], compressed...)
		w.resultCh <- work
	}
}

// runWriter receives compressed blocks and writes them in blockID order
// using a reorder buffer.
func (w *Writer) runWriter() {
	defer close(w.writerDone)

	pending := make(map[uint32][]byte)
	nextBlockID := uint32(0)

	for result := range w.resultCh {
		if result.err != nil {
			w.writerDone <- result.err
			// Drain remaining results so compressors don't block.
			for range w.resultCh {
			}
			return
		}
		pending[result.blockID] = result.data

		// Drain all consecutive ready blocks in order.
		for data, ok := pending[nextBlockID]; ok; data, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)
			if err := w.pw.Append(data); err != nil {
				w.writerDone <- err
				// Drain remaining results so compressors don't block.
				for range w.resultCh {
				}
				return
			}
			nextBlockID++
		}
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
		// Hash directly from w.buf + w.sizes (zero alloc).
		if w.hasher != nil {
			var lenBuf [4]byte
			offset := 0
			for _, size := range w.sizes {
				record.HashEntry(w.hasher, &lenBuf, w.buf[offset:offset+int(size)])
				offset += int(size)
			}
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

	// Concurrent path: build hash input before buildBlock resets w.buf/w.sizes.
	var hashBuf []byte
	if w.hashCh != nil {
		hashBuf = w.hashBuf[:0]
		offset := 0
		for _, size := range w.sizes {
			hashBuf = binary.LittleEndian.AppendUint32(hashBuf, size)
			hashBuf = append(hashBuf, w.buf[offset:offset+int(size)]...)
			offset += int(size)
		}
		w.hashBuf = make([]byte, 0, cap(hashBuf))
	}

	block := w.buildBlock()

	// Feed compression first — this is the throughput-critical pipeline.
	w.workCh <- blockResult{blockID: w.nextBlockID, data: block}
	w.nextBlockID++

	// Send to hash goroutine. Deep hashCh buffer (256) prevents this from
	// blocking the compression pipeline even when the hash goroutine lags.
	if hashBuf != nil {
		w.hashCh <- hashBuf
	}
	return nil
}

// hashWorker processes hash buffers sequentially in a dedicated goroutine.
// This runs concurrently with compression, hiding hash latency.
func (w *Writer) hashWorker() {
	defer close(w.hashDone)
	for buf := range w.hashCh {
		w.hasher.Write(buf)
	}
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
	// Flush any partial block.
	if len(w.sizes) > 0 {
		if err := w.flush(); err != nil {
			w.err = err
			return err
		}
	}

	// Drain the hash pipeline (must complete before reading final hash).
	if w.hashCh != nil {
		close(w.hashCh)
		<-w.hashDone
	}

	// Drain the streaming compression pipeline.
	if w.workCh != nil {
		close(w.workCh)       // signal compress workers to stop
		err := <-w.writerDone // wait for writer to finish
		w.workCh = nil        // safe now — all workers have exited
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
	if w.hashCh != nil {
		close(w.hashCh)
		<-w.hashDone
	}
	if w.workCh != nil {
		close(w.workCh)
		<-w.writerDone
	}
	return w.pw.Abort()
}
