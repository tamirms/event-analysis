package eventstore

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/recordcodec"
)

const DefaultBlockSize = 128

// batchSize is the number of blocks accumulated before a parallel flush.
const batchSize = 256

// WriterOptions configures how the eventstore is written.
type WriterOptions struct {
	BlockSize   int // events per block; 0 defaults to DefaultBlockSize (128)
	Concurrency int // compression goroutines; 0 or 1 = serial
}

type Writer struct {
	pw     *packfile.Writer
	buf    []byte   // accumulates raw events for current block
	sizes  []uint32 // event sizes in current block
	total  int      // total events written
	blockN int      // events per block
	closed bool     // set by Finish or Abort
	err    error    // sticky — once set, all subsequent ops fail

	// Parallel compression state.
	concurrency int
	pending     [][]byte // uncompressed blocks awaiting batch flush
}

// Create starts writing a new eventstore at path.
func Create(path string, opts WriterOptions) (*Writer, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.BlockSize < 0 {
		return nil, errors.New("eventstore: BlockSize must be > 0")
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{})
	if err != nil {
		return nil, err
	}
	return &Writer{
		pw:          pw,
		blockN:      opts.BlockSize,
		concurrency: opts.Concurrency,
	}, nil
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
	encoded := packfile.EncodeGroup(w.sizes)
	block := make([]byte, len(w.buf)+len(encoded))
	copy(block, w.buf)
	copy(block[len(w.buf):], encoded[1:]) // min + packed
	block[len(block)-1] = encoded[0]      // W as last byte
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
	return block
}

func (w *Writer) flush() error {
	block := w.buildBlock()

	if w.concurrency <= 1 {
		compressed := recordcodec.Encode(block)
		return w.pw.Append(compressed)
	}

	w.pending = append(w.pending, block)
	if len(w.pending) >= batchSize {
		return w.flushBatch()
	}
	return nil
}

// flushBatch compresses all pending blocks in parallel, then writes them sequentially.
func (w *Writer) flushBatch() error {
	n := len(w.pending)
	if n == 0 {
		return nil
	}

	compressed := make([][]byte, n)
	var wg sync.WaitGroup
	perWorker := (n + w.concurrency - 1) / w.concurrency

	for i := range w.concurrency {
		lo := i * perWorker
		hi := min(lo+perWorker, n)
		if lo >= n {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := lo; j < hi; j++ {
				compressed[j] = recordcodec.Encode(w.pending[j])
			}
		}()
	}
	wg.Wait()

	for i, c := range compressed {
		if err := w.pw.Append(c); err != nil {
			return err
		}
		w.pending[i] = nil // allow GC
	}
	w.pending = w.pending[:0]
	return nil
}

// Finish flushes any partial batch, writes metadata, and finalizes the packfile.
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
	if len(w.pending) > 0 {
		if err := w.flushBatch(); err != nil {
			w.err = err
			return err
		}
	}

	// Encode metadata: [4B eventCount LE][4B blockSize LE]
	var meta [8]byte
	binary.LittleEndian.PutUint32(meta[0:], uint32(w.total))
	binary.LittleEndian.PutUint32(meta[4:], uint32(w.blockN))
	w.pw.SetMetadata(meta[:])

	_, err := w.pw.Finish()
	if err != nil {
		w.err = err
		return err
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
	return w.pw.Abort()
}
