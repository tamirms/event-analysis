package eventstore

import (
	"encoding/binary"
	"errors"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/recordcodec"
)

const DefaultBlockSize = 128

type Writer struct {
	pw     *packfile.Writer
	buf    []byte   // accumulates raw events for current batch
	sizes  []uint32 // event sizes in current batch
	total  int      // total events written
	blockN int      // events per block
	closed bool     // set by Finish or Abort
	err    error    // sticky — once set, all subsequent ops fail
}

// Create starts writing a new eventstore at path. blockSize is events per block.
func Create(path string, blockSize int) (*Writer, error) {
	if blockSize <= 0 {
		return nil, errors.New("eventstore: blockSize must be > 0")
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{})
	if err != nil {
		return nil, err
	}
	return &Writer{
		pw:     pw,
		blockN: blockSize,
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

func (w *Writer) flush() error {
	// buf already contains [event₀]...[eventₙ₋₁]
	// Build block: [events...][min(4B)][packed][W(1B)]
	encoded := packfile.EncodeGroup(w.sizes)
	// Use a separate buffer for the block to avoid corrupting w.buf on error.
	block := make([]byte, len(w.buf)+len(encoded))
	copy(block, w.buf)
	copy(block[len(w.buf):], encoded[1:]) // min + packed
	block[len(block)-1] = encoded[0]      // W as last byte

	compressed := recordcodec.Encode(block)
	if err := w.pw.Append(compressed); err != nil {
		return err
	}

	// Reset, keep backing arrays
	w.buf = w.buf[:0]
	w.sizes = w.sizes[:0]
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
