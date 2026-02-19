package eventstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/recordcodec"
)

var ErrIndexRange = errors.New("eventstore: event index out of range")

// blockBuf holds reusable buffers and a dedicated zstd decoder for ReadEvent.
// Pooled via sync.Pool to avoid per-call allocations and shared decoder contention.
type blockBuf struct {
	compressed   []byte
	decompressed []byte
	sizes        []uint32
	padded       []byte
	decoder      *zstd.Decoder
}

var blockBufPool = sync.Pool{
	New: func() any {
		return &blockBuf{decoder: recordcodec.NewDecoder()}
	},
}

type Reader struct {
	pr          *packfile.Reader
	nEvents     int
	blockN      int
	concurrency int
}

type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadIndices. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Open opens an eventstore for reading.
func Open(path string, opts ...ReaderOption) (*Reader, error) {
	pr, err := packfile.Open(path)
	if err != nil {
		return nil, err
	}

	meta := pr.Metadata()
	if len(meta) < 8 {
		pr.Close()
		return nil, fmt.Errorf("eventstore: invalid metadata length %d", len(meta))
	}

	nEvents := int(binary.LittleEndian.Uint32(meta[0:]))
	blockN := int(binary.LittleEndian.Uint32(meta[4:]))

	r := &Reader{
		pr:          pr,
		nEvents:     nEvents,
		blockN:      blockN,
		concurrency: 8,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// EventCount returns the total number of events.
func (r *Reader) EventCount() int { return r.nEvents }

// Close closes the underlying packfile.
func (r *Reader) Close() error { return r.pr.Close() }

func (r *Reader) blockCount() int {
	return (r.nEvents + r.blockN - 1) / r.blockN
}

func (r *Reader) eventsInBlock(blockIdx int) int {
	last := r.blockCount() - 1
	if blockIdx < last {
		return r.blockN
	}
	rem := r.nEvents % r.blockN
	if rem == 0 {
		return r.blockN
	}
	return rem
}

// decodeBlock parses the trailing FOR index from a decompressed block.
// Returns event sizes and the byte offset where event data ends.
// Reuses caller-provided slices for sizes and padded scratch space.
func decodeBlock(raw []byte, n int, sizes []uint32, padded []byte) ([]uint32, int, error) {
	if len(raw) < 6 { // minimum: 1 event byte + 4B min + 1B W
		return sizes, 0, fmt.Errorf("eventstore: block too small (%d bytes)", len(raw))
	}

	w := raw[len(raw)-1] // W is the last byte
	packSize := (int(w)*n + 7) / 8
	indexSize := 4 + packSize + 1 // min(4) + packed + W(1)

	if indexSize > len(raw) {
		return sizes, 0, fmt.Errorf("eventstore: index size %d exceeds block size %d", indexSize, len(raw))
	}

	indexStart := len(raw) - indexSize

	// Reconstruct standard FOR layout: [W][min][packed] with 7-byte overshoot
	paddedSize := 1 + 4 + packSize + 7
	if cap(padded) < paddedSize {
		padded = make([]byte, paddedSize)
	} else {
		padded = padded[:paddedSize]
		clear(padded)
	}
	padded[0] = w
	copy(padded[1:], raw[indexStart:len(raw)-1]) // min + packed

	sizes, _ = packfile.DecodeGroup(padded, n, sizes)
	dataEnd := indexStart

	// Validate sum(sizes) == dataEnd
	sum := 0
	for _, s := range sizes {
		sum += int(s)
	}
	if sum != dataEnd {
		return sizes, 0, fmt.Errorf("eventstore: size sum %d != data end %d", sum, dataEnd)
	}

	return sizes, dataEnd, nil
}

// ReadEvent reads a single event by global index.
// The caller owns the returned slice.
func (r *Reader) ReadEvent(index int) ([]byte, error) {
	if index < 0 || index >= r.nEvents {
		return nil, ErrIndexRange
	}

	blockIdx := index / r.blockN
	localIdx := index % r.blockN

	bb := blockBufPool.Get().(*blockBuf)

	var err error
	bb.compressed, err = r.pr.ReadRecordInto(blockIdx, bb.compressed)
	if err != nil {
		blockBufPool.Put(bb)
		return nil, err
	}

	bb.decompressed, err = bb.decoder.DecodeAll(bb.compressed, bb.decompressed[:0])
	if err != nil {
		blockBufPool.Put(bb)
		return nil, err
	}

	bb.sizes, _, err = decodeBlock(bb.decompressed, r.eventsInBlock(blockIdx), bb.sizes, bb.padded)
	if err != nil {
		blockBufPool.Put(bb)
		return nil, err
	}

	offset := 0
	for i := 0; i < localIdx; i++ {
		offset += int(bb.sizes[i])
	}

	// Copy event bytes into a fresh small allocation so we can pool the block buffers.
	eventSize := int(bb.sizes[localIdx])
	event := make([]byte, eventSize)
	copy(event, bb.decompressed[offset:])

	blockBufPool.Put(bb)
	return event, nil
}

// ReadEvents returns an iterator over count contiguous events starting at start.
// Each yielded []byte is valid only until the next iteration.
func (r *Reader) ReadEvents(start, count int) iter.Seq2[[]byte, error] {
	if start < 0 || count < 0 || start > r.nEvents || count > r.nEvents-start {
		panic(fmt.Sprintf("eventstore: ReadEvents(%d, %d) out of range [0, %d)",
			start, count, r.nEvents))
	}

	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}

		firstBlock := start / r.blockN
		lastBlock := (start + count - 1) / r.blockN
		blockCount := lastBlock - firstBlock + 1

		bb := blockBufPool.Get().(*blockBuf)
		defer blockBufPool.Put(bb)

		globalIdx := start

		blockIdx := firstBlock
		for compressed, rangeErr := range r.pr.ReadRecords(firstBlock, blockCount) {
			if rangeErr != nil {
				yield(nil, rangeErr)
				return
			}

			var err error
			bb.decompressed, err = bb.decoder.DecodeAll(compressed, bb.decompressed[:0])
			if err != nil {
				yield(nil, err)
				return
			}

			n := r.eventsInBlock(blockIdx)
			bb.sizes, _, err = decodeBlock(bb.decompressed, n, bb.sizes, bb.padded)
			if err != nil {
				yield(nil, err)
				return
			}

			// Compute start/end within this block
			blockStart := blockIdx * r.blockN
			localStart := 0
			if globalIdx > blockStart {
				localStart = globalIdx - blockStart
			}
			localEnd := n
			remaining := start + count - blockStart
			if remaining < localEnd {
				localEnd = remaining
			}

			// Compute byte offset for localStart
			offset := 0
			for i := 0; i < localStart; i++ {
				offset += int(bb.sizes[i])
			}

			for i := localStart; i < localEnd; i++ {
				end := offset + int(bb.sizes[i])
				if !yield(bb.decompressed[offset:end], nil) {
					return
				}
				offset = end
				globalIdx++
			}

			blockIdx++
		}
	}
}

// indicesWork holds working state for ReadIndices.
type indicesWork struct {
	blocks []blockRange // unique blocks to read
	events [][]byte     // output: one per input index
}

// blockRange maps a block to its slice of requested indices.
// Since indices are sorted, all indices for a given block are contiguous
// in the original indices slice — represented as [idxStart, idxEnd).
type blockRange struct {
	blockIdx int
	idxStart int // start index into indices[]
	idxEnd   int // end index into indices[]
}

func (w *indicesWork) reset(n int) {
	w.blocks = w.blocks[:0]
	if cap(w.events) < n {
		w.events = make([][]byte, n)
	} else {
		w.events = w.events[:n]
		clear(w.events)
	}
}

// ReadIndices reads events at scattered indices with parallel I/O.
// indices must be sorted in ascending order with no duplicates. Panics if any
// index is out of [0, EventCount) or if indices are not sorted/unique.
// Each yielded []byte is owned by the caller (copied out of pooled buffers).
func (r *Reader) ReadIndices(ctx context.Context, indices []int) iter.Seq2[[]byte, error] {
	// Validate bounds and sorted+unique invariant
	for i, idx := range indices {
		if idx < 0 || idx >= r.nEvents {
			panic(fmt.Sprintf("eventstore: ReadIndices index %d out of range [0, %d)",
				idx, r.nEvents))
		}
		if i > 0 && indices[i] <= indices[i-1] {
			panic(fmt.Sprintf("eventstore: ReadIndices indices not sorted/unique at position %d: %d <= %d",
				i, indices[i], indices[i-1]))
		}
	}

	return func(yield func([]byte, error) bool) {
		if len(indices) == 0 {
			return
		}

		w := &indicesWork{}
		w.reset(len(indices))

		// Phase 1: group indices by block.
		prevBlockIdx := -1
		for i, idx := range indices {
			blkIdx := idx / r.blockN
			if blkIdx != prevBlockIdx {
				if len(w.blocks) > 0 {
					w.blocks[len(w.blocks)-1].idxEnd = i
				}
				w.blocks = append(w.blocks, blockRange{blockIdx: blkIdx, idxStart: i})
				prevBlockIdx = blkIdx
			}
		}
		if len(w.blocks) > 0 {
			w.blocks[len(w.blocks)-1].idxEnd = len(indices)
		}

		// Phase 2: fixed worker pool, each worker steals blocks via atomic counter.
		var errOnce sync.Once
		var firstErr error

		numBlocks := len(w.blocks)
		numWorkers := min(numBlocks, r.concurrency)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var nextBlock atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for range numWorkers {
			go func() {
				defer wg.Done()
				bb := blockBufPool.Get().(*blockBuf)
				defer blockBufPool.Put(bb)

				for {
					bi := int(nextBlock.Add(1)) - 1
					if bi >= numBlocks || ctx.Err() != nil {
						return
					}
					if err := r.processBlock(w, bi, indices, bb); err != nil {
						errOnce.Do(func() { firstErr = err })
						cancel()
						return
					}
				}
			}()
		}
		wg.Wait()

		// Phase 3: yield results in index order.
		if firstErr != nil {
			yield(nil, firstErr)
			return
		}
		for _, ev := range w.events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// processBlock reads and decodes a single block, populating w.events for
// the requested indices within that block.
func (r *Reader) processBlock(w *indicesWork, bi int, indices []int, bb *blockBuf) error {
	blk := w.blocks[bi]

	var err error
	bb.compressed, err = r.pr.ReadRecordInto(blk.blockIdx, bb.compressed)
	if err != nil {
		return err
	}
	bb.decompressed, err = bb.decoder.DecodeAll(bb.compressed, bb.decompressed[:0])
	if err != nil {
		return err
	}

	n := r.eventsInBlock(blk.blockIdx)
	bb.sizes, _, err = decodeBlock(bb.decompressed, n, bb.sizes, bb.padded)
	if err != nil {
		return err
	}

	for j := blk.idxStart; j < blk.idxEnd; j++ {
		localIdx := indices[j] % r.blockN
		offset := 0
		for k := range localIdx {
			offset += int(bb.sizes[k])
		}
		eventSize := int(bb.sizes[localIdx])
		ev := make([]byte, eventSize)
		copy(ev, bb.decompressed[offset:offset+eventSize])
		w.events[j] = ev
	}
	return nil
}
