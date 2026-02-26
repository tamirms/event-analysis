package eventstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"runtime"
	"sync"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

var ErrIndexRange = fmt.Errorf("eventstore: %w", packfile.ErrIndexRange)

// blockBuf holds reusable buffers and a dedicated zstd decompressor for ReadEvent.
// Pooled via sync.Pool to avoid per-call allocations and shared decoder contention.
type blockBuf struct {
	compressed   []byte
	decompressed []byte
	sizes        []uint32
	offsets      []int // prefix sum of sizes: offsets[i] = byte offset of event i
	padded       []byte
	decompressor *zstd.Decompressor
}

var blockBufPool = sync.Pool{
	New: func() any {
		bb := &blockBuf{decompressor: zstd.NewDecompressor()}
		runtime.SetFinalizer(bb, func(b *blockBuf) {
			b.decompressor.Close()
		})
		return bb
	},
}

// decode decompresses a block (unless noCompress) and parses its trailing FOR
// index, populating sizes, offsets, and decompressed data.
func (bb *blockBuf) decode(data []byte, n int, noCompress bool) error {
	if noCompress {
		bb.decompressed = append(bb.decompressed[:0], data...)
	} else {
		decoded, err := bb.decompressor.Decode(bb.decompressed[:0], data)
		if err != nil {
			return err
		}
		bb.decompressed = decoded
	}
	var err error
	bb.sizes, bb.padded, err = decodeBlock(bb.decompressed, n, bb.sizes, bb.padded)
	if err != nil {
		return err
	}
	// Build prefix sum of sizes for offset lookups.
	if cap(bb.offsets) <= n {
		bb.offsets = make([]int, n+1)
	} else {
		bb.offsets = bb.offsets[:n+1]
	}
	bb.offsets[0] = 0
	for i, s := range bb.sizes {
		bb.offsets[i+1] = bb.offsets[i] + int(s)
	}
	return nil
}

// event returns a copy of the event at localIdx within the decoded block.
func (bb *blockBuf) event(localIdx int) []byte {
	offset := bb.offsets[localIdx]
	ev := make([]byte, bb.offsets[localIdx+1]-offset)
	copy(ev, bb.decompressed[offset:])
	return ev
}

type Reader struct {
	pr          *packfile.Reader // from packfile.Open (non-blocking)
	concurrency int              // from options, safe at construction time

	// Deferred init:
	once       sync.Once
	nEvents    int
	blockN     int
	noCompress bool
	openErr    error
}

type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadIndices.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

func (r *Reader) waitOpen() error {
	r.once.Do(func() {
		meta, err := r.pr.Metadata() // blocks on packfile ready
		if err != nil {
			r.openErr = err
			return
		}
		if len(meta) < 12 {
			r.openErr = fmt.Errorf("eventstore: invalid metadata length %d (want >= 12)", len(meta))
			return
		}

		r.nEvents = int(binary.LittleEndian.Uint32(meta[0:]))
		r.blockN = int(binary.LittleEndian.Uint32(meta[4:]))
		flags := binary.LittleEndian.Uint32(meta[8:])
		r.noCompress = flags&1 != 0

		if r.blockN <= 0 {
			r.openErr = fmt.Errorf("eventstore: invalid blockN %d in metadata", r.blockN)
			return
		}

		// Cross-validate: number of blocks implied by nEvents must match packfile record count.
		rc, err := r.pr.RecordCount()
		if err != nil {
			r.openErr = err
			return
		}
		expectedBlocks := (r.nEvents + r.blockN - 1) / r.blockN
		if r.nEvents == 0 {
			expectedBlocks = 0
		}
		if expectedBlocks != rc {
			r.openErr = fmt.Errorf("eventstore: metadata says %d events / %d blockN = %d blocks, but packfile has %d records",
				r.nEvents, r.blockN, expectedBlocks, rc)
		}
	})
	return r.openErr
}

// Open opens an eventstore for reading. Returns immediately; all I/O
// is deferred to the first method call that needs the result.
// Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader {
	r := &Reader{
		pr:          packfile.Open(path), // non-blocking
		concurrency: 8,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}
	return r
}

// EventCount returns the total number of events.
func (r *Reader) EventCount() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	return r.nEvents, nil
}

// Close closes the underlying packfile.
func (r *Reader) Close() error { return r.pr.Close() }

func (r *Reader) blockCount() int {
	return (r.nEvents + r.blockN - 1) / r.blockN
}

func (r *Reader) eventsInBlock(blockIdx int) int {
	if r.nEvents == 0 {
		return 0
	}
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
// Returns event sizes and the reusable padded scratch buffer.
// Callers must reassign both sizes and padded from the return values
// to benefit from buffer reuse.
func decodeBlock(raw []byte, n int, sizes []uint32, padded []byte) ([]uint32, []byte, error) {
	if len(raw) < 6 { // minimum: 1 event byte + 4B min + 1B W
		return sizes, padded, fmt.Errorf("eventstore: block too small (%d bytes)", len(raw))
	}

	w := raw[len(raw)-1] // W is the last byte
	if w > 32 {
		return sizes, padded, fmt.Errorf("eventstore: invalid FOR width %d in block (max 32)", w)
	}
	packSize := (int(w)*n + 7) / 8
	indexSize := 4 + packSize + 1 // min(4) + packed + W(1)

	if indexSize > len(raw) {
		return sizes, padded, fmt.Errorf("eventstore: index size %d exceeds block size %d", indexSize, len(raw))
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
		return sizes, padded, fmt.Errorf("eventstore: size sum %d != data end %d", sum, dataEnd)
	}

	return sizes, padded, nil
}

// ReadEvent reads a single event by global index.
// The caller owns the returned slice.
func (r *Reader) ReadEvent(index int) ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}

	if index < 0 || index >= r.nEvents {
		return nil, ErrIndexRange
	}

	blockIdx := index / r.blockN
	localIdx := index % r.blockN

	bb := blockBufPool.Get().(*blockBuf)
	defer blockBufPool.Put(bb)

	var err error
	bb.compressed, err = r.pr.ReadRecord(blockIdx, bb.compressed)
	if err != nil {
		return nil, err
	}
	if err := bb.decode(bb.compressed, r.eventsInBlock(blockIdx), r.noCompress); err != nil {
		return nil, err
	}
	return bb.event(localIdx), nil
}

// ReadEvents returns an iterator over count contiguous events starting at start.
// Each yielded []byte is valid only until the next iteration.
func (r *Reader) ReadEvents(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		if start < 0 || count < 0 || start > r.nEvents || count > r.nEvents-start {
			panic(fmt.Sprintf("eventstore: ReadEvents(%d, %d) out of range [0, %d)",
				start, count, r.nEvents))
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

			n := r.eventsInBlock(blockIdx)
			if err := bb.decode(compressed, n, r.noCompress); err != nil {
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

			offset := bb.offsets[localStart]
			for i := localStart; i < localEnd; i++ {
				end := bb.offsets[i+1]
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

// blockRange maps a block to its slice of requested indices.
// Since indices are sorted, all indices for a given block are contiguous
// in the original indices slice — represented as [inputStart, inputEnd).
type blockRange struct {
	blockIdx   int
	inputStart int // start index into indices[]
	inputEnd   int // end index into indices[]
}

// ReadIndices reads events at scattered indices with parallel I/O.
// indices must be sorted in ascending order with no duplicates. Panics if any
// index is out of [0, EventCount) or if indices are not sorted/unique.
// Each yielded []byte is owned by the caller (copied out of pooled buffers).
func (r *Reader) ReadIndices(ctx context.Context, indices []int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if len(indices) == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		// Validate bounds and sorted+unique invariant.
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

		// Phase 1: group indices by block, build blockIndices and blockGroups.
		var blockGroups []blockRange
		var blockIndices []int
		prevBlockIdx := -1
		for i, idx := range indices {
			blkIdx := idx / r.blockN
			if blkIdx != prevBlockIdx {
				if len(blockGroups) > 0 {
					blockGroups[len(blockGroups)-1].inputEnd = i
				}
				blockGroups = append(blockGroups, blockRange{blockIdx: blkIdx, inputStart: i})
				blockIndices = append(blockIndices, blkIdx)
				prevBlockIdx = blkIdx
			}
		}
		if len(blockGroups) > 0 {
			blockGroups[len(blockGroups)-1].inputEnd = len(indices)
		}

		events := make([][]byte, len(indices))

		// Pre-allocate per-worker blockBufs.
		numWorkers := min(len(blockGroups), r.concurrency)
		workers := make([]*blockBuf, numWorkers)
		for i := range workers {
			workers[i] = blockBufPool.Get().(*blockBuf)
		}

		// Phase 2: ReadScattered replaces manual work-stealing.
		err := r.pr.ReadScattered(ctx, blockIndices, r.concurrency,
			func(workerID, inputPos int, data []byte) error {
				bb := workers[workerID]
				// Safe to alias: data is stable for the duration of this callback,
				// and bb.decode + bb.event fully consume it before returning.
				bb.compressed = data
				blk := blockGroups[inputPos]
				if err := bb.decode(bb.compressed, r.eventsInBlock(blk.blockIdx), r.noCompress); err != nil {
					return err
				}
				for j := blk.inputStart; j < blk.inputEnd; j++ {
					events[j] = bb.event(indices[j] % r.blockN)
				}
				return nil
			})

		// Return workers to pool (skip on panic — process crash is fine).
		for _, bb := range workers {
			blockBufPool.Put(bb)
		}

		// Phase 3: yield results in index order.
		if err != nil {
			yield(nil, err)
			return
		}
		for _, ev := range events {
			if !yield(ev, nil) {
				return
			}
		}
	}
}
