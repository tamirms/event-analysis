package eventstore

import (
	"context"
	"fmt"
	"iter"
	"sync"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/packfile/record"
)

var ErrIndexRange = fmt.Errorf("eventstore: %w", packfile.ErrIndexRange)

type Reader struct {
	pr          *packfile.Reader // from packfile.Open (non-blocking)
	concurrency int              // from options, safe at construction time

	// Deferred init:
	once       sync.Once
	nEvents    int
	blockN     int
	compressed bool
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

		totalItems, recordSize, flags, err := packfile.DecodeMetadata(meta)
		if err != nil {
			r.openErr = fmt.Errorf("eventstore: %w", err)
			return
		}

		if flags&^packfile.FlagNoCompression != 0 {
			r.openErr = fmt.Errorf("eventstore: unknown metadata flags: %08x", flags)
			return
		}

		r.nEvents = totalItems
		r.blockN = recordSize
		r.compressed = flags&packfile.FlagNoCompression == 0

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

	rd := record.Get()
	defer record.Put(rd)

	n := packfile.ItemsInRecord(r.nEvents, r.blockN, blockIdx)
	if err := rd.ReadAndDecode(r.pr, blockIdx, n, r.compressed); err != nil {
		return nil, err
	}
	return rd.EntryCopy(localIdx), nil
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

		rd := record.Get()
		defer record.Put(rd)

		globalIdx := start

		blockIdx := firstBlock
		for recData, rangeErr := range r.pr.ReadRecords(firstBlock, blockCount) {
			if rangeErr != nil {
				yield(nil, rangeErr)
				return
			}

			n := packfile.ItemsInRecord(r.nEvents, r.blockN, blockIdx)
			if err := rd.Decode(recData, n, r.compressed); err != nil {
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

			for i := localStart; i < localEnd; i++ {
				if !yield(rd.Entry(i), nil) {
					return
				}
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

		// Pre-allocate per-worker decoders.
		numWorkers := min(len(blockGroups), r.concurrency)
		workers := make([]*record.Decoder, numWorkers)
		for i := range workers {
			workers[i] = record.Get()
		}

		// Phase 2: ReadScattered replaces manual work-stealing.
		err := r.pr.ReadScattered(ctx, blockIndices, r.concurrency,
			func(workerID, inputPos int, data []byte) error {
				rd := workers[workerID]
				blk := blockGroups[inputPos]
				n := packfile.ItemsInRecord(r.nEvents, r.blockN, blk.blockIdx)
				if err := rd.Decode(data, n, r.compressed); err != nil {
					return err
				}
				for j := blk.inputStart; j < blk.inputEnd; j++ {
					events[j] = rd.EntryCopy(indices[j] % r.blockN)
				}
				return nil
			})

		// Return workers to pool (skip on panic — process crash is fine).
		for _, rd := range workers {
			record.Put(rd)
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
