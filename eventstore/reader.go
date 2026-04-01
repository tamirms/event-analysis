package eventstore

import (
	"bytes"
	"context"
	"iter"

	"github.com/tamir/events-analysis/packfile"
)

// ErrPositionOutOfRange is returned when a position is out of bounds.
var ErrPositionOutOfRange = packfile.ErrPositionOutOfRange

// ReaderOption configures Reader behavior.
type ReaderOption func(*readerConfig)

type readerConfig struct {
	concurrency int
}

// WithConcurrency sets the max parallel goroutines for ReadPositions.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(cfg *readerConfig) { cfg.concurrency = n }
}

// Reader reads events from an eventstore packfile.
type Reader struct {
	pr *packfile.Reader
}

// Open opens an eventstore for reading. Returns immediately; all I/O
// is deferred to the first method call that needs the result.
// Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader {
	var cfg readerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	var prOpts []packfile.ReaderOption
	if cfg.concurrency != 0 {
		prOpts = append(prOpts, packfile.WithConcurrency(cfg.concurrency))
	}
	return &Reader{
		pr: packfile.Open(path, prOpts...),
	}
}

// EventCount returns the total number of events.
func (r *Reader) EventCount() (int, error) {
	return r.pr.TotalItems()
}

// ReadEvent reads a single event by position.
// The caller owns the returned slice.
func (r *Reader) ReadEvent(position int) ([]byte, error) {
	var result []byte
	if err := r.pr.ReadItem(position, func(data []byte) error {
		result = bytes.Clone(data)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// ReadEvents returns an iterator over count contiguous events starting at start.
// Each yielded []byte is valid only until the next iteration.
func (r *Reader) ReadEvents(start, count int) iter.Seq2[[]byte, error] {
	return r.pr.ReadRange(start, count)
}

// ReadPositions reads events at scattered positions with parallel I/O.
// positions must be sorted ascending with no duplicates.
// Each yielded []byte is owned by the caller.
func (r *Reader) ReadPositions(ctx context.Context, positions []int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if len(positions) == 0 {
			return
		}
		results := make([][]byte, len(positions))
		if err := r.pr.ReadItems(ctx, positions, func(idx int, data []byte) error {
			results[idx] = bytes.Clone(data)
			return nil
		}); err != nil {
			yield(nil, err)
			return
		}
		for _, item := range results {
			if !yield(item, nil) {
				return
			}
		}
	}
}

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	return r.pr.ContentHash()
}

// Verify recomputes the SHA-256 content hash and compares to stored hash.
func (r *Reader) Verify(ctx context.Context) error {
	return r.pr.Verify(ctx)
}

// Close closes the underlying packfile.
func (r *Reader) Close() error { return r.pr.Close() }
