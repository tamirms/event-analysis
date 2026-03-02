package eventstore

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/tamir/events-analysis/packfile"
)

var ErrIndexRange = fmt.Errorf("eventstore: %w", packfile.ErrIndexRange)

// ReaderOption configures Reader behavior.
type ReaderOption func(*readerConfig)

type readerConfig struct {
	concurrency int
}

// WithConcurrency sets the max parallel goroutines for ReadIndices.
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

// ReadEvent reads a single event by global index.
// The caller owns the returned slice.
func (r *Reader) ReadEvent(index int) ([]byte, error) {
	data, err := r.pr.ReadItem(index)
	if errors.Is(err, packfile.ErrIndexRange) {
		return nil, ErrIndexRange
	}
	return data, err
}

// ReadEvents returns an iterator over count contiguous events starting at start.
// Each yielded []byte is valid only until the next iteration.
func (r *Reader) ReadEvents(start, count int) iter.Seq2[[]byte, error] {
	return r.pr.ReadRange(start, count)
}

// ReadIndices reads events at scattered indices with parallel I/O.
// indices must be sorted ascending with no duplicates.
// Each yielded []byte is valid only until the next iteration — copy if needed.
func (r *Reader) ReadIndices(ctx context.Context, indices []int) iter.Seq2[[]byte, error] {
	return r.pr.ReadItems(ctx, indices)
}

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	return r.pr.ContentHash()
}

// Verify recomputes the SHA-256 content hash and compares to stored hash.
func (r *Reader) Verify(ctx context.Context) error {
	if err := r.pr.Verify(ctx); err != nil {
		return fmt.Errorf("eventstore: %w", err)
	}
	return nil
}

// Close closes the underlying packfile.
func (r *Reader) Close() error { return r.pr.Close() }
