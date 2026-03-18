package eventstore

import (
	"context"
	"iter"
)

// StoreWriter writes events to a store sequentially.
type StoreWriter interface {
	Append(event []byte) error
	Finish() error
	Close() error
}

// StoreReader reads events from a store.
//
// Data lifetime:
//   - ReadEvent returns a caller-owned copy.
//   - ReadEvents yields slices valid only until the next iteration.
//   - ReadIndices yields slices valid only until the next iteration.
type StoreReader interface {
	EventCount() (int, error)
	ReadEvent(index int) ([]byte, error)
	ReadEvents(start, count int) iter.Seq2[[]byte, error]
	ReadIndices(ctx context.Context, indices []int) iter.Seq2[[]byte, error]
	Close() error
}

// Compile-time conformance checks.
var (
	_ StoreWriter = (*Writer)(nil)
	_ StoreReader = (*Reader)(nil)
)
