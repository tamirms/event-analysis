package eventstore

import (
	"context"
	"iter"
)

// StoreWriter writes events to a store sequentially.
type StoreWriter interface {
	AppendEvent(event []byte) error
	Finish() error
	Close() error
}

// StoreReader reads events from a store.
//
// Data lifetime:
//   - ReadEvent returns a caller-owned copy.
//   - ReadEvents yields slices valid only until the next iteration.
//   - ReadPositions yields slices valid only until the next iteration.
type StoreReader interface {
	EventCount() (int, error)
	ReadEvent(position int) ([]byte, error)
	ReadEvents(start, count int) iter.Seq2[[]byte, error]
	ReadPositions(ctx context.Context, positions []int) iter.Seq2[[]byte, error]
	Close() error
}

// Compile-time conformance checks.
var (
	_ StoreWriter = (*Writer)(nil)
	_ StoreReader = (*Reader)(nil)
)
