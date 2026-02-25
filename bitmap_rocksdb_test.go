package main

import (
	"context"
	"fmt"

	rocksdbBI "github.com/tamir/events-analysis/bitmapindex/rocksdb"
	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/eventstore"
)

// buildRocksDBFromEventStore streams all events and builds a RocksDB bitmap index.
func buildRocksDBFromEventStore(ctx context.Context, storePath, dbPath string) error {
	er := eventstore.Open(storePath)
	defer er.Close()

	w := rocksdbBI.NewWriter(dbPath, rocksdbBI.WriterOptions{CapacityHint: 600_000})

	ordinal := uint32(0)
	var ev event.Event
	ec, err := er.EventCount()
	if err != nil {
		return fmt.Errorf("bitmapindex: event count: %w", err)
	}
	for data, err := range er.ReadEvents(0, ec) {
		if err != nil {
			return fmt.Errorf("bitmapindex: read event %d: %w", ordinal, err)
		}
		if err := event.Unmarshal(data, &ev); err != nil {
			return fmt.Errorf("bitmapindex: unmarshal event %d: %w", ordinal, err)
		}
		w.Add(&ev, ordinal)
		ordinal++
	}

	return w.Finish(ctx)
}
