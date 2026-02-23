package main

import (
	"context"
	"fmt"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/eventstore"
)

// buildBitmapFromEventStore streams all events from the given eventstore and builds
// an MPHF+packfile bitmap index.
func buildBitmapFromEventStore(ctx context.Context, storePath, mphfPath, dataPath string) error {
	er, err := eventstore.Open(storePath)
	if err != nil {
		return fmt.Errorf("bitmapindex: open eventstore: %w", err)
	}
	defer er.Close()

	w := bitmapindex.NewWriter(bitmapindex.WriterOptions{CapacityHint: 600_000})

	ordinal := uint32(0)
	var ev event.Event
	for data, err := range er.ReadEvents(0, er.EventCount()) {
		if err != nil {
			return fmt.Errorf("bitmapindex: read event %d: %w", ordinal, err)
		}
		if err := event.Unmarshal(data, &ev); err != nil {
			return fmt.Errorf("bitmapindex: unmarshal event %d: %w", ordinal, err)
		}
		w.Add(&ev, ordinal)
		ordinal++
	}

	return w.Finish(ctx, mphfPath, dataPath)
}
