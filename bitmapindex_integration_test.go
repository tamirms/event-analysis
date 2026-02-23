package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/eventstore"
)

func TestBitmapIndexIntegration(t *testing.T) {
	if err := ensureFixtureDir(); err != nil {
		t.Fatal(err)
	}

	eventstorePath := filepath.Join(fixtureDir, "bench.events")
	mphfPath := filepath.Join(fixtureDir, "bitmapindex.mphf")
	dataPath := filepath.Join(fixtureDir, "bitmapindex.bitmaps")

	// Ensure eventstore fixture exists.
	if _, err := os.Stat(eventstorePath); err != nil {
		dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
		if dataLoadErr != nil {
			t.Fatalf("load events: %v", dataLoadErr)
		}
		t.Logf("building eventstore fixture with %d events...", len(allEvents))
		ew, err := eventstore.Create(eventstorePath, eventstore.WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := ew.Finish(); err != nil {
			t.Fatal(err)
		}
	}

	// Reuse cached bitmap index if not stale.
	needsBuild := true
	if _, err := os.Stat(dataPath); err == nil {
		if _, err := os.Stat(mphfPath); err == nil {
			if !fixturesStale() {
				t.Log("using cached bitmap index fixtures")
				needsBuild = false
			}
		}
	}

	if needsBuild {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		t.Log("building bitmap index from eventstore...")
		start := time.Now()
		if err := buildBitmapFromEventStore(ctx, eventstorePath, mphfPath, dataPath); err != nil {
			t.Fatalf("buildBitmapFromEventStore: %v", err)
		}
		t.Logf("bitmap index built in %v", time.Since(start))
	}

	// Print sizes.
	mphfInfo, _ := os.Stat(mphfPath)
	dataInfo, _ := os.Stat(dataPath)
	t.Logf("MPHF: %d bytes (%.1f KB)", mphfInfo.Size(), float64(mphfInfo.Size())/1024)
	t.Logf("Packfile: %d bytes (%.1f MB)", dataInfo.Size(), float64(dataInfo.Size())/(1024*1024))
	t.Logf("Total: %d bytes (%.1f MB)", mphfInfo.Size()+dataInfo.Size(),
		float64(mphfInfo.Size()+dataInfo.Size())/(1024*1024))

	// Open and verify.
	r, err := bitmapindex.Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Open eventstore for verification scans.
	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		t.Fatalf("open eventstore for verification: %v", err)
	}
	defer er.Close()

	allFields := []bitmapindex.Field{
		bitmapindex.FieldContractID,
		bitmapindex.FieldTopic0,
		bitmapindex.FieldTopic1,
		bitmapindex.FieldTopic2,
		bitmapindex.FieldTopic3,
	}

	// Count unique keys per field by scanning all events.
	fieldKeys := make([]map[string]struct{}, len(allFields))
	for i := range allFields {
		fieldKeys[i] = make(map[string]struct{})
	}

	var ev event.Event
	for data, err := range er.ReadEvents(0, er.EventCount()) {
		if err != nil {
			t.Fatal(err)
		}
		if err := event.Unmarshal(data, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.ContractID != nil {
			fieldKeys[0][string(ev.ContractID)] = struct{}{}
		}
		for i, topic := range ev.Topics {
			if i >= 4 {
				break
			}
			fieldKeys[1+i][string(topic)] = struct{}{}
		}
	}

	// Verify per-field key counts against known dataset values.
	expectedFieldCounts := map[bitmapindex.Field]int{
		bitmapindex.FieldContractID: 8358,
		bitmapindex.FieldTopic0:     172,
		bitmapindex.FieldTopic1:     401110,
		bitmapindex.FieldTopic2:     176309,
		bitmapindex.FieldTopic3:     7112,
	}

	totalKeys := 0
	t.Log("per-field key counts:")
	for i, f := range allFields {
		actual := len(fieldKeys[i])
		totalKeys += actual
		expected := expectedFieldCounts[f]
		t.Logf("  field %d: %d keys (expected %d)", f, actual, expected)
		if actual != expected {
			t.Errorf("field %d: got %d keys, want %d", f, actual, expected)
		}
	}
	t.Logf("total unique keys: %d (expected ~593061)", totalKeys)

	// Spot-check: find the dominant contract ID and verify bitmap cardinality.
	contractCounts := make(map[string]uint64, 10000)
	for data, err := range er.ReadEvents(0, er.EventCount()) {
		if err != nil {
			t.Fatal(err)
		}
		if err := event.Unmarshal(data, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.ContractID != nil {
			contractCounts[string(ev.ContractID)]++
		}
	}

	var maxContract string
	var maxCount uint64
	for k, v := range contractCounts {
		if v > maxCount {
			maxContract = k
			maxCount = v
		}
	}
	t.Logf("dominant contract: %d events", maxCount)

	bm, err := r.Lookup(bitmapindex.FieldContractID, []byte(maxContract))
	if err != nil {
		t.Fatalf("Lookup dominant contract: %v", err)
	}
	card := bm.GetCardinality()
	t.Logf("dominant contract bitmap cardinality: %d", card)
	if card != maxCount {
		t.Errorf("cardinality mismatch: bitmap=%d, scan=%d", card, maxCount)
	}

	fmt.Printf("\n=== Bitmap Index Stats ===\n")
	fmt.Printf("MPHF: %.1f KB\n", float64(mphfInfo.Size())/1024)
	fmt.Printf("Packfile: %.1f MB\n", float64(dataInfo.Size())/(1024*1024))
	fmt.Printf("Total: %.1f MB\n", float64(mphfInfo.Size()+dataInfo.Size())/(1024*1024))
	fmt.Printf("Total keys: %d\n", totalKeys)
	fmt.Printf("Dominant contract bitmap cardinality: %d\n", card)
}
