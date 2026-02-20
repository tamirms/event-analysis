package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
)

// TestWriteThroughput measures end-to-end write throughput across concurrency
// levels without GOGC=1 overhead. Use WRITE_DIR to control the target device
// (defaults to os.TempDir, typically EBS).
func TestWriteThroughput(t *testing.T) {
	sstOnce.Do(func() { sstDataErr = loadAllEvents() })
	if sstDataErr != nil {
		t.Fatal(sstDataErr)
	}

	baseDir := os.Getenv("WRITE_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	for _, conc := range []int{1, 4, 8, 16, 24, 32} {
		dir, err := os.MkdirTemp(baseDir, "wp-*")
		if err != nil {
			t.Fatal(err)
		}
		p := dir + "/bench.events"

		start := time.Now()
		ew, err := eventstore.Create(p, eventstore.WriterOptions{Concurrency: conc})
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
		elapsed := time.Since(start)
		fmt.Printf("c=%2d: %.2fs  %4.0f MB/s\n", conc, elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}
}
