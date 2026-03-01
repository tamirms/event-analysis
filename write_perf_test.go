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
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		t.Fatal(dataLoadErr)
	}

	baseDir := os.Getenv("WRITE_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	type packConfig struct {
		label string
		opts  eventstore.WriterOptions
	}
	var configs []packConfig
	for _, conc := range []int{1, 4, 8, 16, 24} {
		configs = append(configs,
			packConfig{fmt.Sprintf("c=%2d         ", conc), eventstore.WriterOptions{Concurrency: conc}},
			packConfig{fmt.Sprintf("c=%2d hash    ", conc), eventstore.WriterOptions{Concurrency: conc, ContentHash: true}},
		)
	}

	// Warmup: one throwaway write to stabilize CPU frequency and caches.
	{
		dir, err := os.MkdirTemp(baseDir, "wp-warmup-*")
		if err != nil {
			t.Fatal(err)
		}
		p := dir + "/warmup.events"
		ew, err := eventstore.Create(p, eventstore.WriterOptions{Concurrency: 8})
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
		os.RemoveAll(dir)
	}

	const iters = 5
	fmt.Println("\nPackfile:")
	for _, cfg := range configs {
		var best time.Duration
		for i := range iters {
			dir, err := os.MkdirTemp(baseDir, "wp-*")
			if err != nil {
				t.Fatal(err)
			}
			p := dir + "/bench.events"

			start := time.Now()
			ew, err := eventstore.Create(p, cfg.opts)
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
			if i == 0 || elapsed < best {
				best = elapsed
			}
			os.RemoveAll(dir)
		}
		fmt.Printf("  %s: %.2fs  %4.0f MB/s\n", cfg.label, best.Seconds(),
			float64(totalRawBytes)/best.Seconds()/1e6)
	}

	fmt.Println("\nPackfile (no compression, serial):")
	{
		dir, err := os.MkdirTemp(baseDir, "wp-nocomp-*")
		if err != nil {
			t.Fatal(err)
		}
		p := dir + "/bench.events"

		start := time.Now()
		ew, err := eventstore.Create(p, eventstore.WriterOptions{NoCompression: true})
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
		fmt.Printf("  %.2fs  %4.0f MB/s\n", elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}
}
