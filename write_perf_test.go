package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"
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

	type benchConfig struct {
		label string
		opts  eventstore.WriterOptions
	}

	// Compressed formats × concurrency × hash.
	var compressed []benchConfig
	for _, conc := range []int{1, 4, 8, 16, 24} {
		compressed = append(compressed,
			benchConfig{fmt.Sprintf("c=%2d         ", conc), eventstore.WriterOptions{Concurrency: conc}},
			benchConfig{fmt.Sprintf("c=%2d hash    ", conc), eventstore.WriterOptions{Concurrency: conc, ContentHash: true}},
		)
	}

	// Non-compressed formats × concurrency × hash.
	var noCompression []benchConfig
	for _, format := range []packfile.RecordFormat{packfile.Uncompressed, packfile.Raw} {
		for _, conc := range []int{0, 4, 8} {
			for _, hash := range []bool{false, true} {
				noCompression = append(noCompression, benchConfig{
					fmt.Sprintf("%-12s c=%d hash=%-5v", format, conc, hash),
					eventstore.WriterOptions{Format: format, Concurrency: conc, ContentHash: hash},
				})
			}
		}
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
			if err := ew.AppendEvent(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := ew.Finish(); err != nil {
			t.Fatal(err)
		}
		os.RemoveAll(dir)
	}

	runConfigs := func(header string, configs []benchConfig) {
		const iters = 5
		fmt.Printf("\n%s:\n", header)
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
					if err := ew.AppendEvent(ev); err != nil {
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
	}

	runConfigs("Packfile", compressed)
	runConfigs("Packfile (no compression)", noCompression)
}
