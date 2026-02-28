package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
	rocksdbES "github.com/tamir/events-analysis/eventstore/rocksdb"
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
	for _, conc := range []int{1, 4, 8, 16, 24, 32} {
		configs = append(configs,
			packConfig{fmt.Sprintf("c=%2d         ", conc), eventstore.WriterOptions{Concurrency: conc}},
			packConfig{fmt.Sprintf("c=%2d hash    ", conc), eventstore.WriterOptions{Concurrency: conc, ContentHash: true}},
		)
	}

	fmt.Println("\nPackfile:")
	for _, cfg := range configs {
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
		fmt.Printf("  %s: %.2fs  %4.0f MB/s\n", cfg.label, elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
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

	avgEventSize := totalRawBytes / len(allEvents)
	blockSize := 128 * avgEventSize

	fmt.Println("\nRocksDB:")
	for _, threads := range []int{1, 4, 8} {
		dir, err := os.MkdirTemp(baseDir, "rdb-*")
		if err != nil {
			t.Fatal(err)
		}
		dbPath := dir + "/rocks.db"

		start := time.Now()
		w, err := rocksdbES.Create(dbPath, rocksdbES.WriterOptions{
			BlockSize:          blockSize,
			CompressionThreads: threads,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := w.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("  t=%2d: %.2fs  %4.0f MB/s\n", threads, elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}

	fmt.Println("\nRocksDB (no compression, serial):")
	{
		dir, err := os.MkdirTemp(baseDir, "rdb-nocomp-*")
		if err != nil {
			t.Fatal(err)
		}
		dbPath := dir + "/rocks.db"

		start := time.Now()
		w, err := rocksdbES.Create(dbPath, rocksdbES.WriterOptions{
			BlockSize:     blockSize,
			NoCompression: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := w.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("  %.2fs  %4.0f MB/s\n", elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}
}
