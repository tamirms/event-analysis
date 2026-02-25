package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
	rocksdbES "github.com/tamir/events-analysis/eventstore/rocksdb"
)

// TestReadThroughputNoCompression measures read throughput for packfile and
// RocksDB with compression disabled, isolating raw I/O and decoding overhead.
func TestReadThroughputNoCompression(t *testing.T) {
	// Load events.
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		t.Fatal(dataLoadErr)
	}

	numEvents := len(allEvents)
	avgEventSize := totalRawBytes / numEvents
	blockSize := 128 * avgEventSize

	fmt.Printf("\nNo-compression read benchmark: %d events, %s total raw bytes, block size %s\n",
		numEvents, fmtKB(float64(totalRawBytes)), fmtKB(float64(blockSize)))

	baseDir := os.TempDir()

	// --- Write no-compression packfile ---
	packDir, err := os.MkdirTemp(baseDir, "nocomp-pack-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(packDir)

	packPath := filepath.Join(packDir, "bench.events")
	{
		start := time.Now()
		ew, err := eventstore.Create(packPath, eventstore.WriterOptions{NoCompression: true})
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
		info, _ := os.Stat(packPath)
		fmt.Printf("  Packfile (no-comp) written: %s in %.2fs\n",
			fmtKB(float64(info.Size())), elapsed.Seconds())
	}

	// --- Write no-compression RocksDB ---
	rocksDir, err := os.MkdirTemp(baseDir, "nocomp-rocks-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rocksDir)

	dbPath := filepath.Join(rocksDir, "rocks.db")
	{
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
		rocksSize := dirSize(dbPath)
		fmt.Printf("  RocksDB (no-comp) written: %s in %.2fs\n",
			fmtKB(float64(rocksSize)), elapsed.Seconds())
	}

	// ======================================================================
	// Sequential read benchmark
	// ======================================================================
	fmt.Println("\n--- Sequential Read (full scan) ---")

	const seqRuns = 5

	// Packfile sequential
	{
		er := eventstore.Open(packPath)
		n, err := er.EventCount()
		if err != nil {
			t.Fatal(err)
		}

		times := make([]float64, seqRuns)
		for r := range seqRuns {
			start := time.Now()
			for ev, err := range er.ReadEvents(0, n) {
				if err != nil {
					t.Fatal(err)
				}
				_ = ev
			}
			times[r] = time.Since(start).Seconds()
		}
		er.Close()

		sort.Float64s(times)
		median := times[seqRuns/2]
		mbps := float64(totalRawBytes) / median / 1e6
		fmt.Printf("  Packfile seq read:  median %.4fs  %4.0f MB/s  (%d runs)\n", median, mbps, seqRuns)
	}

	// RocksDB sequential
	{
		rr := rocksdbES.Open(dbPath)
		n, err := rr.EventCount()
		if err != nil {
			t.Fatal(err)
		}

		times := make([]float64, seqRuns)
		for r := range seqRuns {
			start := time.Now()
			for ev, err := range rr.ReadEvents(0, n) {
				if err != nil {
					t.Fatal(err)
				}
				_ = ev
			}
			times[r] = time.Since(start).Seconds()
		}
		rr.Close()

		sort.Float64s(times)
		median := times[seqRuns/2]
		mbps := float64(totalRawBytes) / median / 1e6
		fmt.Printf("  RocksDB seq read:   median %.4fs  %4.0f MB/s  (%d runs)\n", median, mbps, seqRuns)
	}

	// ======================================================================
	// Random point read benchmark (1000 reads)
	// ======================================================================
	fmt.Println("\n--- Random Point Reads (1000 lookups) ---")

	const pointReads = 1000
	const pointRuns = 5

	// Generate deterministic random indices.
	rng := rand.New(rand.NewSource(42))
	pointIndices := make([]int, pointReads)
	for i := range pointIndices {
		pointIndices[i] = rng.Intn(numEvents)
	}

	// Packfile point reads
	{
		er := eventstore.Open(packPath)

		times := make([]float64, pointRuns)
		for r := range pointRuns {
			start := time.Now()
			for _, idx := range pointIndices {
				ev, err := er.ReadEvent(idx)
				if err != nil {
					t.Fatal(err)
				}
				_ = ev
			}
			times[r] = time.Since(start).Seconds()
		}
		er.Close()

		sort.Float64s(times)
		median := times[pointRuns/2]
		usPerRead := median / float64(pointReads) * 1e6
		fmt.Printf("  Packfile point:     median %.4fs  %.1f us/read  (%d reads x %d runs)\n",
			median, usPerRead, pointReads, pointRuns)
	}

	// RocksDB point reads
	{
		rr := rocksdbES.Open(dbPath)

		times := make([]float64, pointRuns)
		for r := range pointRuns {
			start := time.Now()
			for _, idx := range pointIndices {
				ev, err := rr.ReadEvent(idx)
				if err != nil {
					t.Fatal(err)
				}
				_ = ev
			}
			times[r] = time.Since(start).Seconds()
		}
		rr.Close()

		sort.Float64s(times)
		median := times[pointRuns/2]
		usPerRead := median / float64(pointReads) * 1e6
		fmt.Printf("  RocksDB point:      median %.4fs  %.1f us/read  (%d reads x %d runs)\n",
			median, usPerRead, pointReads, pointRuns)
	}

	// ======================================================================
	// Scattered read benchmark (50 sorted indices, like ReadIndices)
	// ======================================================================
	fmt.Println("\n--- Scattered Reads (50 sorted indices x 100 iterations) ---")

	const scatteredIter = 100
	const scatteredN = 50
	const scatteredRuns = 3

	// Packfile scattered reads via ReadIndices
	{
		er := eventstore.Open(packPath)
		n, err := er.EventCount()
		if err != nil {
			t.Fatal(err)
		}

		times := make([]float64, scatteredRuns)
		for r := range scatteredRuns {
			rng := rand.New(rand.NewSource(99))
			start := time.Now()
			for range scatteredIter {
				indices := generateScatteredIndices(rng, scatteredN, n)
				for ev, err := range er.ReadIndices(context.Background(), indices) {
					if err != nil {
						t.Fatal(err)
					}
					_ = ev
				}
			}
			times[r] = time.Since(start).Seconds()
		}
		er.Close()

		sort.Float64s(times)
		median := times[scatteredRuns/2]
		usPerCall := median / float64(scatteredIter) * 1e6
		fmt.Printf("  Packfile scattered: median %.4fs  %.0f us/call  (%d x %d indices, %d runs)\n",
			median, usPerCall, scatteredIter, scatteredN, scatteredRuns)
	}

	// RocksDB scattered reads via ReadIndices
	{
		rr := rocksdbES.Open(dbPath)

		times := make([]float64, scatteredRuns)
		for r := range scatteredRuns {
			rng := rand.New(rand.NewSource(99))
			start := time.Now()
			for range scatteredIter {
				indices := generateScatteredIndices(rng, scatteredN, numEvents)
				for ev, err := range rr.ReadIndices(context.Background(), indices) {
					if err != nil {
						t.Fatal(err)
					}
					_ = ev
				}
			}
			times[r] = time.Since(start).Seconds()
		}
		rr.Close()

		sort.Float64s(times)
		median := times[scatteredRuns/2]
		usPerCall := median / float64(scatteredIter) * 1e6
		fmt.Printf("  RocksDB scattered:  median %.4fs  %.0f us/call  (%d x %d indices, %d runs)\n",
			median, usPerCall, scatteredIter, scatteredN, scatteredRuns)
	}

	// ======================================================================
	// Range scan from random offset (128 events)
	// ======================================================================
	fmt.Println("\n--- Range Scan 128 from Random Offset (1000 iterations) ---")

	const rangeScanIter = 1000
	const rangeScanLen = 128
	const rangeScanRuns = 3

	// Packfile range scan
	{
		er := eventstore.Open(packPath)
		n, err := er.EventCount()
		if err != nil {
			t.Fatal(err)
		}

		times := make([]float64, rangeScanRuns)
		for r := range rangeScanRuns {
			rng := rand.New(rand.NewSource(77))
			start := time.Now()
			for range rangeScanIter {
				off := rng.Intn(n - rangeScanLen)
				for ev, err := range er.ReadEvents(off, rangeScanLen) {
					if err != nil {
						t.Fatal(err)
					}
					_ = ev
				}
			}
			times[r] = time.Since(start).Seconds()
		}
		er.Close()

		sort.Float64s(times)
		median := times[rangeScanRuns/2]
		usPerScan := median / float64(rangeScanIter) * 1e6
		fmt.Printf("  Packfile range128:  median %.4fs  %.0f us/scan  (%d scans, %d runs)\n",
			median, usPerScan, rangeScanIter, rangeScanRuns)
	}

	// RocksDB range scan
	{
		rr := rocksdbES.Open(dbPath)
		n, err := rr.EventCount()
		if err != nil {
			t.Fatal(err)
		}

		times := make([]float64, rangeScanRuns)
		for r := range rangeScanRuns {
			rng := rand.New(rand.NewSource(77))
			start := time.Now()
			for range rangeScanIter {
				off := rng.Intn(n - rangeScanLen)
				for ev, err := range rr.ReadEvents(off, rangeScanLen) {
					if err != nil {
						t.Fatal(err)
					}
					_ = ev
				}
			}
			times[r] = time.Since(start).Seconds()
		}
		rr.Close()

		sort.Float64s(times)
		median := times[rangeScanRuns/2]
		usPerScan := median / float64(rangeScanIter) * 1e6
		fmt.Printf("  RocksDB range128:   median %.4fs  %.0f us/scan  (%d scans, %d runs)\n",
			median, usPerScan, rangeScanIter, rangeScanRuns)
	}
}
