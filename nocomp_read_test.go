package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
)

// openNoCompRocksDB opens a RocksDB for read-only access with no block cache.
// Returns db, opts, and a cleanup function that closes/destroys all resources.
func openNoCompRocksDB(path string) (*grocksdb.DB, func(), error) {
	opts := grocksdb.NewDefaultOptions()
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	opts.SetBlockBasedTableFactory(bbto)
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}
	cleanup := func() {
		db.Close()
		opts.Destroy()
	}
	return db, cleanup, nil
}

// TestReadThroughputNoCompression measures read throughput for packfile and
// RocksDB with compression disabled, isolating raw I/O and decoding overhead.
func TestReadThroughputNoCompression(t *testing.T) {
	// Load events.
	sstOnce.Do(func() { sstDataErr = loadAllEvents() })
	if sstDataErr != nil {
		t.Fatal(sstDataErr)
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

	sstFilePath := filepath.Join(rocksDir, "ingest.sst")
	dbPath := filepath.Join(rocksDir, "rocks.db")
	{
		start := time.Now()

		writeOpts := grocksdb.NewDefaultOptions()
		writeOpts.SetCompression(grocksdb.NoCompression)
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(blockSize)
		writeOpts.SetBlockBasedTableFactory(bbto)

		envOpts := grocksdb.NewDefaultEnvOptions()
		sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
		if err := sfw.Open(sstFilePath); err != nil {
			t.Fatal(err)
		}
		key := make([]byte, 4)
		for i, ev := range allEvents {
			binary.BigEndian.PutUint32(key, uint32(i))
			if err := sfw.Add(key, ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := sfw.Finish(); err != nil {
			t.Fatal(err)
		}
		sfw.Destroy()
		envOpts.Destroy()
		writeOpts.Destroy()

		dbOpts := grocksdb.NewDefaultOptions()
		dbOpts.SetCreateIfMissing(true)
		dbOpts.SetCompression(grocksdb.NoCompression)
		dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
		dbBbto.SetNoBlockCache(true)
		dbBbto.SetBlockSize(blockSize)
		dbOpts.SetBlockBasedTableFactory(dbBbto)

		db, err := grocksdb.OpenDb(dbOpts, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
		if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
			t.Fatal(err)
		}
		ingestOpts.Destroy()
		db.Close()
		dbOpts.Destroy()
		os.Remove(sstFilePath)

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
		er, err := eventstore.Open(packPath)
		if err != nil {
			t.Fatal(err)
		}
		n := er.EventCount()

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
		db, cleanup, err := openNoCompRocksDB(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		ro := grocksdb.NewDefaultReadOptions()
		ro.SetVerifyChecksums(false)

		times := make([]float64, seqRuns)
		for r := range seqRuns {
			start := time.Now()
			it := db.NewIterator(ro)
			it.SeekToFirst()
			for ; it.Valid(); it.Next() {
				_ = it.Value().Data()
			}
			it.Close()
			times[r] = time.Since(start).Seconds()
		}
		ro.Destroy()
		cleanup()

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
		er, err := eventstore.Open(packPath)
		if err != nil {
			t.Fatal(err)
		}

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
		db, cleanup, err := openNoCompRocksDB(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		ro := grocksdb.NewDefaultReadOptions()
		ro.SetVerifyChecksums(false)

		key := make([]byte, 4)
		times := make([]float64, pointRuns)
		for r := range pointRuns {
			start := time.Now()
			for _, idx := range pointIndices {
				binary.BigEndian.PutUint32(key, uint32(idx))
				val, err := db.GetPinned(ro, key)
				if err != nil {
					t.Fatal(err)
				}
				_ = val.Data()
				val.Destroy()
			}
			times[r] = time.Since(start).Seconds()
		}
		ro.Destroy()
		cleanup()

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
		er, err := eventstore.Open(packPath)
		if err != nil {
			t.Fatal(err)
		}
		n := er.EventCount()

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

	// RocksDB scattered reads via BatchedMultiGetCF
	{
		db, cleanup, err := openNoCompRocksDB(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		cf := db.GetDefaultColumnFamily()

		times := make([]float64, scatteredRuns)
		for r := range scatteredRuns {
			rng := rand.New(rand.NewSource(99))
			start := time.Now()
			for range scatteredIter {
				indices := generateScatteredIndices(rng, scatteredN, numEvents)
				keys := make([][]byte, len(indices))
				for i, idx := range indices {
					k := make([]byte, 4)
					binary.BigEndian.PutUint32(k, uint32(idx))
					keys[i] = k
				}
				ro := grocksdb.NewDefaultReadOptions()
				ro.SetVerifyChecksums(false)
				ro.SetFillCache(false)
				vals, err := db.BatchedMultiGetCF(ro, cf, true, keys...)
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range vals {
					_ = v.Data()
				}
				vals.Destroy()
				ro.Destroy()
			}
			times[r] = time.Since(start).Seconds()
		}
		cleanup()

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
		er, err := eventstore.Open(packPath)
		if err != nil {
			t.Fatal(err)
		}
		n := er.EventCount()

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
		db, cleanup, err := openNoCompRocksDB(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		ro := grocksdb.NewDefaultReadOptions()
		ro.SetVerifyChecksums(false)

		key := make([]byte, 4)

		times := make([]float64, rangeScanRuns)
		for r := range rangeScanRuns {
			rng := rand.New(rand.NewSource(77))
			start := time.Now()
			for range rangeScanIter {
				off := rng.Intn(numEvents - rangeScanLen)
				binary.BigEndian.PutUint32(key, uint32(off))
				it := db.NewIterator(ro)
				it.Seek(key)
				for i := 0; i < rangeScanLen && it.Valid(); i++ {
					_ = it.Value().Data()
					it.Next()
				}
				it.Close()
			}
			times[r] = time.Since(start).Seconds()
		}
		ro.Destroy()
		cleanup()

		sort.Float64s(times)
		median := times[rangeScanRuns/2]
		usPerScan := median / float64(rangeScanIter) * 1e6
		fmt.Printf("  RocksDB range128:   median %.4fs  %.0f us/scan  (%d scans, %d runs)\n",
			median, usPerScan, rangeScanIter, rangeScanRuns)
	}
}
