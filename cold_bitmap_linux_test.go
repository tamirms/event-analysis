//go:build linux

package main

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tamir/events-analysis/bitmapindex"
)

func benchColdBitmapMPHF(b *testing.B, numLookups int) {
	setupBitmapBenchData(b)

	dir := coldFixtureDir()
	mphfPath := filepath.Join(dir, "bitmapindex.mphf")
	dataPath := filepath.Join(dir, "bitmapindex.bitmaps")

	if _, err := os.Stat(mphfPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", mphfPath)
	}
	if _, err := os.Stat(dataPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", dataPath)
	}

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	// Pre-generate lookup keys for each iteration.
	lookupKeys := make([]int, numLookups)
	for i := range lookupKeys {
		lookupKeys[i] = rng.Intn(len(keys))
	}

	for b.Loop() {
		b.StopTimer()
		if err := dropFileCache(mphfPath); err != nil {
			b.Fatal(err)
		}
		if err := dropFileCache(dataPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		r := bitmapindex.Open(mphfPath, dataPath, bitmapindex.WithExpectedLookups(numLookups))

		for _, ki := range lookupKeys {
			k := keys[ki]
			bm, err := r.Lookup(k.Field, k.Key)
			if err != nil {
				b.Fatal(err)
			}
			_ = bm
		}

		r.Close()
	}
}

func benchColdBitmapMPHFLookupKeys(b *testing.B, numLookups int) {
	setupBitmapBenchData(b)

	dir := coldFixtureDir()
	mphfPath := filepath.Join(dir, "bitmapindex.mphf")
	dataPath := filepath.Join(dir, "bitmapindex.bitmaps")

	if _, err := os.Stat(mphfPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", mphfPath)
	}
	if _, err := os.Stat(dataPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", dataPath)
	}

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	// Pre-generate lookup key slices for each iteration.
	lookupKeys := make([]bitmapindex.FieldKey, numLookups)
	for i := range lookupKeys {
		k := keys[rng.Intn(len(keys))]
		lookupKeys[i] = bitmapindex.FieldKey{Field: k.Field, Key: k.Key}
	}

	ctx := context.Background()

	for b.Loop() {
		b.StopTimer()
		if err := dropFileCache(mphfPath); err != nil {
			b.Fatal(err)
		}
		if err := dropFileCache(dataPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		r := bitmapindex.Open(mphfPath, dataPath, bitmapindex.WithExpectedLookups(numLookups))

		results, err := r.LookupKeys(ctx, lookupKeys)
		if err != nil {
			b.Fatal(err)
		}
		_ = results

		r.Close()
	}
}

func benchColdBitmapRocksDBParallel(b *testing.B, numLookups int) {
	setupBitmapBenchData(b)

	dir := coldFixtureDir()
	rdbPath := filepath.Join(dir, "bitmapindex.rocksdb")

	if _, err := os.Stat(rdbPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", rdbPath)
	}

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	lookupKeys := make([]int, numLookups)
	for i := range lookupKeys {
		lookupKeys[i] = rng.Intn(len(keys))
	}

	for b.Loop() {
		b.StopTimer()
		if err := dropDirCache(rdbPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		r, err := openRocksDBBitmap(rdbPath)
		if err != nil {
			b.Fatal(err)
		}

		var wg sync.WaitGroup
		var errOnce sync.Once
		var firstErr error
		for _, ki := range lookupKeys {
			wg.Go(func() {
				k := keys[ki]
				bm, err := r.Lookup(k.Field, k.Key)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				_ = bm
			})
		}
		wg.Wait()
		if firstErr != nil {
			b.Fatal(firstErr)
		}

		r.Close()
	}
}

func benchColdBitmapRocksDB(b *testing.B, numLookups int) {
	setupBitmapBenchData(b)

	dir := coldFixtureDir()
	rdbPath := filepath.Join(dir, "bitmapindex.rocksdb")

	if _, err := os.Stat(rdbPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", rdbPath)
	}

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	lookupKeys := make([]int, numLookups)
	for i := range lookupKeys {
		lookupKeys[i] = rng.Intn(len(keys))
	}

	for b.Loop() {
		b.StopTimer()
		if err := dropDirCache(rdbPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		r, err := openRocksDBBitmap(rdbPath)
		if err != nil {
			b.Fatal(err)
		}

		for _, ki := range lookupKeys {
			k := keys[ki]
			bm, err := r.Lookup(k.Field, k.Key)
			if err != nil {
				b.Fatal(err)
			}
			_ = bm
		}

		r.Close()
	}
}

// --- 1 lookup (open + single query) ---
func BenchmarkColdBitmapMPHF1(b *testing.B)            { benchColdBitmapMPHF(b, 1) }
func BenchmarkColdBitmapMPHFLookupKeys1(b *testing.B)  { benchColdBitmapMPHFLookupKeys(b, 1) }
func BenchmarkColdBitmapRocksDB1(b *testing.B)         { benchColdBitmapRocksDB(b, 1) }
func BenchmarkColdBitmapRocksDBParallel1(b *testing.B) { benchColdBitmapRocksDBParallel(b, 1) }

// --- 5 lookups ---
func BenchmarkColdBitmapMPHF5(b *testing.B)            { benchColdBitmapMPHF(b, 5) }
func BenchmarkColdBitmapMPHFLookupKeys5(b *testing.B)  { benchColdBitmapMPHFLookupKeys(b, 5) }
func BenchmarkColdBitmapRocksDB5(b *testing.B)         { benchColdBitmapRocksDB(b, 5) }
func BenchmarkColdBitmapRocksDBParallel5(b *testing.B) { benchColdBitmapRocksDBParallel(b, 5) }

// --- 15 lookups ---
func BenchmarkColdBitmapMPHF15(b *testing.B)            { benchColdBitmapMPHF(b, 15) }
func BenchmarkColdBitmapMPHFLookupKeys15(b *testing.B)  { benchColdBitmapMPHFLookupKeys(b, 15) }
func BenchmarkColdBitmapRocksDB15(b *testing.B)         { benchColdBitmapRocksDB(b, 15) }
func BenchmarkColdBitmapRocksDBParallel15(b *testing.B) { benchColdBitmapRocksDBParallel(b, 15) }

// --- 50 lookups ---
func BenchmarkColdBitmapMPHF50(b *testing.B)            { benchColdBitmapMPHF(b, 50) }
func BenchmarkColdBitmapMPHFLookupKeys50(b *testing.B)  { benchColdBitmapMPHFLookupKeys(b, 50) }
func BenchmarkColdBitmapRocksDB50(b *testing.B)         { benchColdBitmapRocksDB(b, 50) }
func BenchmarkColdBitmapRocksDBParallel50(b *testing.B) { benchColdBitmapRocksDBParallel(b, 50) }
