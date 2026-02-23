package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/eventstore"
)

var (
	bitmapBenchOnce sync.Once
	bitmapBenchErr  error

	bitmapMPHFPath    string
	bitmapDataPath    string
	bitmapRocksDBPath string
	bitmapSampleKeys  []bitmapSampleKey
)

type bitmapSampleKey struct {
	field   field  // local type from bitmap_helpers_test.go
	key     []byte // raw key for RocksDB
	mphfKey []byte // pre-composed key for MPHF (key || field_byte)
}

func setupBitmapBenchData(b *testing.B) {
	b.Helper()
	bitmapBenchOnce.Do(func() {
		bitmapBenchErr = doBitmapSetup()
	})
	if bitmapBenchErr != nil {
		b.Fatal(bitmapBenchErr)
	}
}

func doBitmapSetup() error {
	if err := ensureFixtureDir(); err != nil {
		return fmt.Errorf("fixture dir: %w", err)
	}

	bitmapMPHFPath = filepath.Join(fixtureDir, "bitmapindex.mphf")
	bitmapDataPath = filepath.Join(fixtureDir, "bitmapindex.bitmaps")
	bitmapRocksDBPath = filepath.Join(fixtureDir, "bitmapindex.rocksdb")
	eventstorePath := filepath.Join(fixtureDir, "bench.events")

	// Check if all bitmap index fixtures exist and are fresh.
	if bitmapBenchFixturesExist() && !fixturesStale() {
		fmt.Println("bitmap bench: using cached fixtures")
		return loadBitmapSampleKeys(eventstorePath)
	}

	// Clean stale fixture files.
	os.Remove(bitmapMPHFPath)
	os.Remove(bitmapDataPath)
	os.RemoveAll(bitmapRocksDBPath)

	// Ensure the eventstore fixture exists (reuses parallel_bench_test setup).
	if _, err := os.Stat(eventstorePath); err != nil {
		// Need to build the eventstore first.
		dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
		if dataLoadErr != nil {
			return fmt.Errorf("load events: %w", dataLoadErr)
		}
		fmt.Printf("bitmap bench: building eventstore fixture with %d events...\n", len(allEvents))
		ew, err := eventstore.Create(eventstorePath, eventstore.WriterOptions{})
		if err != nil {
			return err
		}
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				return err
			}
		}
		if err := ew.Finish(); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Build MPHF+packfile bitmap index.
	fmt.Println("bitmap bench: building MPHF+packfile index...")
	start := time.Now()
	if err := buildBitmapFromEventStore(ctx, eventstorePath, bitmapMPHFPath, bitmapDataPath); err != nil {
		return fmt.Errorf("build MPHF index: %w", err)
	}
	mphfBuildTime := time.Since(start)

	// Build RocksDB bitmap index.
	fmt.Println("bitmap bench: building RocksDB index...")
	start = time.Now()
	if err := buildRocksDBFromEventStore(ctx, eventstorePath, bitmapRocksDBPath); err != nil {
		return fmt.Errorf("build RocksDB index: %w", err)
	}
	rocksBuildTime := time.Since(start)

	// Report sizes.
	mphfInfo, _ := os.Stat(bitmapMPHFPath)
	dataInfo, _ := os.Stat(bitmapDataPath)
	rocksSize := dirSize(bitmapRocksDBPath)

	fmt.Printf("bitmap bench: MPHF %s + packfile %s = %s total\n",
		fmtKB(float64(mphfInfo.Size())),
		fmtKB(float64(dataInfo.Size())),
		fmtKB(float64(mphfInfo.Size()+dataInfo.Size())))
	fmt.Printf("bitmap bench: RocksDB %s\n", fmtKB(float64(rocksSize)))

	// Report build times.
	fmt.Printf("bitmap bench: MPHF+packfile built in %v\n", mphfBuildTime)
	fmt.Printf("bitmap bench: RocksDB built in %v\n", rocksBuildTime)

	return loadBitmapSampleKeys(eventstorePath)
}

// loadBitmapSampleKeys scans the first 50K events from the eventstore to
// extract real (field, key) pairs for benchmark lookups.
func loadBitmapSampleKeys(eventstorePath string) error {
	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		return fmt.Errorf("open eventstore for sampling: %w", err)
	}
	defer er.Close()

	seen := make(map[string]struct{}, 10000)
	sampleCount := min(50000, er.EventCount())

	for event, err := range er.ReadEvents(0, sampleCount) {
		if err != nil {
			return fmt.Errorf("read event for sampling: %w", err)
		}
		for _, f := range allFields {
			key := extractKey(event, f)
			if key == nil {
				continue
			}
			// Deduplicate by field+key.
			dedup := string(append([]byte{byte(f)}, key...))
			if _, ok := seen[dedup]; ok {
				continue
			}
			seen[dedup] = struct{}{}
			// Copy key since event slices are reused by iterator.
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			bitmapSampleKeys = append(bitmapSampleKeys, bitmapSampleKey{
				field:   f,
				key:     keyCopy,
				mphfKey: composeKey(keyCopy, f),
			})
		}
	}

	fmt.Printf("bitmap bench: %d sample keys loaded from %d events\n",
		len(bitmapSampleKeys), sampleCount)
	return nil
}

// --- Single-key lookup benchmarks ---

func BenchmarkBitmapMPHFLookup(b *testing.B) {
	setupBitmapBenchData(b)

	r, err := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	keys := bitmapSampleKeys
	b.ResetTimer()

	for i := range b.N {
		k := keys[i%len(keys)]
		bm, err := r.Lookup(k.mphfKey)
		if err != nil {
			b.Fatal(err)
		}
		_ = bm
	}
}

func BenchmarkBitmapRocksDBLookup(b *testing.B) {
	setupBitmapBenchData(b)

	r, err := openRocksDBBitmap(bitmapRocksDBPath)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	keys := bitmapSampleKeys
	b.ResetTimer()

	for i := range b.N {
		k := keys[i%len(keys)]
		bm, err := r.Lookup(k.field, k.key)
		if err != nil {
			b.Fatal(err)
		}
		_ = bm
	}
}

// --- Parallel lookup benchmarks ---

func BenchmarkBitmapMPHFParallel15(b *testing.B) {
	setupBitmapBenchData(b)

	r, err := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	keys := bitmapSampleKeys
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := keys[rng.Intn(len(keys))]
			bm, err := r.Lookup(k.mphfKey)
			if err != nil {
				b.Error(err)
				return
			}
			_ = bm
		}
	})
}

func BenchmarkBitmapMPHFLookupKeys15(b *testing.B) {
	setupBitmapBenchData(b)

	r, err := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	// Pre-generate batches of 15 keys.
	type batch [15][]byte
	const numBatches = 1024
	batches := make([]batch, numBatches)
	for i := range batches {
		for j := range 15 {
			batches[i][j] = keys[rng.Intn(len(keys))].mphfKey
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := range b.N {
		bt := &batches[i%numBatches]
		results, err := r.LookupKeys(ctx, bt[:])
		if err != nil {
			b.Fatal(err)
		}
		_ = results
	}
}

func BenchmarkBitmapRocksDBParallel15(b *testing.B) {
	setupBitmapBenchData(b)

	r, err := openRocksDBBitmap(bitmapRocksDBPath)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	keys := bitmapSampleKeys
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := keys[rng.Intn(len(keys))]
			bm, err := r.Lookup(k.field, k.key)
			if err != nil {
				b.Error(err)
				return
			}
			_ = bm
		}
	})
}
