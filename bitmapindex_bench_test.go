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
	rocksdbBI "github.com/tamir/events-analysis/bitmapindex/rocksdb"
	"github.com/tamir/events-analysis/event"
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
	Field bitmapindex.Field
	Key   []byte
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
	er := eventstore.Open(eventstorePath)
	defer er.Close()

	type fieldKey struct {
		field bitmapindex.Field
		key   string
	}
	seen := make(map[fieldKey]struct{}, 10000)
	ec, err := er.EventCount()
	if err != nil {
		return fmt.Errorf("event count: %w", err)
	}
	sampleCount := min(50000, ec)

	var ev event.Event
	for data, err := range er.ReadEvents(0, sampleCount) {
		if err != nil {
			return fmt.Errorf("read event for sampling: %w", err)
		}
		if err := event.Unmarshal(data, &ev); err != nil {
			return fmt.Errorf("unmarshal event for sampling: %w", err)
		}

		// Extract all fields from the parsed event.
		type candidate struct {
			f   bitmapindex.Field
			key []byte
		}
		var candidates [5]candidate
		n := 0
		if ev.ContractID != nil {
			candidates[n] = candidate{bitmapindex.FieldContractID, ev.ContractID}
			n++
		}
		for i, topic := range ev.Topics {
			if i >= 4 {
				break
			}
			candidates[n] = candidate{bitmapindex.FieldTopic0 + bitmapindex.Field(i), topic}
			n++
		}

		for _, c := range candidates[:n] {
			fk := fieldKey{field: c.f, key: string(c.key)}
			if _, ok := seen[fk]; ok {
				continue
			}
			seen[fk] = struct{}{}
			keyCopy := make([]byte, len(c.key))
			copy(keyCopy, c.key)
			bitmapSampleKeys = append(bitmapSampleKeys, bitmapSampleKey{
				Field: c.f,
				Key:   keyCopy,
			})
		}
	}

	fmt.Printf("bitmap bench: %d sample keys loaded from %d events\n",
		len(bitmapSampleKeys), sampleCount)
	return nil
}

// --- Generic bitmap benchmark helpers ---

func benchBitmapLookup(b *testing.B, r bitmapindex.IndexReader) {
	b.Helper()
	keys := bitmapSampleKeys
	b.ResetTimer()

	for i := range b.N {
		k := keys[i%len(keys)]
		bm, err := r.Lookup(k.Field, k.Key)
		if err != nil {
			b.Fatal(err)
		}
		_ = bm
	}
}

func benchBitmapParallelLookup(b *testing.B, r bitmapindex.IndexReader) {
	b.Helper()
	keys := bitmapSampleKeys
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			k := keys[rng.Intn(len(keys))]
			bm, err := r.Lookup(k.Field, k.Key)
			if err != nil {
				b.Error(err)
				return
			}
			_ = bm
		}
	})
}

// --- Single-key lookup benchmarks ---

func BenchmarkBitmapMPHFLookup(b *testing.B) {
	setupBitmapBenchData(b)
	r := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	defer r.Close()
	benchBitmapLookup(b, r)
}

func BenchmarkBitmapRocksDBLookup(b *testing.B) {
	setupBitmapBenchData(b)
	r := rocksdbBI.Open(bitmapRocksDBPath)
	defer r.Close()
	benchBitmapLookup(b, r)
}

// --- Parallel lookup benchmarks ---

func BenchmarkBitmapMPHFParallel15(b *testing.B) {
	setupBitmapBenchData(b)
	r := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	defer r.Close()
	benchBitmapParallelLookup(b, r)
}

func BenchmarkBitmapRocksDBParallel15(b *testing.B) {
	setupBitmapBenchData(b)
	r := rocksdbBI.Open(bitmapRocksDBPath)
	defer r.Close()
	benchBitmapParallelLookup(b, r)
}

// --- LookupKeys benchmark (MPHF-specific) ---

func BenchmarkBitmapMPHFLookupKeys15(b *testing.B) {
	setupBitmapBenchData(b)

	r := bitmapindex.Open(bitmapMPHFPath, bitmapDataPath)
	defer r.Close()

	keys := bitmapSampleKeys
	rng := rand.New(rand.NewSource(42))

	// Pre-generate batches of 15 keys.
	type batch [15]bitmapindex.FieldKey
	const numBatches = 1024
	batches := make([]batch, numBatches)
	for i := range batches {
		for j := range 15 {
			k := keys[rng.Intn(len(keys))]
			batches[i][j] = bitmapindex.FieldKey{Field: k.Field, Key: k.Key}
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
