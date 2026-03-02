package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/tamir/events-analysis/eventstore"
	rocksdbES "github.com/tamir/events-analysis/eventstore/rocksdb"
)

var (
	setupOnce      sync.Once
	setupErr       error
	totalEvents    int
	eventstorePath string
	rocksDBPath    string

	benchSink any // prevents compiler from eliminating benchmark work
)

func setupBenchData(b *testing.B) {
	b.Helper()
	setupOnce.Do(func() {
		setupErr = doSetup()
	})
	if setupErr != nil {
		b.Fatal(setupErr)
	}
}

func mustEventCount(b *testing.B, r eventstore.StoreReader) int {
	b.Helper()
	n, err := r.EventCount()
	if err != nil {
		b.Fatalf("EventCount failed: %v", err)
	}
	if n == 0 {
		b.Fatal("EventCount returned 0 — fixture is corrupt or missing")
	}
	return n
}

func doSetup() error {
	if err := ensureFixtureDir(); err != nil {
		return fmt.Errorf("fixture dir: %w", err)
	}

	eventstorePath = filepath.Join(fixtureDir, "bench.events")
	rocksDBPath = filepath.Join(fixtureDir, "rocks.db")

	// Use cached fixtures if available and not stale.
	if benchFixturesExist() && !fixturesStale() {
		meta, err := loadMeta()
		if err != nil {
			return fmt.Errorf("load meta: %w", err)
		}
		totalEvents = meta.TotalEvents
		totalRawBytes = meta.TotalRawBytes
		fmt.Printf("bench setup: using cached fixtures (%d events, %s total raw bytes)\n",
			totalEvents, fmtKB(float64(totalRawBytes)))
		return nil
	}

	// Clean stale fixture files before regenerating.
	os.Remove(eventstorePath)
	os.RemoveAll(rocksDBPath)

	// Load events from source (via dataOnce to avoid double-load if
	// ensureAllEvents is called independently by write benchmarks).
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		return fmt.Errorf("load events: %w", dataLoadErr)
	}
	totalEvents = len(allEvents)
	fmt.Printf("bench setup: %d events loaded, %s total raw bytes\n",
		totalEvents, fmtKB(float64(totalRawBytes)))

	avgEventSize := totalRawBytes / totalEvents
	rocksBlockSize := 128 * avgEventSize
	fmt.Printf("bench setup: avg event size=%dB, RocksDB block size=%s\n",
		avgEventSize, fmtKB(float64(rocksBlockSize)))

	// Write eventstore (packfile with event-level access).
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

	// Write RocksDB eventstore via new package.
	rw, err := rocksdbES.Create(rocksDBPath, rocksdbES.WriterOptions{
		BlockSize: rocksBlockSize,
	})
	if err != nil {
		return fmt.Errorf("rocksdb create: %w", err)
	}
	for _, ev := range allEvents {
		if err := rw.Append(ev); err != nil {
			return fmt.Errorf("rocksdb append: %w", err)
		}
	}
	if err := rw.Finish(); err != nil {
		return fmt.Errorf("rocksdb finish: %w", err)
	}

	// Report sizes.
	esInfo, _ := os.Stat(eventstorePath)
	rocksSize := dirSize(rocksDBPath)
	fmt.Printf("bench setup: eventstore %s, rocksdb %s\n",
		fmtKB(float64(esInfo.Size())), fmtKB(float64(rocksSize)))

	// Save metadata for cache validation.
	meta, err := buildMeta()
	if err != nil {
		return fmt.Errorf("build meta: %w", err)
	}
	if err := saveMeta(meta); err != nil {
		return fmt.Errorf("save meta: %w", err)
	}

	return nil
}

func dirSize(path string) int64 {
	var total int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// --- Sequential read benchmarks ---

func benchSeqRead(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for b.Loop() {
		count := 0
		for ev, err := range r.ReadEvents(0, n) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
			count++
		}
		if count == 0 {
			b.Fatal("ReadEvents yielded 0 items")
		}
	}
}

func BenchmarkPackfileSeqRead(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchSeqRead(b, er, n)
}

func BenchmarkRocksDBSeqRead(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchSeqRead(b, r, n)
}

// --- Generic read benchmarks (via StoreReader interface) ---

func benchRandomRead(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for b.Loop() {
		idx := rng.Intn(n)
		ev, err := r.ReadEvent(idx)
		if err != nil {
			b.Fatal(err)
		}
		benchSink = ev
	}
}

func benchReadIndices(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for b.Loop() {
		indices := generateScatteredIndices(rng, numIndices, n)
		count := 0
		for ev, err := range r.ReadIndices(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
			count++
		}
		if count != numIndices {
			b.Fatalf("ReadIndices yielded %d items, want %d", count, numIndices)
		}
	}
}

// benchReadIndicesConsecutive reads indices that span consecutive records,
// exercising the batch I/O path in ReadItems.
func benchReadIndicesConsecutive(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	const blockSize = 128
	const numBlocks = 1000
	startBlock := 100
	indices := make([]int, numBlocks)
	for i := range indices {
		indices[i] = (startBlock + i) * blockSize
	}
	// Clamp to valid range.
	for i := range indices {
		if indices[i] >= n {
			indices = indices[:i]
			break
		}
	}
	if len(indices) == 0 {
		b.Fatal("no valid indices — fixture has fewer events than expected")
	}
	b.ResetTimer()

	for b.Loop() {
		count := 0
		for ev, err := range r.ReadIndices(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
			count++
		}
		if count != len(indices) {
			b.Fatalf("ReadIndices yielded %d items, want %d", count, len(indices))
		}
	}
}

func benchParallelRead(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			idx := rng.Intn(n)
			ev, err := r.ReadEvent(idx)
			if err != nil {
				b.Error(err)
				return
			}
			_ = ev
		}
	})
}

func benchParallelReadIndices(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	const numIndices = 50
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, n)
			count := 0
			for ev, err := range r.ReadIndices(context.Background(), indices) {
				if err != nil {
					b.Error(err)
					return
				}
				_ = ev
				count++
			}
			if count != numIndices {
				b.Errorf("ReadIndices yielded %d items, want %d", count, numIndices)
				return
			}
		}
	})
}

// --- Random point-read benchmarks ---

func BenchmarkPackfileRandomRead(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchRandomRead(b, er, n)
}

func BenchmarkRocksDBRandomRead(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchRandomRead(b, r, n)
}

// --- Parallel read benchmarks ---

func BenchmarkPackfileParallelRead(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchParallelRead(b, er, n)
}

func BenchmarkRocksDBParallelRead(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchParallelRead(b, r, n)
}

// --- Batch 128 benchmarks ---

func benchReadBatch128(b *testing.B, r eventstore.StoreReader) {
	b.Helper()
	batchSize := min(128, totalEvents)
	b.ResetTimer()

	for b.Loop() {
		count := 0
		for ev, err := range r.ReadEvents(0, batchSize) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
			count++
		}
		if count != batchSize {
			b.Fatalf("ReadEvents yielded %d items, want %d", count, batchSize)
		}
	}
}

func BenchmarkPackfileReadBatch128(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	benchReadBatch128(b, er)
}

func BenchmarkRocksDBReadBatch128(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	benchReadBatch128(b, r)
}

// --- Range scan from random offset benchmarks ---

func benchRangeScan128(b *testing.B, r eventstore.StoreReader, n int) {
	b.Helper()
	const scanLen = 128
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for b.Loop() {
		start := rng.Intn(n - scanLen)
		count := 0
		for ev, err := range r.ReadEvents(start, scanLen) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
			count++
		}
		if count != scanLen {
			b.Fatalf("ReadEvents yielded %d items, want %d", count, scanLen)
		}
	}
}

func BenchmarkPackfileRangeScan128(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchRangeScan128(b, er, n)
}

func BenchmarkRocksDBRangeScan128(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchRangeScan128(b, r, n)
}

// --- Scattered read benchmarks (ReadIndices) ---

// generateScatteredIndices returns n sorted unique random indices in [0, total).
func generateScatteredIndices(rng *rand.Rand, n, total int) []int {
	m := make(map[int]struct{}, n)
	for len(m) < n {
		m[rng.Intn(total)] = struct{}{}
	}
	indices := make([]int, 0, n)
	for idx := range m {
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	return indices
}

func BenchmarkPackfileReadIndices(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchReadIndices(b, er, n)
}

func BenchmarkRocksDBReadIndices(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchReadIndices(b, r, n)
}

func BenchmarkPackfileReadIndicesConsecutive(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchReadIndicesConsecutive(b, er, n)
}

func BenchmarkRocksDBReadIndicesConsecutive(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchReadIndicesConsecutive(b, r, n)
}

func BenchmarkPackfileParallelReadIndices(b *testing.B) {
	setupBenchData(b)
	er := eventstore.Open(eventstorePath)
	defer er.Close()
	n := mustEventCount(b, er)
	benchParallelReadIndices(b, er, n)
}

func BenchmarkRocksDBParallelReadIndices(b *testing.B) {
	setupBenchData(b)
	r := rocksdbES.Open(rocksDBPath)
	defer r.Close()
	n := mustEventCount(b, r)
	benchParallelReadIndices(b, r, n)
}

// --- Open latency benchmarks (backend-specific) ---

func BenchmarkPackfileOpen(b *testing.B) {
	setupBenchData(b)
	b.ResetTimer()

	for b.Loop() {
		r := eventstore.Open(eventstorePath)
		r.Close()
	}
}

func BenchmarkRocksDBOpen(b *testing.B) {
	setupBenchData(b)
	b.ResetTimer()

	for b.Loop() {
		r := rocksdbES.Open(rocksDBPath)
		r.Close()
	}
}

// --- Write / ingestion benchmarks ---

func ensureAllEvents(b *testing.B) {
	b.Helper()
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		b.Fatal(dataLoadErr)
	}
}

func benchPackfileWrite(b *testing.B, concurrency int) {
	ensureAllEvents(b)

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for b.Loop() {
		p := filepath.Join(b.TempDir(), "bench.events")
		ew, err := eventstore.Create(p, eventstore.WriterOptions{Concurrency: concurrency})
		if err != nil {
			b.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				b.Fatal(err)
			}
		}
		if err := ew.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPackfileWrite(b *testing.B) {
	benchPackfileWrite(b, 0)
}

func BenchmarkPackfileWriteParallel4(b *testing.B) {
	benchPackfileWrite(b, 4)
}

func BenchmarkPackfileWriteParallel8(b *testing.B) {
	benchPackfileWrite(b, 8)
}

func BenchmarkPackfileWriteParallel16(b *testing.B) {
	benchPackfileWrite(b, 16)
}

func BenchmarkPackfileWriteParallel24(b *testing.B) {
	benchPackfileWrite(b, 24)
}

func BenchmarkPackfileWriteParallel32(b *testing.B) {
	benchPackfileWrite(b, 32)
}

// rocksDBWriteCore contains the shared write logic for throughput and memory benchmarks.
func rocksDBWriteCore(b *testing.B, parallelComp int) {
	dir := b.TempDir()
	dbPath := filepath.Join(dir, "rocks.db")

	avgEventSize := totalRawBytes / len(allEvents)
	w, err := rocksdbES.Create(dbPath, rocksdbES.WriterOptions{
		BlockSize:          128 * avgEventSize,
		CompressionThreads: parallelComp,
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, ev := range allEvents {
		if err := w.Append(ev); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		b.Fatal(err)
	}
}

func benchRocksDBWrite(b *testing.B, parallelComp int) {
	ensureAllEvents(b)

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for b.Loop() {
		rocksDBWriteCore(b, parallelComp)
	}
}

func BenchmarkRocksDBWrite(b *testing.B) {
	benchRocksDBWrite(b, 1)
}

func BenchmarkRocksDBWriteParallel4(b *testing.B) {
	benchRocksDBWrite(b, 4)
}

func BenchmarkRocksDBWriteParallel8(b *testing.B) {
	benchRocksDBWrite(b, 8)
}
