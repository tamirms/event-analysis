package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
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

	// Write RocksDB: individual events via SSTFileWriter.
	key := make([]byte, 4)
	sstFilePath := filepath.Join(fixtureDir, "rocks_ingest.sst")

	sstWriteOpts := grocksdb.NewDefaultOptions()
	sstWriteOpts.SetCompression(grocksdb.ZSTDCompression)
	sstWriteOpts.SetCompressionOptions(grocksdb.NewCompressionOptions(-14, 3, 0, 0))
	sstBbto := grocksdb.NewDefaultBlockBasedTableOptions()
	sstBbto.SetBlockSize(rocksBlockSize)
	sstBbto.SetBlockSizeDeviation(100)
	sstBbto.SetFormatVersion(5)
	sstBbto.SetBlockRestartInterval(128)
	sstWriteOpts.SetBlockBasedTableFactory(sstBbto)
	sstBbto.Destroy()

	envOpts := grocksdb.NewDefaultEnvOptions()
	sfw := grocksdb.NewSSTFileWriter(envOpts, sstWriteOpts)
	if err := sfw.Open(sstFilePath); err != nil {
		return fmt.Errorf("sst file writer open: %w", err)
	}
	for i, ev := range allEvents {
		binary.BigEndian.PutUint32(key, uint32(i))
		if err := sfw.Add(key, ev); err != nil {
			return fmt.Errorf("sst file writer add: %w", err)
		}
	}
	if err := sfw.Finish(); err != nil {
		return fmt.Errorf("sst file writer finish: %w", err)
	}
	sfw.Destroy()
	envOpts.Destroy()
	sstWriteOpts.Destroy()

	// Create DB and ingest the single SST file.
	dbOpts := grocksdb.NewDefaultOptions()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCompression(grocksdb.ZSTDCompression)
	dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
	dbBbto.SetNoBlockCache(true)
	dbBbto.SetBlockSize(rocksBlockSize)
	dbOpts.SetBlockBasedTableFactory(dbBbto)
	dbBbto.Destroy()

	rdb, err := grocksdb.OpenDb(dbOpts, rocksDBPath)
	if err != nil {
		return fmt.Errorf("open rocksdb: %w", err)
	}
	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	ingestOpts.SetMoveFiles(true)
	if err := rdb.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
		rdb.Close()
		return fmt.Errorf("rocksdb ingest: %w", err)
	}
	ingestOpts.Destroy()
	rdb.Close()
	dbOpts.Destroy()
	os.Remove(sstFilePath) // clean up intermediate file

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

func openRocksDB(b *testing.B) *grocksdb.DB {
	b.Helper()
	db, opts, err := openReadOnlyRocksDB(rocksDBPath)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { opts.Destroy() })
	return db
}


// --- Sequential read benchmarks ---

func BenchmarkPackfileSeqRead(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		for ev, err := range er.ReadEvents(0, n) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
	}
}

func BenchmarkRocksDBSeqRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := newBenchReadOptions()
	defer ro.Destroy()

	it := db.NewIterator(ro)
	defer it.Close()

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		it.SeekToFirst()
		for ; it.Valid(); it.Next() {
			_ = it.ValueSlice().Data()
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Random point-read benchmarks ---

func BenchmarkPackfileRandomRead(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		idx := rng.Intn(n)
		ev, err := er.ReadEvent(idx)
		if err != nil {
			b.Fatal(err)
		}
		benchSink = ev
	}
}

func BenchmarkRocksDBRandomRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := newBenchReadOptions()
	defer ro.Destroy()

	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		idx := rng.Intn(totalEvents)
		binary.BigEndian.PutUint32(key, uint32(idx))
		val, err := db.GetPinned(ro, key)
		if err != nil {
			b.Fatal(err)
		}
		benchSink = val.Data()
		val.Destroy()
	}
}

// --- Parallel read benchmarks ---

func BenchmarkPackfileParallelRead(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			idx := rng.Intn(n)
			ev, err := er.ReadEvent(idx)
			if err != nil {
				b.Error(err)
				return
			}
			_ = ev
		}
	})
}

func BenchmarkRocksDBParallelRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ro := newBenchReadOptions()
		defer ro.Destroy()

		rng := rand.New(rand.NewSource(rand.Int63()))
		key := make([]byte, 4)

		for pb.Next() {
			idx := rng.Intn(totalEvents)
			binary.BigEndian.PutUint32(key, uint32(idx))
			val, err := db.GetPinned(ro, key)
			if err != nil {
				b.Error(err)
				return
			}
			_ = val.Data()
			val.Destroy()
		}
	})
}

// --- Batch 128 benchmarks ---

func BenchmarkPackfileReadBatch128(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	batchSize := min(128, totalEvents)

	b.ResetTimer()

	for range b.N {
		for ev, err := range er.ReadEvents(0, batchSize) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
	}
}

func BenchmarkRocksDBReadBatch128(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := newBenchReadOptions()
	defer ro.Destroy()

	batchSize := min(128, totalEvents)

	it := db.NewIterator(ro)
	defer it.Close()

	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		binary.BigEndian.PutUint32(key, uint32(0))
		it.Seek(key)
		if !it.Valid() {
			b.Fatal("Seek(0): key not found")
		}
		_ = it.ValueSlice().Data()
		for i := 1; i < batchSize; i++ {
			it.Next()
			if !it.Valid() {
				b.Fatalf("Next at %d: unexpected end", i)
			}
			_ = it.ValueSlice().Data()
		}
	}
	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
}

// --- Range scan from random offset benchmarks ---

func BenchmarkPackfileRangeScan128(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	const scanLen = 128
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		start := rng.Intn(n - scanLen)
		for ev, err := range er.ReadEvents(start, scanLen) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
	}
}

func BenchmarkRocksDBRangeScan128(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := newBenchReadOptions()
	defer ro.Destroy()

	it := db.NewIterator(ro)
	defer it.Close()

	const scanLen = 128
	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		start := rng.Intn(totalEvents - scanLen)
		binary.BigEndian.PutUint32(key, uint32(start))
		it.Seek(key)
		if !it.Valid() {
			b.Fatalf("Seek(%d): key not found", start)
		}
		_ = it.ValueSlice().Data()
		for i := 1; i < scanLen; i++ {
			it.Next()
			if !it.Valid() {
				b.Fatalf("Next at %d: unexpected end", i)
			}
			_ = it.ValueSlice().Data()
		}
	}
	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
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
	sort.Ints(indices)
	return indices
}

func BenchmarkPackfileReadIndices(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	// 50 scattered indices per call — typical query fan-out
	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, n)
		for ev, err := range er.ReadIndices(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
	}
}

func BenchmarkPackfileReadEventParallel50(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, n)
		var wg sync.WaitGroup
		wg.Add(numIndices)
		for _, idx := range indices {
			go func(idx int) {
				defer wg.Done()
				ev, err := er.ReadEvent(idx)
				if err != nil {
					b.Error(err)
				}
				_ = ev
			}(idx)
		}
		wg.Wait()
	}
}

func BenchmarkPackfileReadEventSeq50(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, n)
		for _, idx := range indices {
			ev, err := er.ReadEvent(idx)
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
	}
}

// rocksDBParallelMultiGet splits keys across goroutines for parallel I/O,
// matching packfile ReadIndices' internal goroutine parallelism.
func rocksDBParallelMultiGet(b *testing.B, db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, ro *grocksdb.ReadOptions, keys [][]byte, concurrency int) {
	b.Helper()
	if concurrency <= 1 || len(keys) < concurrency {
		vals, err := db.BatchedMultiGetCF(ro, cf, true, keys...)
		if err != nil {
			b.Fatal(err)
		}
		for _, v := range vals {
			_ = v.Data()
		}
		vals.Destroy()
		return
	}
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	perWorker := (len(keys) + concurrency - 1) / concurrency
	for i := range concurrency {
		lo := i * perWorker
		hi := min(lo+perWorker, len(keys))
		if lo >= len(keys) {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			vals, err := db.BatchedMultiGetCF(ro, cf, true, keys[lo:hi]...)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			for _, v := range vals {
				_ = v.Data()
			}
			vals.Destroy()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
}

func BenchmarkRocksDBReadIndices(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	cf := db.GetDefaultColumnFamily()
	ro := newBenchReadOptions()
	defer ro.Destroy()

	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	keys := make([][]byte, numIndices)
	keyBuf := make([]byte, numIndices*4)
	for i := range keys {
		keys[i] = keyBuf[i*4 : i*4+4]
	}
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, totalEvents)
		for i, idx := range indices {
			binary.BigEndian.PutUint32(keys[i], uint32(idx))
		}
		rocksDBParallelMultiGet(b, db, cf, ro, keys, 8)
	}
}

func BenchmarkPackfileParallelReadIndices(b *testing.B) {
	setupBenchData(b)

	er, err := eventstore.Open(eventstorePath)
	if err != nil {
		b.Fatal(err)
	}
	defer er.Close()

	n := er.EventCount()
	const numIndices = 50
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, n)
			for ev, err := range er.ReadIndices(context.Background(), indices) {
				if err != nil {
					b.Error(err)
					return
				}
				_ = ev
			}
		}
	})
}

func BenchmarkRocksDBParallelReadIndices(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	cf := db.GetDefaultColumnFamily()
	ro := newBenchReadOptions()
	defer ro.Destroy()

	const numIndices = 50

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		keys := make([][]byte, numIndices)
		keyBuf := make([]byte, numIndices*4)
		for i := range keys {
			keys[i] = keyBuf[i*4 : i*4+4]
		}
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, totalEvents)
			for i, idx := range indices {
				binary.BigEndian.PutUint32(keys[i], uint32(idx))
			}
			rocksDBParallelMultiGet(b, db, cf, ro, keys, 8)
		}
	})
}

// --- Open latency benchmarks ---

func BenchmarkPackfileOpen(b *testing.B) {
	setupBenchData(b)
	b.ResetTimer()

	for range b.N {
		r, err := eventstore.Open(eventstorePath)
		if err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}

func BenchmarkRocksDBOpen(b *testing.B) {
	setupBenchData(b)

	opts := grocksdb.NewDefaultOptions()
	opts.SetSkipStatsUpdateOnDBOpen(true)
	opts.SetSkipCheckingSSTFileSizesOnDBOpen(true)
	opts.SetMaxFileOpeningThreads(1)
	opts.SetDisableAutoCompactions(true)
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	bbto.SetFormatVersion(5)
	opts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()

	b.ResetTimer()
	for range b.N {
		db, err := grocksdb.OpenDbForReadOnly(opts, rocksDBPath, false)
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
	b.StopTimer()
	opts.Destroy()
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

	for range b.N {
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
	sstFilePath := filepath.Join(dir, "ingest.sst")
	dbPath := filepath.Join(dir, "rocks.db")

	// Write SST file via SSTFileWriter.
	writeOpts := grocksdb.NewDefaultOptions()
	writeOpts.SetCompression(grocksdb.ZSTDCompression)
	writeOpts.SetCompressionOptions(grocksdb.NewCompressionOptions(-14, 3, 0, 0))
	if parallelComp > 1 {
		writeOpts.SetCompressionOptionsParallelThreads(parallelComp)
	}
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	avgEventSize := totalRawBytes / len(allEvents)
	bbto.SetBlockSize(128 * avgEventSize)
	bbto.SetBlockSizeDeviation(100)
	bbto.SetFormatVersion(5)
	bbto.SetBlockRestartInterval(128)
	writeOpts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()

	envOpts := grocksdb.NewDefaultEnvOptions()
	sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
	if err := sfw.Open(sstFilePath); err != nil {
		b.Fatal(err)
	}
	key := make([]byte, 4)
	for i, ev := range allEvents {
		binary.BigEndian.PutUint32(key, uint32(i))
		if err := sfw.Add(key, ev); err != nil {
			b.Fatal(err)
		}
	}
	if err := sfw.Finish(); err != nil {
		b.Fatal(err)
	}
	sfw.Destroy()
	envOpts.Destroy()
	writeOpts.Destroy()

	// Open DB and ingest.
	dbOpts := grocksdb.NewDefaultOptions()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCompression(grocksdb.ZSTDCompression)
	dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
	dbBbto.SetNoBlockCache(true)
	dbBbto.SetBlockSize(128 * avgEventSize)
	dbOpts.SetBlockBasedTableFactory(dbBbto)
	dbBbto.Destroy()

	db, err := grocksdb.OpenDb(dbOpts, dbPath)
	if err != nil {
		b.Fatal(err)
	}
	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	ingestOpts.SetMoveFiles(true)
	if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
		b.Fatal(err)
	}
	ingestOpts.Destroy()
	db.Close()
	dbOpts.Destroy()
}

func benchRocksDBWrite(b *testing.B, parallelComp int) {
	ensureAllEvents(b)

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
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
