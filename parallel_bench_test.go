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

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
)

var (
	setupOnce      sync.Once
	setupErr       error
	totalEvents    int
	eventstorePath string
	sstPath        string
	rocksDBPath    string
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
	sstPath = filepath.Join(fixtureDir, "bench.sst")
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
	os.Remove(sstPath)
	os.RemoveAll(rocksDBPath)

	// Load events from source.
	if err := loadAllEvents(); err != nil {
		return fmt.Errorf("load events: %w", err)
	}
	totalEvents = len(allEvents)
	fmt.Printf("bench setup: %d events loaded, %s total raw bytes\n",
		totalEvents, fmtKB(float64(totalRawBytes)))

	avgEventSize := totalRawBytes / totalEvents
	sstBlockSize := 128 * avgEventSize
	fmt.Printf("bench setup: avg event size=%dB, SST/RocksDB block size=%s\n",
		avgEventSize, fmtKB(float64(sstBlockSize)))

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

	// Write SSTable: individual events as KV pairs with zstd block compression.
	sf, err := vfs.Default.Create(sstPath)
	if err != nil {
		return err
	}
	writable := objstorageprovider.NewFileWritable(sf)
	w := sstable.NewWriter(writable, sstable.WriterOptions{
		BlockSize:            sstBlockSize,
		Compression:          sstable.ZstdCompression,
		BlockSizeThreshold:   100,
		BlockRestartInterval: 1024,
	})
	key := make([]byte, 4)
	for i, ev := range allEvents {
		binary.BigEndian.PutUint32(key, uint32(i))
		if err := w.Set(key, ev); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}

	// Write RocksDB: individual events via SSTFileWriter.
	sstFilePath := filepath.Join(fixtureDir, "rocks_ingest.sst")

	sstWriteOpts := grocksdb.NewDefaultOptions()
	sstWriteOpts.SetCompression(grocksdb.ZSTDCompression)
	sstBbto := grocksdb.NewDefaultBlockBasedTableOptions()
	sstBbto.SetBlockSize(sstBlockSize)
	sstWriteOpts.SetBlockBasedTableFactory(sstBbto)

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
	dbBbto.SetBlockSize(sstBlockSize)
	dbOpts.SetBlockBasedTableFactory(dbBbto)

	rdb, err := grocksdb.OpenDb(dbOpts, rocksDBPath)
	if err != nil {
		return fmt.Errorf("open rocksdb: %w", err)
	}
	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
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
	sstInfo, _ := os.Stat(sstPath)
	rocksSize := dirSize(rocksDBPath)
	fmt.Printf("bench setup: eventstore %s, sstable %s, rocksdb %s\n",
		fmtKB(float64(esInfo.Size())), fmtKB(float64(sstInfo.Size())), fmtKB(float64(rocksSize)))

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
	opts := grocksdb.NewDefaultOptions()
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	opts.SetBlockBasedTableFactory(bbto)
	db, err := grocksdb.OpenDbForReadOnly(opts, rocksDBPath, false)
	if err != nil {
		b.Fatal(err)
	}
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

func BenchmarkSSTSeqRead(b *testing.B) {
	setupBenchData(b)

	f, err := os.Open(sstPath)
	if err != nil {
		b.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			_ = val.ValueOrHandle
		}
		iter.Close()
	}
}

func BenchmarkRocksDBSeqRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		it := db.NewIterator(ro)
		it.SeekToFirst()
		for ; it.Valid(); it.Next() {
			_ = it.Value().Data()
		}
		it.Close()
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
		_ = ev
	}
}

func BenchmarkSSTRandomRead(b *testing.B) {
	setupBenchData(b)

	f, err := os.Open(sstPath)
	if err != nil {
		b.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer iter.Close()

	b.ResetTimer()

	for range b.N {
		idx := rng.Intn(totalEvents)
		binary.BigEndian.PutUint32(key, uint32(idx))
		k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
		if k == nil {
			b.Fatalf("SeekGE(%d): key not found", idx)
		}
		_ = val.ValueOrHandle
	}
}

func BenchmarkRocksDBRandomRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
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
		_ = val.Data()
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
				b.Fatal(err)
			}
			_ = ev
		}
	})
}

func BenchmarkSSTParallelRead(b *testing.B) {
	setupBenchData(b)

	f, err := os.Open(sstPath)
	if err != nil {
		b.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		key := make([]byte, 4)

		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()

		for pb.Next() {
			idx := rng.Intn(totalEvents)
			binary.BigEndian.PutUint32(key, uint32(idx))
			k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
			if k == nil {
				b.Fatalf("SeekGE(%d): key not found", idx)
			}
			_ = val.ValueOrHandle
		}
	})
}

func BenchmarkRocksDBParallelRead(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()

		rng := rand.New(rand.NewSource(rand.Int63()))
		key := make([]byte, 4)

		for pb.Next() {
			idx := rng.Intn(totalEvents)
			binary.BigEndian.PutUint32(key, uint32(idx))
			val, err := db.GetPinned(ro, key)
			if err != nil {
				b.Fatal(err)
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

func BenchmarkSSTReadBatch128(b *testing.B) {
	setupBenchData(b)

	f, err := os.Open(sstPath)
	if err != nil {
		b.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	batchSize := min(128, totalEvents)

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer iter.Close()

	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		binary.BigEndian.PutUint32(key, uint32(0))
		k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
		if k == nil {
			b.Fatal("SeekGE(0): key not found")
		}
		_ = val.ValueOrHandle
		for i := 1; i < batchSize; i++ {
			k, val = iter.Next()
			if k == nil {
				b.Fatalf("Next at %d: unexpected end", i)
			}
			_ = val.ValueOrHandle
		}
	}
}

func BenchmarkRocksDBReadBatch128(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	batchSize := min(128, totalEvents)

	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		binary.BigEndian.PutUint32(key, uint32(0))
		it := db.NewIterator(ro)
		it.Seek(key)
		if !it.Valid() {
			b.Fatal("Seek(0): key not found")
		}
		_ = it.Value().Data()
		for i := 1; i < batchSize; i++ {
			it.Next()
			if !it.Valid() {
				b.Fatalf("Next at %d: unexpected end", i)
			}
			_ = it.Value().Data()
		}
		it.Close()
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

func BenchmarkSSTReadIndices(b *testing.B) {
	setupBenchData(b)

	f, err := os.Open(sstPath)
	if err != nil {
		b.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		b.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer iter.Close()

	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, totalEvents)
		for _, idx := range indices {
			binary.BigEndian.PutUint32(key, uint32(idx))
			k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
			if k == nil {
				b.Fatalf("SeekGE(%d): key not found", idx)
			}
			_ = val.ValueOrHandle
		}
	}
}

func BenchmarkRocksDBReadIndices(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, totalEvents)
		for _, idx := range indices {
			binary.BigEndian.PutUint32(key, uint32(idx))
			val, err := db.GetPinned(ro, key)
			if err != nil {
				b.Fatal(err)
			}
			_ = val.Data()
			val.Destroy()
		}
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
					b.Fatal(err)
				}
				_ = ev
			}
		}
	})
}

func BenchmarkRocksDBParallelReadIndices(b *testing.B) {
	setupBenchData(b)

	const numIndices = 50

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		db := openRocksDB(b)
		defer db.Close()
		ro := grocksdb.NewDefaultReadOptions()
		defer ro.Destroy()

		rng := rand.New(rand.NewSource(rand.Int63()))
		key := make([]byte, 4)
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, totalEvents)
			for _, idx := range indices {
				binary.BigEndian.PutUint32(key, uint32(idx))
				val, err := db.GetPinned(ro, key)
				if err != nil {
					b.Fatal(err)
				}
				_ = val.Data()
				val.Destroy()
			}
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

func BenchmarkSSTOpen(b *testing.B) {
	setupBenchData(b)
	b.ResetTimer()

	for range b.N {
		f, err := os.Open(sstPath)
		if err != nil {
			b.Fatal(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
		if err != nil {
			b.Fatal(err)
		}
		reader.Close()
	}
}

func BenchmarkRocksDBOpen(b *testing.B) {
	setupBenchData(b)
	b.ResetTimer()

	for range b.N {
		opts := grocksdb.NewDefaultOptions()
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetNoBlockCache(true)
		opts.SetBlockBasedTableFactory(bbto)
		db, err := grocksdb.OpenDbForReadOnly(opts, rocksDBPath, false)
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
}

// --- Write / ingestion benchmarks ---

func ensureAllEvents(b *testing.B) {
	b.Helper()
	sstOnce.Do(func() { sstDataErr = loadAllEvents() })
	if sstDataErr != nil {
		b.Fatal(sstDataErr)
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

func BenchmarkSSTWrite(b *testing.B) {
	ensureAllEvents(b)

	avgEventSize := totalRawBytes / len(allEvents)
	blockSize := 128 * avgEventSize

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		p := filepath.Join(b.TempDir(), "bench.sst")
		f, err := vfs.Default.Create(p)
		if err != nil {
			b.Fatal(err)
		}
		writable := objstorageprovider.NewFileWritable(f)
		w := sstable.NewWriter(writable, sstable.WriterOptions{
			BlockSize:            blockSize,
			Compression:          sstable.ZstdCompression,
			BlockSizeThreshold:   100,
			BlockRestartInterval: 1024,
		})
		key := make([]byte, 4)
		for i, ev := range allEvents {
			binary.BigEndian.PutUint32(key, uint32(i))
			if err := w.Set(key, ev); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchRocksDBWrite(b *testing.B, parallelComp int) {
	ensureAllEvents(b)

	avgEventSize := totalRawBytes / len(allEvents)
	blockSize := 128 * avgEventSize

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	for range b.N {
		dir := b.TempDir()
		sstFilePath := filepath.Join(dir, "ingest.sst")
		dbPath := filepath.Join(dir, "rocks.db")

		// Write SST file via SSTFileWriter.
		writeOpts := grocksdb.NewDefaultOptions()
		writeOpts.SetCompression(grocksdb.ZSTDCompression)
		if parallelComp > 1 {
			writeOpts.SetCompressionOptionsParallelThreads(parallelComp)
		}
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(blockSize)
		writeOpts.SetBlockBasedTableFactory(bbto)

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
		dbBbto.SetBlockSize(blockSize)
		dbOpts.SetBlockBasedTableFactory(dbBbto)

		db, err := grocksdb.OpenDb(dbOpts, dbPath)
		if err != nil {
			b.Fatal(err)
		}
		ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
		if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
			b.Fatal(err)
		}
		ingestOpts.Destroy()
		db.Close()
		dbOpts.Destroy()
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
