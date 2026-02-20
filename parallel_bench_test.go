package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
	"golang.org/x/sys/unix"
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

func BenchmarkSSTRangeScan128(b *testing.B) {
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

	const scanLen = 128
	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer iter.Close()

	b.ResetTimer()

	for range b.N {
		start := rng.Intn(totalEvents - scanLen)
		binary.BigEndian.PutUint32(key, uint32(start))
		k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
		if k == nil {
			b.Fatalf("SeekGE(%d): key not found", start)
		}
		_ = val.ValueOrHandle
		for i := 1; i < scanLen; i++ {
			k, val = iter.Next()
			if k == nil {
				b.Fatalf("Next at %d: unexpected end", i)
			}
			_ = val.ValueOrHandle
		}
	}
}

func BenchmarkRocksDBRangeScan128(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	const scanLen = 128
	rng := rand.New(rand.NewSource(42))
	key := make([]byte, 4)
	b.ResetTimer()

	for range b.N {
		start := rng.Intn(totalEvents - scanLen)
		binary.BigEndian.PutUint32(key, uint32(start))
		it := db.NewIterator(ro)
		it.Seek(key)
		if !it.Valid() {
			b.Fatalf("Seek(%d): key not found", start)
		}
		_ = it.Value().Data()
		for i := 1; i < scanLen; i++ {
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

// sstParallelSeekGE splits indices across goroutines for parallel SSTable lookups,
// matching packfile ReadIndices' internal goroutine parallelism.
func sstParallelSeekGE(b *testing.B, reader *sstable.Reader, indices []int, concurrency int) {
	b.Helper()
	if concurrency <= 1 || len(indices) < concurrency {
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		key := make([]byte, 4)
		for _, idx := range indices {
			binary.BigEndian.PutUint32(key, uint32(idx))
			k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
			if k == nil {
				b.Fatalf("SeekGE(%d): key not found", idx)
			}
			_ = val.ValueOrHandle
		}
		iter.Close()
		return
	}
	var wg sync.WaitGroup
	perWorker := (len(indices) + concurrency - 1) / concurrency
	for i := range concurrency {
		lo := i * perWorker
		hi := min(lo+perWorker, len(indices))
		if lo >= len(indices) {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			iter, err := reader.NewIter(nil, nil)
			if err != nil {
				b.Fatal(err)
			}
			key := make([]byte, 4)
			for _, idx := range indices[lo:hi] {
				binary.BigEndian.PutUint32(key, uint32(idx))
				k, val := iter.SeekGE(key, sstable.SeekGEFlags(0))
				if k == nil {
					b.Fatalf("SeekGE(%d): key not found", idx)
				}
				_ = val.ValueOrHandle
			}
			iter.Close()
		}()
	}
	wg.Wait()
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

	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, totalEvents)
		sstParallelSeekGE(b, reader, indices, 8)
	}
}

// rocksDBParallelMultiGet splits keys across goroutines for parallel I/O,
// matching packfile ReadIndices' internal goroutine parallelism.
func rocksDBParallelMultiGet(b *testing.B, db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, keys [][]byte, concurrency int) {
	b.Helper()
	if concurrency <= 1 || len(keys) < concurrency {
		ro := grocksdb.NewDefaultReadOptions()
		ro.SetVerifyChecksums(false)
		ro.SetFillCache(false)
		vals, err := db.BatchedMultiGetCF(ro, cf, true, keys...)
		if err != nil {
			b.Fatal(err)
		}
		for _, v := range vals {
			_ = v.Data()
		}
		vals.Destroy()
		ro.Destroy()
		return
	}
	var wg sync.WaitGroup
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
			ro := grocksdb.NewDefaultReadOptions()
			ro.SetVerifyChecksums(false)
			ro.SetFillCache(false)
			vals, err := db.BatchedMultiGetCF(ro, cf, true, keys[lo:hi]...)
			if err != nil {
				b.Fatal(err)
			}
			for _, v := range vals {
				_ = v.Data()
			}
			vals.Destroy()
			ro.Destroy()
		}()
	}
	wg.Wait()
}

func BenchmarkRocksDBReadIndices(b *testing.B) {
	setupBenchData(b)

	db := openRocksDB(b)
	defer db.Close()

	cf := db.GetDefaultColumnFamily()

	const numIndices = 50
	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for range b.N {
		indices := generateScatteredIndices(rng, numIndices, totalEvents)
		keys := make([][]byte, len(indices))
		for i, idx := range indices {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(idx))
			keys[i] = k
		}
		rocksDBParallelMultiGet(b, db, cf, keys, 8)
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

func BenchmarkSSTParallelReadIndices(b *testing.B) {
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
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, totalEvents)
			sstParallelSeekGE(b, reader, indices, 8)
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

		cf := db.GetDefaultColumnFamily()

		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			indices := generateScatteredIndices(rng, numIndices, totalEvents)
			keys := make([][]byte, len(indices))
			for i, idx := range indices {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(idx))
				keys[i] = k
			}
			rocksDBParallelMultiGet(b, db, cf, keys, 8)
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

// readRssAnon reads the anonymous RSS (heap+stack, excluding page cache) in bytes.
// Use with GODEBUG=madvdontneed=1 for accurate readings after GC.
func readRssAnon() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "RssAnon:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseInt(fields[1], 10, 64)
				return kb * 1024
			}
		}
	}
	return 0
}

// peakRssAnonDelta runs fn while sampling RssAnon every 5ms.
// Returns peak RssAnon minus baseline. Sets GOGC=1 during fn to
// minimize GC headroom so the delta reflects actual working memory.
func peakRssAnonDelta(fn func()) int64 {
	runtime.GC()
	debug.FreeOSMemory()
	time.Sleep(50 * time.Millisecond)

	prev := debug.SetGCPercent(1) // aggressive GC during measurement
	defer debug.SetGCPercent(prev)

	baseline := readRssAnon()
	var peak atomic.Int64
	peak.Store(baseline)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				v := readRssAnon()
				for {
					old := peak.Load()
					if v <= old || peak.CompareAndSwap(old, v) {
						break
					}
				}
			}
		}
	}()

	fn()

	close(done)
	// Final sample.
	v := readRssAnon()
	for {
		old := peak.Load()
		if v <= old || peak.CompareAndSwap(old, v) {
			break
		}
	}
	return peak.Load() - baseline
}

func benchPackfileWrite(b *testing.B, concurrency int) {
	ensureAllEvents(b)

	b.SetBytes(int64(totalRawBytes))
	b.ResetTimer()

	var peakDelta int64
	for range b.N {
		p := filepath.Join(b.TempDir(), "bench.events")
		peakDelta = peakRssAnonDelta(func() {
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
		})
	}
	b.ReportMetric(float64(peakDelta)/(1<<20), "peak-delta-MB")
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

	var peakDelta int64
	for range b.N {
		dir := b.TempDir()
		sstFilePath := filepath.Join(dir, "ingest.sst")
		dbPath := filepath.Join(dir, "rocks.db")

		peakDelta = peakRssAnonDelta(func() {
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
		})
	}
	b.ReportMetric(float64(peakDelta)/(1<<20), "peak-delta-MB")
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

// --- Cold cache scattered read benchmarks ---

// dropFileCache evicts a file's pages from the kernel page cache via posix_fadvise.
func dropFileCache(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return unix.Fadvise(int(f.Fd()), 0, fi.Size(), unix.FADV_DONTNEED)
}

// dropDirCache evicts all files in a directory from the page cache.
func dropDirCache(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := dropFileCache(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// coldScatteredIndices generates n indices, each on a different block of blockSize events.
func coldScatteredIndices(rng *rand.Rand, n, totalEvents, blockSize int) []int {
	totalBlocks := totalEvents / blockSize
	blocks := rng.Perm(totalBlocks)[:n]
	sort.Ints(blocks)
	indices := make([]int, n)
	for i, blk := range blocks {
		indices[i] = blk*blockSize + rng.Intn(blockSize)
	}
	return indices
}

// coldFixtureDir returns the fixture directory for cold cache benchmarks.
// Set COLD_FIXTURES_DIR to override (e.g., /tmp/ebs-fixtures for EBS testing).
func coldFixtureDir() string {
	if d := os.Getenv("COLD_FIXTURES_DIR"); d != "" {
		return d
	}
	return fixtureDir
}

func benchColdPackfileReadIndices(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numReads = 1000
	const blockSize = 128

	esPath := filepath.Join(coldFixtureDir(), "bench.events")
	if _, err := os.Stat(esPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", esPath)
	}

	rng := rand.New(rand.NewSource(42))
	indices := coldScatteredIndices(rng, numReads, totalEvents, blockSize)

	for range b.N {
		b.StopTimer()
		if err := dropFileCache(esPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		er, err := eventstore.Open(esPath, eventstore.WithConcurrency(concurrency))
		if err != nil {
			b.Fatal(err)
		}
		for ev, err := range er.ReadIndices(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
		er.Close()
	}
}

func BenchmarkColdPackfileReadIndices1(b *testing.B)   { benchColdPackfileReadIndices(b, 1) }
func BenchmarkColdPackfileReadIndices4(b *testing.B)   { benchColdPackfileReadIndices(b, 4) }
func BenchmarkColdPackfileReadIndices(b *testing.B)    { benchColdPackfileReadIndices(b, 8) }
func BenchmarkColdPackfileReadIndices16(b *testing.B)  { benchColdPackfileReadIndices(b, 16) }
func BenchmarkColdPackfileReadIndices32(b *testing.B)  { benchColdPackfileReadIndices(b, 32) }
func BenchmarkColdPackfileReadIndices64(b *testing.B)  { benchColdPackfileReadIndices(b, 64) }
func BenchmarkColdPackfileReadIndices128(b *testing.B) { benchColdPackfileReadIndices(b, 128) }

// openOptimizedRocksDB opens a RocksDB with all applicable optimizations for
// scattered point reads: skipped stats/size checks, no block cache.
func openOptimizedRocksDB(path string) (*grocksdb.DB, *grocksdb.Options, error) {
	opts := grocksdb.NewDefaultOptions()
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	bbto.SetFormatVersion(5)
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetSkipStatsUpdateOnDBOpen(true)
	opts.SetSkipCheckingSSTFileSizesOnDBOpen(true)
	opts.SetMaxFileOpeningThreads(1)
	opts.SetDisableAutoCompactions(true)
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}
	return db, opts, nil
}

func benchColdRocksDBReadIndices(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numReads = 1000
	const blockSize = 128

	rdbPath := filepath.Join(coldFixtureDir(), "rocks.db")
	if _, err := os.Stat(rdbPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", rdbPath)
	}

	rng := rand.New(rand.NewSource(42))
	indices := coldScatteredIndices(rng, numReads, totalEvents, blockSize)

	keys := make([][]byte, numReads)
	for i, idx := range indices {
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k, uint32(idx))
		keys[i] = k
	}

	for range b.N {
		b.StopTimer()
		if err := dropDirCache(rdbPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		db, dbOpts, err := openOptimizedRocksDB(rdbPath)
		if err != nil {
			b.Fatal(err)
		}
		cf := db.GetDefaultColumnFamily()

		rocksDBParallelMultiGet(b, db, cf, keys, concurrency)

		db.Close()
		dbOpts.Destroy()
	}
}

func BenchmarkColdRocksDBReadIndices(b *testing.B)   { benchColdRocksDBReadIndices(b, 1) }
func BenchmarkColdRocksDBReadIndices8(b *testing.B)  { benchColdRocksDBReadIndices(b, 8) }
func BenchmarkColdRocksDBReadIndices32(b *testing.B) { benchColdRocksDBReadIndices(b, 32) }
func BenchmarkColdRocksDBReadIndices64(b *testing.B) { benchColdRocksDBReadIndices(b, 64) }

