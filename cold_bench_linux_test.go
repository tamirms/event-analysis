//go:build linux

package main

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
	"golang.org/x/sys/unix"
)

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
