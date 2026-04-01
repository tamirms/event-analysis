//go:build linux

package main

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/tamir/events-analysis/eventstore"
	rocksdbES "github.com/tamir/events-analysis/eventstore/rocksdb"
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

func benchColdPackfileReadPositions(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numReads = 1000
	const blockSize = 128

	esPath := filepath.Join(coldFixtureDir(), "bench.events")
	if _, err := os.Stat(esPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", esPath)
	}

	rng := rand.New(rand.NewSource(42))
	indices := coldScatteredIndices(rng, numReads, totalEvents, blockSize)

	for b.Loop() {
		b.StopTimer()
		if err := dropFileCache(esPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		er := eventstore.Open(esPath, eventstore.WithConcurrency(concurrency))
		for ev, err := range er.ReadPositions(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
		er.Close()
	}
}

func BenchmarkColdPackfileReadPositions1(b *testing.B)   { benchColdPackfileReadPositions(b, 1) }
func BenchmarkColdPackfileReadPositions4(b *testing.B)   { benchColdPackfileReadPositions(b, 4) }
func BenchmarkColdPackfileReadPositions8(b *testing.B)   { benchColdPackfileReadPositions(b, 8) }
func BenchmarkColdPackfileReadPositions16(b *testing.B)  { benchColdPackfileReadPositions(b, 16) }
func BenchmarkColdPackfileReadPositions32(b *testing.B)  { benchColdPackfileReadPositions(b, 32) }
func BenchmarkColdPackfileReadPositions64(b *testing.B)  { benchColdPackfileReadPositions(b, 64) }
func BenchmarkColdPackfileReadPositions128(b *testing.B) { benchColdPackfileReadPositions(b, 128) }

func benchColdRocksDBReadPositions(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numReads = 1000
	const blockSize = 128

	rdbPath := filepath.Join(coldFixtureDir(), "rocks.db")
	if _, err := os.Stat(rdbPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", rdbPath)
	}

	rng := rand.New(rand.NewSource(42))
	indices := coldScatteredIndices(rng, numReads, totalEvents, blockSize)

	for b.Loop() {
		b.StopTimer()
		if err := dropDirCache(rdbPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		r := rocksdbES.Open(rdbPath, rocksdbES.WithConcurrency(concurrency))
		for ev, err := range r.ReadPositions(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
		r.Close()
	}
}

func BenchmarkColdRocksDBReadPositions1(b *testing.B)  { benchColdRocksDBReadPositions(b, 1) }
func BenchmarkColdRocksDBReadPositions8(b *testing.B)  { benchColdRocksDBReadPositions(b, 8) }
func BenchmarkColdRocksDBReadPositions32(b *testing.B) { benchColdRocksDBReadPositions(b, 32) }
func BenchmarkColdRocksDBReadPositions64(b *testing.B) { benchColdRocksDBReadPositions(b, 64) }

// coldConsecutiveIndices generates indices that span numBlocks consecutive blocks,
// each picking one event per block starting from startBlock.
func coldConsecutiveIndices(numBlocks, startBlock, totalEvents, blockSize int) []int {
	indices := make([]int, 0, numBlocks)
	for i := range numBlocks {
		idx := (startBlock + i) * blockSize
		if idx >= totalEvents {
			break
		}
		indices = append(indices, idx)
	}
	return indices
}

// coldMixedIndices generates a mix of ~50% consecutive runs and ~50% scattered indices.
func coldMixedIndices(rng *rand.Rand, n, totalEvents, blockSize int) []int {
	totalBlocks := totalEvents / blockSize
	half := n / 2

	// First half: consecutive run starting at a random block.
	startBlock := rng.Intn(totalBlocks - half)
	indices := make([]int, 0, n)
	for i := range half {
		indices = append(indices, (startBlock+i)*blockSize)
	}

	// Second half: scattered random blocks (avoiding the consecutive range).
	scattered := make(map[int]struct{}, n-half)
	for len(scattered) < n-half {
		blk := rng.Intn(totalBlocks)
		if blk >= startBlock && blk < startBlock+half {
			continue
		}
		scattered[blk] = struct{}{}
	}
	for blk := range scattered {
		indices = append(indices, blk*blockSize+rng.Intn(blockSize))
	}

	sort.Ints(indices)
	return indices
}

func benchColdPackfileReadPositionsConsecutive(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numBlocks = 1000
	const blockSize = 128
	const startBlock = 100

	esPath := filepath.Join(coldFixtureDir(), "bench.events")
	if _, err := os.Stat(esPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", esPath)
	}

	indices := coldConsecutiveIndices(numBlocks, startBlock, totalEvents, blockSize)

	for b.Loop() {
		b.StopTimer()
		if err := dropFileCache(esPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		er := eventstore.Open(esPath, eventstore.WithConcurrency(concurrency))
		for ev, err := range er.ReadPositions(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
		er.Close()
	}
}

func BenchmarkColdPackfileReadPositionsConsec1(b *testing.B)  { benchColdPackfileReadPositionsConsecutive(b, 1) }
func BenchmarkColdPackfileReadPositionsConsec8(b *testing.B)  { benchColdPackfileReadPositionsConsecutive(b, 8) }
func BenchmarkColdPackfileReadPositionsConsec32(b *testing.B) { benchColdPackfileReadPositionsConsecutive(b, 32) }

func benchColdPackfileReadPositionsMixed(b *testing.B, concurrency int) {
	setupBenchData(b)

	const numReads = 1000
	const blockSize = 128

	esPath := filepath.Join(coldFixtureDir(), "bench.events")
	if _, err := os.Stat(esPath); err != nil {
		b.Fatalf("fixture not found: %s (set COLD_FIXTURES_DIR for alternate location)", esPath)
	}

	rng := rand.New(rand.NewSource(42))
	indices := coldMixedIndices(rng, numReads, totalEvents, blockSize)

	for b.Loop() {
		b.StopTimer()
		if err := dropFileCache(esPath); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		er := eventstore.Open(esPath, eventstore.WithConcurrency(concurrency))
		for ev, err := range er.ReadPositions(context.Background(), indices) {
			if err != nil {
				b.Fatal(err)
			}
			_ = ev
		}
		er.Close()
	}
}

func BenchmarkColdPackfileReadPositionsMixed8(b *testing.B) { benchColdPackfileReadPositionsMixed(b, 8) }
