//go:build linux

package main

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
)

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

// --- Write memory benchmarks ---

func benchPackfileWriteMemory(b *testing.B, concurrency int) {
	ensureAllEvents(b)

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

func benchRocksDBWriteMemory(b *testing.B, parallelComp int) {
	ensureAllEvents(b)

	var peakDelta int64
	for range b.N {
		peakDelta = peakRssAnonDelta(func() {
			rocksDBWriteCore(b, parallelComp)
		})
	}
	b.ReportMetric(float64(peakDelta)/(1<<20), "peak-delta-MB")
}

func BenchmarkPackfileWriteMemory(b *testing.B)          { benchPackfileWriteMemory(b, 0) }
func BenchmarkPackfileWriteMemoryParallel8(b *testing.B) { benchPackfileWriteMemory(b, 8) }
func BenchmarkRocksDBWriteMemory(b *testing.B)           { benchRocksDBWriteMemory(b, 1) }
func BenchmarkRocksDBWriteMemoryParallel8(b *testing.B)  { benchRocksDBWriteMemory(b, 8) }

// --- Read memory benchmarks ---

func BenchmarkPackfileSeqReadMemory(b *testing.B) {
	setupBenchData(b)

	var peakDelta int64
	for range b.N {
		peakDelta = peakRssAnonDelta(func() {
			er := eventstore.Open(eventstorePath)
			ec, _ := er.EventCount()
			for ev, err := range er.ReadEvents(0, ec) {
				if err != nil {
					b.Fatal(err)
				}
				_ = ev
			}
			er.Close()
		})
	}
	b.ReportMetric(float64(peakDelta)/(1<<20), "peak-delta-MB")
}

func benchPackfileReadIndicesMemory(b *testing.B, concurrency int) {
	setupBenchData(b)

	er := eventstore.Open(eventstorePath, eventstore.WithConcurrency(concurrency))
	defer er.Close()

	n, _ := er.EventCount()
	const numIndices = 1000
	rng := rand.New(rand.NewSource(42))
	indices := generateScatteredIndices(rng, numIndices, n)

	var peakDelta int64
	for range b.N {
		peakDelta = peakRssAnonDelta(func() {
			for ev, err := range er.ReadIndices(context.Background(), indices) {
				if err != nil {
					b.Fatal(err)
				}
				_ = ev
			}
		})
	}
	b.ReportMetric(float64(peakDelta)/(1<<20), "peak-delta-MB")
}

func BenchmarkPackfileReadIndicesMemory(b *testing.B)   { benchPackfileReadIndicesMemory(b, 8) }
func BenchmarkPackfileReadIndicesMemory1(b *testing.B)  { benchPackfileReadIndicesMemory(b, 1) }
func BenchmarkPackfileReadIndicesMemory32(b *testing.B) { benchPackfileReadIndicesMemory(b, 32) }
