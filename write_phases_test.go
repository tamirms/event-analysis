package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tamir/events-analysis/eventstore"
	rocksdbES "github.com/tamir/events-analysis/eventstore/rocksdb"
)

func readProcIO() string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", os.Getpid()))
	if err != nil {
		return ""
	}
	return string(data)
}

func parseProcIO(s string) map[string]int64 {
	m := make(map[string]int64)
	for _, line := range strings.Split(s, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			var val int64
			fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &val)
			m[key] = val
		}
	}
	return m
}

// TestWritePhases measures per-phase timing for packfile and RocksDB writes.
// Run each subtest in a SEPARATE process.
func TestWritePhases(t *testing.T) {
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		t.Fatal(dataLoadErr)
	}

	baseDir := os.Getenv("WRITE_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	inputMB := float64(totalRawBytes) / 1e6
	avgEventSize := totalRawBytes / len(allEvents)
	blockSize := 128 * avgEventSize

	t.Run("packfile-c8", func(t *testing.T) {
		dir, err := os.MkdirTemp(baseDir, "phase-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		p := filepath.Join(dir, "bench.events")

		io0 := parseProcIO(readProcIO())

		createStart := time.Now()
		ew, err := eventstore.Create(p, eventstore.WriterOptions{Concurrency: 8})
		if err != nil {
			t.Fatal(err)
		}
		createTime := time.Since(createStart)

		appendStart := time.Now()
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		appendTime := time.Since(appendStart)

		io1 := parseProcIO(readProcIO())

		finishStart := time.Now()
		if err := ew.Finish(); err != nil {
			t.Fatal(err)
		}
		finishTime := time.Since(finishStart)

		io2 := parseProcIO(readProcIO())

		info, _ := os.Stat(p)
		outMB := float64(info.Size()) / 1e6
		totalTime := createTime + appendTime + finishTime

		fmt.Printf("packfile c=8: total=%.2fs  in=%.0fMB/s  out=%.0fMB\n", totalTime.Seconds(), inputMB/totalTime.Seconds(), outMB)
		fmt.Printf("  create:  %v\n", createTime.Round(time.Millisecond))
		fmt.Printf("  append:  %v\n", appendTime.Round(time.Millisecond))
		fmt.Printf("  finish:  %v\n", finishTime.Round(time.Millisecond))
		fmt.Printf("  I/O during append: write_bytes=%dMB  syscw=%d\n",
			(io1["write_bytes"]-io0["write_bytes"])/(1024*1024), io1["syscw"]-io0["syscw"])
		fmt.Printf("  I/O during finish: write_bytes=%dMB  syscw=%d\n",
			(io2["write_bytes"]-io1["write_bytes"])/(1024*1024), io2["syscw"]-io1["syscw"])
	})

	runRocksDB := func(t *testing.T, name string) {
		dir, err := os.MkdirTemp(baseDir, "phase-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		dbPath := filepath.Join(dir, "rocks.db")

		io0 := parseProcIO(readProcIO())

		createStart := time.Now()
		w, err := rocksdbES.Create(dbPath, rocksdbES.WriterOptions{
			BlockSize:          blockSize,
			CompressionThreads: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		createTime := time.Since(createStart)

		appendStart := time.Now()
		for _, ev := range allEvents {
			if err := w.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		appendTime := time.Since(appendStart)

		io1 := parseProcIO(readProcIO())

		finishStart := time.Now()
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		finishTime := time.Since(finishStart)

		io2 := parseProcIO(readProcIO())

		var outBytes int64
		filepath.Walk(dbPath, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				outBytes += info.Size()
			}
			return nil
		})
		outMB := float64(outBytes) / 1e6
		totalTime := createTime + appendTime + finishTime

		fmt.Printf("%s: total=%.2fs  in=%.0fMB/s  out=%.0fMB\n", name, totalTime.Seconds(), inputMB/totalTime.Seconds(), outMB)
		fmt.Printf("  create:  %v\n", createTime.Round(time.Millisecond))
		fmt.Printf("  append:  %v\n", appendTime.Round(time.Millisecond))
		fmt.Printf("  finish:  %v\n", finishTime.Round(time.Millisecond))
		fmt.Printf("  I/O during append: write_bytes=%dMB  syscw=%d\n",
			(io1["write_bytes"]-io0["write_bytes"])/(1024*1024), io1["syscw"]-io0["syscw"])
		fmt.Printf("  I/O during finish: write_bytes=%dMB  syscw=%d\n",
			(io2["write_bytes"]-io1["write_bytes"])/(1024*1024), io2["syscw"]-io1["syscw"])
	}

	t.Run("rocksdb-t8", func(t *testing.T) {
		runRocksDB(t, "rocksdb t=8")
	})
}
