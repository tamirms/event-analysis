package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
)

// TestWriteThroughput measures end-to-end write throughput across concurrency
// levels without GOGC=1 overhead. Use WRITE_DIR to control the target device
// (defaults to os.TempDir, typically EBS).
func TestWriteThroughput(t *testing.T) {
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		t.Fatal(dataLoadErr)
	}

	baseDir := os.Getenv("WRITE_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	fmt.Println("\nPackfile:")
	for _, conc := range []int{1, 4, 8, 16, 24, 32} {
		dir, err := os.MkdirTemp(baseDir, "wp-*")
		if err != nil {
			t.Fatal(err)
		}
		p := dir + "/bench.events"

		start := time.Now()
		ew, err := eventstore.Create(p, eventstore.WriterOptions{Concurrency: conc})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := ew.Finish(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("  c=%2d: %.2fs  %4.0f MB/s\n", conc, elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}

	fmt.Println("\nPackfile (no compression, serial):")
	{
		dir, err := os.MkdirTemp(baseDir, "wp-nocomp-*")
		if err != nil {
			t.Fatal(err)
		}
		p := dir + "/bench.events"

		start := time.Now()
		ew, err := eventstore.Create(p, eventstore.WriterOptions{NoCompression: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range allEvents {
			if err := ew.Append(ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := ew.Finish(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("  %.2fs  %4.0f MB/s\n", elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}

	avgEventSize := totalRawBytes / len(allEvents)
	blockSize := 128 * avgEventSize

	fmt.Println("\nRocksDB:")
	for _, threads := range []int{1, 4, 8} {
		dir, err := os.MkdirTemp(baseDir, "rdb-*")
		if err != nil {
			t.Fatal(err)
		}
		sstFilePath := filepath.Join(dir, "ingest.sst")
		dbPath := filepath.Join(dir, "rocks.db")

		start := time.Now()

		writeOpts := grocksdb.NewDefaultOptions()
		writeOpts.SetCompression(grocksdb.ZSTDCompression)
		writeOpts.SetCompressionOptions(grocksdb.NewCompressionOptions(-14, 3, 0, 0))
		if threads > 1 {
			writeOpts.SetCompressionOptionsParallelThreads(threads)
		}
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(blockSize)
		bbto.SetBlockSizeDeviation(100)
		bbto.SetFormatVersion(5)
		bbto.SetBlockRestartInterval(128)
		writeOpts.SetBlockBasedTableFactory(bbto)

		envOpts := grocksdb.NewDefaultEnvOptions()
		sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
		if err := sfw.Open(sstFilePath); err != nil {
			t.Fatal(err)
		}
		key := make([]byte, 4)
		for i, ev := range allEvents {
			binary.BigEndian.PutUint32(key, uint32(i))
			if err := sfw.Add(key, ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := sfw.Finish(); err != nil {
			t.Fatal(err)
		}
		sfw.Destroy()
		envOpts.Destroy()
		writeOpts.Destroy()

		dbOpts := grocksdb.NewDefaultOptions()
		dbOpts.SetCreateIfMissing(true)
		dbOpts.SetCompression(grocksdb.ZSTDCompression)
		dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
		dbBbto.SetNoBlockCache(true)
		dbBbto.SetBlockSize(blockSize)
		dbBbto.SetFormatVersion(5)
		dbOpts.SetBlockBasedTableFactory(dbBbto)

		db, err := grocksdb.OpenDb(dbOpts, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
		ingestOpts.SetMoveFiles(true)
		if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
			t.Fatal(err)
		}
		ingestOpts.Destroy()
		db.Close()
		dbOpts.Destroy()

		elapsed := time.Since(start)
		fmt.Printf("  t=%2d: %.2fs  %4.0f MB/s\n", threads, elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}

	fmt.Println("\nRocksDB (no compression, serial):")
	{
		dir, err := os.MkdirTemp(baseDir, "rdb-nocomp-*")
		if err != nil {
			t.Fatal(err)
		}
		sstFilePath := filepath.Join(dir, "ingest.sst")
		dbPath := filepath.Join(dir, "rocks.db")

		start := time.Now()

		writeOpts := grocksdb.NewDefaultOptions()
		writeOpts.SetCompression(grocksdb.NoCompression)
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(blockSize)
		bbto.SetBlockSizeDeviation(100)
		bbto.SetFormatVersion(5)
		bbto.SetBlockRestartInterval(128)
		writeOpts.SetBlockBasedTableFactory(bbto)

		envOpts := grocksdb.NewDefaultEnvOptions()
		sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
		if err := sfw.Open(sstFilePath); err != nil {
			t.Fatal(err)
		}
		key := make([]byte, 4)
		for i, ev := range allEvents {
			binary.BigEndian.PutUint32(key, uint32(i))
			if err := sfw.Add(key, ev); err != nil {
				t.Fatal(err)
			}
		}
		if err := sfw.Finish(); err != nil {
			t.Fatal(err)
		}
		sfw.Destroy()
		envOpts.Destroy()
		writeOpts.Destroy()

		dbOpts := grocksdb.NewDefaultOptions()
		dbOpts.SetCreateIfMissing(true)
		dbOpts.SetCompression(grocksdb.NoCompression)
		dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
		dbBbto.SetNoBlockCache(true)
		dbBbto.SetBlockSize(blockSize)
		dbBbto.SetFormatVersion(5)
		dbOpts.SetBlockBasedTableFactory(dbBbto)

		db, err := grocksdb.OpenDb(dbOpts, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
		ingestOpts.SetMoveFiles(true)
		if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
			t.Fatal(err)
		}
		ingestOpts.Destroy()
		db.Close()
		dbOpts.Destroy()

		elapsed := time.Since(start)
		fmt.Printf("  %.2fs  %4.0f MB/s\n", elapsed.Seconds(),
			float64(totalRawBytes)/elapsed.Seconds()/1e6)

		os.RemoveAll(dir)
	}
}
