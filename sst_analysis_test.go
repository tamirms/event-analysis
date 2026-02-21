package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/packfile"
)

// TestBatchVsSSTAnalysis produces all numbers for batch-vs-sstable-analysis.md.
// It compares packfile (batch) and RocksDB SSTable file sizes across multiple
// block sizes, and extracts SST internal properties via GetProperty.
func TestBatchVsSSTAnalysis(t *testing.T) {
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		t.Fatal(dataLoadErr)
	}

	numEvents := len(allEvents)
	avgEventSize := totalRawBytes / numEvents

	fmt.Printf("\nDataset: %d events, %s total raw, avg %dB/event\n\n",
		numEvents, fmtKB(float64(totalRawBytes)), avgEventSize)

	baseDir := os.TempDir()
	dir, err := os.MkdirTemp(baseDir, "sst-analysis-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	blockSizes := []int{32, 64, 128, 256, 512}

	type batchResult struct {
		n              int
		rawBlockSize   int // N * avgEventSize
		fileSize       int64
		indexSize      int64 // inter-batch index (from packfile trailer)
		compressedData int64 // file - index - metadata(12B) - trailer(32B)
		forIndexBytes  int64 // total intra-batch FOR-N overhead (summed)
	}

	type sstResult struct {
		blockSizeArg   int // block_size parameter passed to RocksDB
		sstFileSize    int64
		numDataBlocks  int64
		rawKeySize     int64
		rawValueSize   int64
		dataBlockSize  int64
		indexBlockSize int64
	}

	var batchResults []batchResult
	var sstResults []sstResult

	// --- Batch (packfile) measurements ---
	fmt.Println("=== Batch (Packfile) Measurements ===")
	fmt.Printf("%-8s %12s %12s %12s %12s %8s\n",
		"N", "FileSize", "CompData", "InterIdx", "FORIdx", "Ratio")

	for _, n := range blockSizes {
		esPath := filepath.Join(dir, fmt.Sprintf("batch_%d.events", n))
		ew, err := eventstore.Create(esPath, eventstore.WriterOptions{BlockSize: n})
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

		// Open via packfile to get trailer info.
		pr, err := packfile.Open(esPath)
		if err != nil {
			t.Fatal(err)
		}
		trailer := pr.Trailer()
		pr.Close()

		fi, err := os.Stat(esPath)
		if err != nil {
			t.Fatal(err)
		}

		// Compute intra-batch FOR-N overhead by manually encoding.
		var totalFOR int64
		for start := 0; start < numEvents; start += n {
			end := start + n
			if end > numEvents {
				end = numEvents
			}
			sizes := make([]uint32, end-start)
			for j := start; j < end; j++ {
				sizes[j-start] = uint32(len(allEvents[j]))
			}
			encoded := packfile.EncodeGroup(sizes)
			totalFOR += int64(len(encoded))
		}

		compData := fi.Size() - int64(trailer.IndexSize) - int64(trailer.MetadataSize) - 32 // 32 = trailerSize
		ratio := float64(totalRawBytes) / float64(fi.Size())

		br := batchResult{
			n:              n,
			rawBlockSize:   n * avgEventSize,
			fileSize:       fi.Size(),
			indexSize:      int64(trailer.IndexSize),
			compressedData: compData,
			forIndexBytes:  totalFOR,
		}
		batchResults = append(batchResults, br)

		fmt.Printf("N=%-5d %12s %12s %12s %12s %7.2fx\n",
			n, fmtKB(float64(fi.Size())), fmtKB(float64(compData)),
			fmtKB(float64(trailer.IndexSize)), fmtKB(float64(totalFOR)), ratio)

		os.Remove(esPath)
	}

	// --- RocksDB SSTable measurements ---
	fmt.Printf("\n=== RocksDB SSTable Measurements ===\n")
	fmt.Printf("%-10s %12s %10s %12s %12s %12s %8s\n",
		"BlkSize", "SSTSize", "DataBlks", "RawKeySize", "RawValSize", "IdxBlkSize", "Ratio")

	for _, n := range blockSizes {
		sstBlockSize := n * avgEventSize
		sstPath := filepath.Join(dir, fmt.Sprintf("sst_%d.sst", n))
		dbPath := filepath.Join(dir, fmt.Sprintf("rocks_%d.db", n))

		// Write SST file.
		writeOpts := grocksdb.NewDefaultOptions()
		writeOpts.SetCompression(grocksdb.ZSTDCompression)
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockSize(sstBlockSize)
		bbto.SetBlockSizeDeviation(100) // fill blocks completely
		bbto.SetBlockRestartInterval(1024)
		writeOpts.SetBlockBasedTableFactory(bbto)

		envOpts := grocksdb.NewDefaultEnvOptions()
		sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
		if err := sfw.Open(sstPath); err != nil {
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

		// Measure SST file size before ingest.
		sstInfo, err := os.Stat(sstPath)
		if err != nil {
			t.Fatal(err)
		}

		// Ingest into DB to query properties.
		dbOpts := grocksdb.NewDefaultOptions()
		dbOpts.SetCreateIfMissing(true)
		dbOpts.SetCompression(grocksdb.ZSTDCompression)
		dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
		dbBbto.SetNoBlockCache(true)
		dbBbto.SetBlockSize(sstBlockSize)
		dbBbto.SetBlockSizeDeviation(100)
		dbBbto.SetBlockRestartInterval(1024)
		dbOpts.SetBlockBasedTableFactory(dbBbto)

		db, err := grocksdb.OpenDb(dbOpts, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
		if err := db.IngestExternalFile([]string{sstPath}, ingestOpts); err != nil {
			t.Fatal(err)
		}
		ingestOpts.Destroy()

		// Query aggregated table properties.
		props := db.GetProperty("rocksdb.aggregated-table-properties")
		db.Close()
		dbOpts.Destroy()

		sr := sstResult{
			blockSizeArg: sstBlockSize,
			sstFileSize:  sstInfo.Size(),
		}
		sr.numDataBlocks = parsePropInt(props, "# data blocks")
		sr.rawKeySize = parsePropInt(props, "raw key size")
		sr.rawValueSize = parsePropInt(props, "raw value size")
		sr.dataBlockSize = parsePropInt(props, "data block size")
		sr.indexBlockSize = parsePropInt(props, "index block size")

		sstResults = append(sstResults, sr)

		ratio := float64(totalRawBytes) / float64(sstInfo.Size())
		fmt.Printf("~%-6sKB %12s %10d %12s %12s %12s %7.2fx\n",
			fmt.Sprintf("%.1f", float64(sstBlockSize)/1024),
			fmtKB(float64(sstInfo.Size())),
			sr.numDataBlocks,
			fmtKB(float64(sr.rawKeySize)),
			fmtKB(float64(sr.rawValueSize)),
			fmtKB(float64(sr.indexBlockSize)),
			ratio)

		os.RemoveAll(dbPath)
		os.Remove(sstPath)
	}

	// --- Comparison table ---
	fmt.Printf("\n=== Batch vs SSTable Comparison (equal uncompressed block size) ===\n")
	fmt.Printf("%-30s %12s %12s %12s %8s\n",
		"Method", "Compressed", "InterIdx", "Total", "Ratio")
	fmt.Printf("%-30s %12s %12s %12s %8s\n",
		strings.Repeat("-", 30), strings.Repeat("-", 12), strings.Repeat("-", 12),
		strings.Repeat("-", 12), strings.Repeat("-", 8))

	for i, n := range blockSizes {
		br := batchResults[i]
		sr := sstResults[i]

		batchRatio := float64(totalRawBytes) / float64(br.fileSize)
		sstRatio := float64(totalRawBytes) / float64(sr.sstFileSize)

		fmt.Printf("Batch N=%-4d (~%.1fKB raw)    %12s %12s %12s %7.2fx\n",
			n, float64(br.rawBlockSize)/1024,
			fmtKB(float64(br.compressedData)),
			fmtKB(float64(br.indexSize)),
			fmtKB(float64(br.fileSize)),
			batchRatio)
		fmt.Printf("SST   bs=%.1fKB              %12s %12s %12s %7.2fx\n",
			float64(sr.blockSizeArg)/1024,
			fmtKB(float64(sr.sstFileSize)),
			"—",
			fmtKB(float64(sr.sstFileSize)),
			sstRatio)
		fmt.Println()
	}

	// --- Delta summary for N=256 ---
	idx256 := 3 // blockSizes[3] == 256
	br256 := batchResults[idx256]
	sr256 := sstResults[idx256]
	delta := sr256.sstFileSize - br256.fileSize
	pct := float64(delta) / float64(br256.fileSize) * 100
	fmt.Printf("N=256 delta: %s (%.1f%%)\n", fmtKB(float64(delta)), pct)

	// --- SST layout breakdown (N=128, canonical block size) ---
	fmt.Printf("\n=== SST Layout Breakdown (N=128, bs=%dB) ===\n", sstResults[2].blockSizeArg)
	sr128 := sstResults[2]
	fmt.Printf("File size:        %12s  (100.0%%)\n", fmtKB(float64(sr128.sstFileSize)))
	fmt.Printf("Data blocks (compressed): %s  [%d blocks]\n",
		fmtKB(float64(sr128.dataBlockSize)), sr128.numDataBlocks)
	fmt.Printf("Index blocks:     %12s\n", fmtKB(float64(sr128.indexBlockSize)))
	fmt.Printf("Raw key size:     %12s  (%.1f B/entry)\n",
		fmtKB(float64(sr128.rawKeySize)), float64(sr128.rawKeySize)/float64(numEvents))
	fmt.Printf("Raw value size:   %12s  (%.1f B/entry)\n",
		fmtKB(float64(sr128.rawValueSize)), float64(sr128.rawValueSize)/float64(numEvents))

	// Per-entry overhead analysis.
	userKeyBytes := int64(numEvents) * 4
	internalTrailerBytes := int64(numEvents) * 8
	totalKeyBytes := userKeyBytes + internalTrailerBytes
	fmt.Printf("\nPer-entry key overhead:\n")
	fmt.Printf("  User key bytes:          %12s  (%.1f B/entry)\n",
		fmtKB(float64(userKeyBytes)), float64(userKeyBytes)/float64(numEvents))
	fmt.Printf("  Internal trailer bytes:  %12s  (%.1f B/entry)\n",
		fmtKB(float64(internalTrailerBytes)), float64(internalTrailerBytes)/float64(numEvents))
	fmt.Printf("  Total key bytes:         %12s  (%.1f B/entry)\n",
		fmtKB(float64(totalKeyBytes)), float64(totalKeyBytes)/float64(numEvents))
	fmt.Printf("  Key %% of raw input:      %.1f%%\n",
		float64(totalKeyBytes)/float64(totalRawBytes)*100)

	// --- Compression ratio analysis ---
	fmt.Printf("\n=== Compression Ratio Analysis (N=256) ===\n")
	br := batchResults[idx256]
	sr := sstResults[idx256]
	batchRaw := int64(totalRawBytes) + br.forIndexBytes
	sstRaw := int64(totalRawBytes) + totalKeyBytes // values + keys
	// Use total file sizes for both to keep the comparison symmetric.
	batchRatioOnRaw := float64(batchRaw) / float64(br.fileSize)
	sstRatioOnRaw := float64(sstRaw) / float64(sr.sstFileSize)
	fmt.Printf("Batch: raw input %s (events + %s FOR index) → file %s = %.2fx\n",
		fmtKB(float64(batchRaw)), fmtKB(float64(br.forIndexBytes)),
		fmtKB(float64(br.fileSize)), batchRatioOnRaw)
	fmt.Printf("SST:   raw input %s (events + %s keys) → file %s = %.2fx\n",
		fmtKB(float64(sstRaw)), fmtKB(float64(totalKeyBytes)),
		fmtKB(float64(sr.sstFileSize)), sstRatioOnRaw)

	// --- Gap explained ---
	fmt.Printf("\n=== Gap Analysis ===\n")
	extraRaw := totalKeyBytes - br.forIndexBytes
	avgRatio := (batchRatioOnRaw + sstRatioOnRaw) / 2
	predictedGap := float64(extraRaw) / avgRatio
	observedGap := float64(sr.sstFileSize - br.fileSize)
	fmt.Printf("Extra raw metadata: %s keys - %s FOR = %s\n",
		fmtKB(float64(totalKeyBytes)), fmtKB(float64(br.forIndexBytes)),
		fmtKB(float64(extraRaw)))
	fmt.Printf("Avg compression ratio: %.2fx\n", avgRatio)
	fmt.Printf("Predicted gap: %s / %.2f = %s\n",
		fmtKB(float64(extraRaw)), avgRatio, fmtKB(predictedGap))
	fmt.Printf("Observed gap:  %s\n", fmtKB(observedGap))

	fmt.Println("\nDone.")
}

// parsePropInt extracts a numeric value from RocksDB's aggregated table properties string.
// The format is semicolon-delimited fields like "# data blocks=12345; raw key size=67890;".
// Some keys have extra annotations, e.g. "index block size (user-key? 1, delta-value? 1)=1097".
func parsePropInt(props, key string) int64 {
	// Match "key" optionally followed by parenthetical annotation, then "=value".
	// Use [^=;]* to avoid overshooting past semicolon-delimited boundaries.
	pattern := regexp.MustCompile(regexp.QuoteMeta(key) + `[^=;]*=\s*(\d+)`)
	m := pattern.FindStringSubmatch(props)
	if m != nil {
		v, _ := strconv.ParseInt(m[1], 10, 64)
		return v
	}
	return 0
}
