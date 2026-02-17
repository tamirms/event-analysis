package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/klauspost/compress/zstd"
)

// --- Data loading (separate from index bench) ---

var (
	sstOnce       sync.Once
	allEvents     [][]byte // each element is one binary-encoded event
	totalRawBytes int
	sstDataErr    error
)

func ensureSSTData(t *testing.T) {
	t.Helper()
	sstOnce.Do(func() { sstDataErr = loadAllEvents() })
	if sstDataErr != nil {
		t.Fatalf("event loading failed: %v", sstDataErr)
	}
}

func loadAllEvents() error {
	reader, err := NewChunkReader("006016.index", "006016.data")
	if err != nil {
		return fmt.Errorf("open chunk: %w", err)
	}
	defer reader.Close()

	numLedgers := reader.NumLedgers()
	fmt.Printf("loading events from %d ledgers...\n", numLedgers)

	flat := make([]byte, 0, 512*1024)
	var eventBuf []IngestEvent

	for i := range numLedgers {
		if i > 0 && i%2000 == 0 {
			fmt.Printf("  ledger %d/%d, events=%d\n", i, numLedgers, len(allEvents))
		}
		ledgerBytes, err := reader.ReadLedger(i)
		if err != nil {
			continue
		}
		eventBuf, err = ExtractEvents(ledgerBytes, eventBuf)
		if err != nil {
			continue
		}
		for idx := range eventBuf {
			flat = AppendBinaryEvent(flat[:0], &eventBuf[idx])
			eventCopy := make([]byte, len(flat))
			copy(eventCopy, flat)
			allEvents = append(allEvents, eventCopy)
			totalRawBytes += len(eventCopy)
		}
	}

	fmt.Printf("  loaded %d events, %s total raw bytes\n", len(allEvents), fmtKB(float64(totalRawBytes)))
	return nil
}

// --- SSTable Compression Benchmark ---

type sstConfig struct {
	label             string
	blockSize         int
	blockSizeThresh   int // percentage, 0 = default (90)
	restartInterval   int // 0 = default (16)
	keySize           int // bytes for ordinal key, 0 = default (4)
}

type sstResult struct {
	label    string
	fileSize int64
	ratio    float64
}

var sstSeq int

func writeSSTFile(t *testing.T, sstPath string, cfg sstConfig) {
	t.Helper()
	f, err := vfs.Default.Create(sstPath)
	if err != nil {
		t.Fatalf("create %s: %v", sstPath, err)
	}
	writable := objstorageprovider.NewFileWritable(f)

	opts := sstable.WriterOptions{
		BlockSize:   cfg.blockSize,
		Compression: sstable.ZstdCompression,
	}
	if cfg.blockSizeThresh > 0 {
		opts.BlockSizeThreshold = cfg.blockSizeThresh
	}
	if cfg.restartInterval > 0 {
		opts.BlockRestartInterval = cfg.restartInterval
	}

	w := sstable.NewWriter(writable, opts)

	keySize := cfg.keySize
	if keySize == 0 {
		keySize = 4
	}
	key := make([]byte, keySize)
	for i, ev := range allEvents {
		binary.BigEndian.PutUint32(key[keySize-4:], uint32(i))
		if err := w.Set(key, ev); err != nil {
			t.Fatalf("write event %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer (%s): %v", cfg.label, err)
	}
}

func inspectSST(t *testing.T, tmpDir string, blockSize int) {
	t.Helper()
	sstSeq++
	sstPath := filepath.Join(tmpDir, fmt.Sprintf("inspect_%d.sst", sstSeq))

	writeSSTFile(t, sstPath, sstConfig{
		label:           fmt.Sprintf("inspect/bs=%s", fmtKB(float64(blockSize))),
		blockSize:       blockSize,
		blockSizeThresh: 100,
		restartInterval: 1024,
	})

	// Open the file and inspect layout
	f, err := os.Open(sstPath)
	if err != nil {
		t.Fatalf("open %s: %v", sstPath, err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		t.Fatalf("simple readable %s: %v", sstPath, err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		t.Fatalf("new reader %s: %v", sstPath, err)
	}
	defer reader.Close()

	layout, err := reader.Layout()
	if err != nil {
		t.Fatalf("layout %s: %v", sstPath, err)
	}

	props := reader.Properties
	fileInfo, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat %s: %v", sstPath, err)
	}
	fileSize := fileInfo.Size()

	// Sum compressed data block sizes
	var dataCompressed uint64
	for _, bh := range layout.Data {
		dataCompressed += bh.BlockHandle.Length
	}

	// Sum index block sizes
	var indexCompressed uint64
	for _, bh := range layout.Index {
		indexCompressed += bh.Length
	}

	// Top-level index (only present for two-level indexes)
	topIndexSize := layout.TopIndex.Length
	filterSize := layout.Filter.Length
	propertiesSize := layout.Properties.Length
	metaIndexSize := layout.MetaIndex.Length
	footerSize := layout.Footer.Length

	// Per-block compression header (1 byte type + 4 byte checksum) per block
	numBlocks := uint64(len(layout.Data) + len(layout.Index))
	if topIndexSize > 0 {
		numBlocks++
	}
	if filterSize > 0 {
		numBlocks++
	}
	numBlocks += 2 // properties + meta-index always present
	otherSize := uint64(fileSize) - dataCompressed - indexCompressed - topIndexSize -
		filterSize - propertiesSize - metaIndexSize - footerSize

	fmt.Printf("\n  BlockSize=%s (threshold=100%%, restart=1024)\n", fmtKB(float64(blockSize)))
	fmt.Printf("  File size:        %10s  (100.0%%)\n", fmtKB(float64(fileSize)))
	fmt.Printf("  ─────────────────────────────────────────\n")
	fmt.Printf("  Data blocks:      %10s  (%5.1f%%)  [%d blocks]\n",
		fmtKB(float64(dataCompressed)), pct(dataCompressed, uint64(fileSize)), len(layout.Data))
	fmt.Printf("  Index blocks:     %10s  (%5.1f%%)  [%d blocks]\n",
		fmtKB(float64(indexCompressed)), pct(indexCompressed, uint64(fileSize)), len(layout.Index))
	if topIndexSize > 0 {
		fmt.Printf("  Top-level index:  %10s  (%5.1f%%)\n",
			fmtKB(float64(topIndexSize)), pct(topIndexSize, uint64(fileSize)))
	}
	if filterSize > 0 {
		fmt.Printf("  Filter:           %10s  (%5.1f%%)\n",
			fmtKB(float64(filterSize)), pct(filterSize, uint64(fileSize)))
	}
	fmt.Printf("  Properties:       %10s  (%5.1f%%)\n",
		fmtKB(float64(propertiesSize)), pct(propertiesSize, uint64(fileSize)))
	fmt.Printf("  Meta-index:       %10s  (%5.1f%%)\n",
		fmtKB(float64(metaIndexSize)), pct(metaIndexSize, uint64(fileSize)))
	fmt.Printf("  Footer:           %10s  (%5.1f%%)\n",
		fmtKB(float64(footerSize)), pct(footerSize, uint64(fileSize)))
	fmt.Printf("  Other (trailers): %10s  (%5.1f%%)  [~%d block trailers × 5B]\n",
		fmtKB(float64(otherSize)), pct(otherSize, uint64(fileSize)), numBlocks)

	fmt.Printf("  ─────────────────────────────────────────\n")
	fmt.Printf("  Properties detail:\n")
	fmt.Printf("    NumEntries:     %d\n", props.NumEntries)
	fmt.Printf("    NumDataBlocks:  %d\n", props.NumDataBlocks)
	fmt.Printf("    RawKeySize:     %s  (%.1fB/entry)\n",
		fmtKB(float64(props.RawKeySize)), float64(props.RawKeySize)/float64(props.NumEntries))
	fmt.Printf("    RawValueSize:   %s  (%.1fB/entry)\n",
		fmtKB(float64(props.RawValueSize)), float64(props.RawValueSize)/float64(props.NumEntries))
	fmt.Printf("    DataSize:       %s  (from props)\n", fmtKB(float64(props.DataSize)))
	fmt.Printf("    IndexSize:      %s  (from props)\n", fmtKB(float64(props.IndexSize)))

	// Key overhead calculation
	rawKeyTotal := props.RawKeySize
	rawValTotal := props.RawValueSize
	// RawKeySize includes the 8-byte internal key trailer per entry
	userKeyBytes := rawKeyTotal - (props.NumEntries * 8)
	fmt.Printf("  ─────────────────────────────────────────\n")
	fmt.Printf("  Key overhead analysis:\n")
	fmt.Printf("    User key bytes:          %10s  (%.1fB/entry)\n",
		fmtKB(float64(userKeyBytes)), float64(userKeyBytes)/float64(props.NumEntries))
	fmt.Printf("    Internal trailer bytes:  %10s  (8.0B/entry)\n",
		fmtKB(float64(props.NumEntries*8)))
	fmt.Printf("    Total key bytes:         %10s  (%.1fB/entry)\n",
		fmtKB(float64(rawKeyTotal)), float64(rawKeyTotal)/float64(props.NumEntries))
	fmt.Printf("    Total value bytes:       %10s  (%.1fB/entry)\n",
		fmtKB(float64(rawValTotal)), float64(rawValTotal)/float64(props.NumEntries))
	fmt.Printf("    Key overhead %%:          %.1f%% of raw (key+value)\n",
		float64(rawKeyTotal)/float64(rawKeyTotal+rawValTotal)*100)

	// Compression efficiency: data blocks contain both keys and values compressed together
	// Compare with the raw value-only size to gauge overhead
	dataRatio := float64(rawValTotal) / float64(dataCompressed)
	totalRatio := float64(rawValTotal) / float64(fileSize)
	fmt.Printf("  Compression (value-only basis):\n")
	fmt.Printf("    Raw values / data blocks = %.2fx\n", dataRatio)
	fmt.Printf("    Raw values / file size   = %.2fx\n", totalRatio)
}

func pct(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func writeSST(t *testing.T, tmpDir string, cfg sstConfig) sstResult {
	t.Helper()
	sstSeq++
	sstPath := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.sst", sstSeq))

	f, err := vfs.Default.Create(sstPath)
	if err != nil {
		t.Fatalf("create %s: %v", sstPath, err)
	}
	writable := objstorageprovider.NewFileWritable(f)

	opts := sstable.WriterOptions{
		BlockSize:   cfg.blockSize,
		Compression: sstable.ZstdCompression,
	}
	if cfg.blockSizeThresh > 0 {
		opts.BlockSizeThreshold = cfg.blockSizeThresh
	}
	if cfg.restartInterval > 0 {
		opts.BlockRestartInterval = cfg.restartInterval
	}

	w := sstable.NewWriter(writable, opts)

	keySize := cfg.keySize
	if keySize == 0 {
		keySize = 4
	}
	key := make([]byte, keySize)
	for i, ev := range allEvents {
		// Write ordinal as big-endian into the last bytes of key
		binary.BigEndian.PutUint32(key[keySize-4:], uint32(i))
		if err := w.Set(key, ev); err != nil {
			t.Fatalf("write event %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer (%s): %v", cfg.label, err)
	}

	info, err := os.Stat(sstPath)
	if err != nil {
		t.Fatalf("stat %s: %v", sstPath, err)
	}
	fileSize := info.Size()
	ratio := float64(totalRawBytes) / float64(fileSize)

	return sstResult{label: cfg.label, fileSize: fileSize, ratio: ratio}
}

func TestSSTableCompression(t *testing.T) {
	ensureSSTData(t)

	tmpDir := t.TempDir()
	numEvents := len(allEvents)
	avgEvent := totalRawBytes / numEvents

	fmt.Printf("\n=== Manual Batch vs SSTable Compression ===\n")
	fmt.Printf("Events: %d, Total raw: %s (avg %dB/event)\n\n", numEvents, fmtKB(float64(totalRawBytes)), avgEvent)

	// --- Manual batch (FOR-N index) vs SSTable (best config: th=100, ri=1024) ---
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatalf("zstd encoder: %v", err)
	}
	defer encoder.Close()

	const forGrpSize = 128 // FOR group size for inter-batch offset encoding

	// Pairs of (batch N, comparable SST block size)
	// Batch of N events ≈ N * avgEventSize uncompressed
	type sizePair struct {
		batchN   int
		sstBlock int // SST block size in bytes
	}
	pairs := []sizePair{
		{32, 8192},    // ~7KB vs 8KB
		{64, 16384},   // ~14KB vs 16KB
		{128, 32768},  // ~28KB vs 32KB
		{256, 65536},  // ~56KB vs 64KB
		{512, 131072}, // ~112KB vs 128KB
		// Equal uncompressed size: SST block = N * avgEvent
		{256, 256 * avgEvent},
	}

	// Batch payload layout (before compression):
	//   [FOR-N intra-batch index] [event₀] [event₁] ... [eventₙ₋₁]
	// The entire payload is zstd-compressed into one record.
	// FOR-N index = [1B width][4B min][packed residuals], no CRC/trailer.

	fmt.Println("\n--- Manual batch (FOR-N index) vs SSTable ---")
	fmt.Printf("  %-28s  %10s  %10s  %10s  %7s\n",
		"Method", "Compressed", "InterIdx", "Total", "Ratio")
	fmt.Println("  ----------------------------  ----------  ----------  ----------  -------")

	for _, p := range pairs {
		// --- Manual batch: prepend FOR-N index, then compress ---
		var batch []byte
		var payload []byte
		var compBuf []byte
		var offsets []int64
		totalComp := 0
		totalIntraRaw := 0 // uncompressed intra-batch index bytes (informational)
		var batchRecordSizes []int
		count := 0

		for _, ev := range allEvents {
			offsets = append(offsets, int64(len(batch)))
			batch = append(batch, ev...)
			count++
			if count == p.batchN {
				offsets = append(offsets, int64(len(batch)))
				deltas := deltaEncode(offsets)
				// Encode intra-batch index, strip CRC(4B) + trailer(10B)
				intraIdx := FOREncode(deltas, p.batchN)
				intraIdx = intraIdx[:len(intraIdx)-14]
				totalIntraRaw += len(intraIdx)

				// Prepend index to batch data, compress as one payload
				payload = append(payload[:0], intraIdx...)
				payload = append(payload, batch...)
				compBuf = encoder.EncodeAll(payload, compBuf[:0])
				totalComp += len(compBuf)
				batchRecordSizes = append(batchRecordSizes, len(compBuf))

				batch = batch[:0]
				offsets = offsets[:0]
				count = 0
			}
		}
		if count > 0 {
			offsets = append(offsets, int64(len(batch)))
			deltas := deltaEncode(offsets)
			intraIdx := FOREncode(deltas, count)
			intraIdx = intraIdx[:len(intraIdx)-14]
			totalIntraRaw += len(intraIdx)

			payload = append(payload[:0], intraIdx...)
			payload = append(payload, batch...)
			compBuf = encoder.EncodeAll(payload, compBuf[:0])
			totalComp += len(compBuf)
			batchRecordSizes = append(batchRecordSizes, len(compBuf))
		}

		// Inter-batch index
		interOffsets := make([]int64, len(batchRecordSizes)+1)
		var running int64
		for i, sz := range batchRecordSizes {
			interOffsets[i] = running
			running += int64(sz)
		}
		interOffsets[len(batchRecordSizes)] = running
		interDeltas := deltaEncode(interOffsets)
		interIndex := PerGroupWEncode(interDeltas, forGrpSize)

		batchTotal := totalComp + len(interIndex)
		batchRatio := float64(totalRawBytes) / float64(batchTotal)
		batchRawSize := p.batchN * avgEvent

		fmt.Printf("  Batch N=%-4d  (~%s raw)     %10s  %10s  %10s  %6.2fx  (intra idx raw: %s, %.2fB/ev)\n",
			p.batchN,
			fmtKB(float64(batchRawSize)),
			fmtKB(float64(totalComp)),
			fmtKB(float64(len(interIndex))),
			fmtKB(float64(batchTotal)),
			batchRatio,
			fmtKB(float64(totalIntraRaw)),
			float64(totalIntraRaw)/float64(numEvents))

		// --- SSTable at comparable block size ---
		sstRes := writeSST(t, tmpDir, sstConfig{
			label:           fmt.Sprintf("SST bs=%s", fmtKB(float64(p.sstBlock))),
			blockSize:       p.sstBlock,
			blockSizeThresh: 100,
			restartInterval: 1024,
		})
		fmt.Printf("  SST    bs=%-4s              %10s  %10s  %10s  %6.2fx\n",
			fmtKB(float64(p.sstBlock)),
			fmtKB(float64(sstRes.fileSize)),
			"—",
			fmtKB(float64(sstRes.fileSize)),
			sstRes.ratio)
		fmt.Println()
	}

	// --- Section 6: SST file layout breakdown ---
	fmt.Println("\n--- SST File Layout Breakdown ---")
	for _, bs := range []int{32768, 65536, 131072} {
		inspectSST(t, tmpDir, bs)
	}
}
