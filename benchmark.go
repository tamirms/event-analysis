package main

import (
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

var batchSizes = []int{32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}

// Stats holds per-batch-size compression statistics.
type Stats struct {
	compressedSizes []int
	rawSizes        []int
	decompTimes     []time.Duration
}

// BenchmarkResults holds all benchmark outputs.
type BenchmarkResults struct {
	eventSizes  []int          // individual encoded event sizes
	stats       map[int]*Stats // batch size → stats
	totalEvents int
}

// RunBenchmark runs the single-pass compression benchmark over all ledgers.
func RunBenchmark(reader *ChunkReader) *BenchmarkResults {
	numLedgers := reader.NumLedgers()
	results := &BenchmarkResults{
		eventSizes: make([]int, 0, numLedgers*500),
		stats:      make(map[int]*Stats),
	}
	for _, b := range batchSizes {
		results.stats[b] = &Stats{}
	}

	// zstd encoder/decoder for benchmark
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create zstd encoder: %v\n", err)
		return results
	}
	defer encoder.Close()

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create zstd decoder: %v\n", err)
		return results
	}
	defer decoder.Close()

	// Pre-allocated buffers reused across the entire benchmark
	flat := make([]byte, 0, 512*1024)
	offsets := make([]int, 0, 1024)
	count := 0
	decompSampleCounter := 0

	var compBuf []byte   // reusable compression output
	var decompBuf []byte // reusable decompression output (for latency sampling)
	var eventBuf []IngestEvent

	for i := 0; i < numLedgers; i++ {
		if i > 0 && i%1000 == 0 {
			fmt.Fprintf(os.Stderr, "progress: %d/%d ledgers, %d events so far\n", i, numLedgers, results.totalEvents)
		}

		ledgerBytes, err := reader.ReadLedger(i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to read ledger %d: %v\n", i, err)
			continue
		}

		eventBuf, err = ExtractEvents(ledgerBytes, eventBuf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to extract events from ledger %d: %v\n", i, err)
			continue
		}

		for idx := range eventBuf {
			ev := &eventBuf[idx]

			// Encode directly into flat buffer (no per-event allocation)
			evStart := len(flat)
			flat = AppendBinaryEvent(flat, ev)
			eventSize := len(flat) - evStart

			results.eventSizes = append(results.eventSizes, eventSize)
			results.totalEvents++

			offsets = append(offsets, evStart)
			count++

			// Check each batch size boundary
			for _, b := range batchSizes {
				if count%b == 0 {
					// Compress the last B events
					blockStart := offsets[count-b]
					blockData := flat[blockStart:]
					rawSize := len(blockData)

					compBuf = encoder.EncodeAll(blockData, compBuf[:0])
					compressedSize := len(compBuf)

					st := results.stats[b]
					st.compressedSizes = append(st.compressedSizes, compressedSize)
					st.rawSizes = append(st.rawSizes, rawSize)

					// Sample decompression latency every 100th compression
					decompSampleCounter++
					if decompSampleCounter%100 == 0 {
						tStart := time.Now()
						var decErr error
						decompBuf, decErr = decoder.DecodeAll(compBuf, decompBuf[:0])
						elapsed := time.Since(tStart)
						if decErr == nil {
							st.decompTimes = append(st.decompTimes, elapsed)
						}
					}
				}
			}

			// Reset buffer at 8192 boundary (LCM of all batch sizes)
			if count%8192 == 0 {
				flat = flat[:0]
				offsets = offsets[:0]
				count = 0
			}
		}
	}

	fmt.Fprintf(os.Stderr, "done: %d ledgers, %d events total\n", numLedgers, results.totalEvents)
	return results
}
