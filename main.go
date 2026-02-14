package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
)

func main() {
	indexPath := flag.String("index", "006016.index", "path to index file")
	dataPath := flag.String("data", "006016.data", "path to data file")
	flag.Parse()

	reader, err := NewChunkReader(*indexPath, *dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	fmt.Printf("chunk: %d ledgers\n\n", reader.NumLedgers())

	results := RunBenchmark(reader)

	PrintEventSizeDistribution(results)
	PrintBatchComparison(results)
	PrintBlockSizeDistribution(results)
	PrintCompressionRatioDistribution(results)
	PrintDecompLatency(results)
}

// PrintEventSizeDistribution prints Section 1.
func PrintEventSizeDistribution(r *BenchmarkResults) {
	if len(r.eventSizes) == 0 {
		fmt.Println("=== Event Size Distribution ===")
		fmt.Println("  no events")
		fmt.Println()
		return
	}

	sorted := make([]int, len(r.eventSizes))
	copy(sorted, r.eventSizes)
	sort.Ints(sorted)

	n := len(sorted)
	sum := 0
	for _, s := range sorted {
		sum += s
	}

	fmt.Println("=== Event Size Distribution ===")
	fmt.Printf("  count:  %d\n", n)
	fmt.Printf("  mean:   %dB\n", sum/n)
	fmt.Printf("  p50:    %dB\n", percentileInt(sorted, 50))
	fmt.Printf("  p95:    %dB\n", percentileInt(sorted, 95))
	fmt.Printf("  p99:    %dB\n", percentileInt(sorted, 99))
	fmt.Printf("  max:    %dB\n", sorted[n-1])
	fmt.Println()
}

// PrintBatchComparison prints Section 2.
func PrintBatchComparison(r *BenchmarkResults) {
	if len(r.eventSizes) == 0 {
		return
	}

	sum := 0
	for _, s := range r.eventSizes {
		sum += s
	}
	avgEventSize := float64(sum) / float64(len(r.eventSizes))

	fmt.Println("=== Batch Size Comparison ===")
	fmt.Printf("%-8s%-8s%-10s%-10s%-11s%-9s%s\n", "B", "ratio", "blockKB", "indexKB", "scatMB", "scatMs", "iopsBound")

	for _, b := range batchSizes {
		st := r.stats[b]
		if len(st.compressedSizes) == 0 {
			continue
		}

		avgCompressed := avgFloat(st.compressedSizes)

		ratio := float64(b) * avgEventSize / avgCompressed
		blockKB := avgCompressed / 1024
		indexKB := float64(10_000_000/b) * 4 / 1024
		scatMB := 1000 * avgCompressed / (1024 * 1024)
		scatMs := scatMB / 125 * 1000
		iopsBound := scatMs < 125

		fmt.Printf("%-8d%-8.1f%-10.1f%-10.0f%-11.1f%-9.0f%v\n",
			b, ratio, blockKB, indexKB, scatMB, scatMs, iopsBound)
	}
	fmt.Println()
}

// PrintBlockSizeDistribution prints Section 3.
func PrintBlockSizeDistribution(r *BenchmarkResults) {
	fmt.Println("=== Block Size Distribution ===")
	fmt.Printf("%-8s%-9s%-9s%-9s%-9s%s\n", "B", "mean", "p50", "p95", "p99", "max")

	for _, b := range batchSizes {
		st := r.stats[b]
		if len(st.compressedSizes) == 0 {
			continue
		}

		sorted := make([]int, len(st.compressedSizes))
		copy(sorted, st.compressedSizes)
		sort.Ints(sorted)

		n := len(sorted)
		sum := 0
		for _, s := range sorted {
			sum += s
		}

		fmt.Printf("%-8d%-9s%-9s%-9s%-9s%s\n",
			b,
			fmtKB(float64(sum)/float64(n)),
			fmtKB(float64(percentileInt(sorted, 50))),
			fmtKB(float64(percentileInt(sorted, 95))),
			fmtKB(float64(percentileInt(sorted, 99))),
			fmtKB(float64(sorted[n-1])),
		)
	}
	fmt.Println()
}

// PrintCompressionRatioDistribution prints Section 4.
func PrintCompressionRatioDistribution(r *BenchmarkResults) {
	fmt.Println("=== Compression Ratio Distribution ===")
	fmt.Printf("%-8s%-8s%-8s%-8s%-8s%s\n", "B", "mean", "p50", "p5", "p1", "min")

	for _, b := range batchSizes {
		st := r.stats[b]
		if len(st.compressedSizes) == 0 || len(st.rawSizes) == 0 {
			continue
		}

		// Compute per-block ratios
		ratios := make([]float64, len(st.compressedSizes))
		for i := range st.compressedSizes {
			if st.compressedSizes[i] > 0 {
				ratios[i] = float64(st.rawSizes[i]) / float64(st.compressedSizes[i])
			}
		}
		sort.Float64s(ratios)

		n := len(ratios)
		sum := 0.0
		for _, r := range ratios {
			sum += r
		}

		fmt.Printf("%-8d%-8.1f%-8.1f%-8.1f%-8.1f%.1f\n",
			b,
			sum/float64(n),
			percentileFloat(ratios, 50),
			percentileFloat(ratios, 5),
			percentileFloat(ratios, 1),
			ratios[0],
		)
	}
	fmt.Println()
}

// PrintDecompLatency prints Section 5.
func PrintDecompLatency(r *BenchmarkResults) {
	fmt.Println("=== Decompression Latency ===")

	for _, b := range batchSizes {
		st := r.stats[b]
		if len(st.decompTimes) == 0 {
			fmt.Printf("  B=%-6d (no samples)\n", b)
			continue
		}

		durations := make([]int64, len(st.decompTimes))
		for i, d := range st.decompTimes {
			durations[i] = d.Microseconds()
		}
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

		n := len(durations)
		sum := int64(0)
		for _, d := range durations {
			sum += d
		}

		fmt.Printf("  B=%-6d mean=%dµs  p50=%dµs  p99=%dµs\n",
			b,
			sum/int64(n),
			percentileInt64(durations, 50),
			percentileInt64(durations, 99),
		)
	}
	fmt.Println()
}

// --- helpers ---

func percentileInt(sorted []int, pct int) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(pct)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func percentileFloat(sorted []float64, pct int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(pct)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func percentileInt64(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(pct)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func avgFloat(vals []int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals))
}

func fmtKB(bytes float64) string {
	return fmt.Sprintf("%.1fKB", bytes/1024)
}
