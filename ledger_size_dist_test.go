package main

import (
	"fmt"
	"sort"
	"testing"
)

func TestLedgerSizeDistribution(t *testing.T) {
	reader, err := NewChunkReader("006016.index", "006016.data")
	if err != nil {
		t.Fatalf("open chunk: %v", err)
	}
	defer reader.Close()

	n := reader.NumLedgers()
	compressedSizes := make([]int, 0, n)
	decompressedSizes := make([]int, 0, n)

	var totalCompressed, totalDecompressed uint64
	for i := 0; i < n; i++ {
		compressed, err := reader.ReadCompressed(i)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		csize := len(compressed)
		compressedSizes = append(compressedSizes, csize)
		totalCompressed += uint64(csize)

		decompressed, err := reader.Decompress(compressed)
		if err != nil {
			t.Fatalf("decompress %d: %v", i, err)
		}
		dsize := len(decompressed)
		decompressedSizes = append(decompressedSizes, dsize)
		totalDecompressed += uint64(dsize)
	}

	sort.Ints(compressedSizes)
	sort.Ints(decompressedSizes)

	fmt.Printf("\n=== Ledger Size Distribution (chunk 006016, %d ledgers) ===\n\n", n)
	fmt.Printf("Totals:\n")
	fmt.Printf("  Raw:        %.2f GB (%d B)\n", float64(totalDecompressed)/1e9, totalDecompressed)
	fmt.Printf("  Compressed: %.2f GB (%d B)\n", float64(totalCompressed)/1e9, totalCompressed)
	fmt.Printf("  Ratio:      %.2fx\n", float64(totalDecompressed)/float64(totalCompressed))
	fmt.Printf("  Avg raw:    %.1f KB\n", float64(totalDecompressed)/float64(n)/1024)
	fmt.Printf("  Avg comp:   %.1f KB\n", float64(totalCompressed)/float64(n)/1024)
	fmt.Println()

	percentiles := []struct {
		label string
		frac  float64
	}{
		{"min", 0.0},
		{"p01", 0.01},
		{"p10", 0.10},
		{"p25", 0.25},
		{"p50", 0.50},
		{"p75", 0.75},
		{"p90", 0.90},
		{"p95", 0.95},
		{"p99", 0.99},
		{"p999", 0.999},
		{"max", 1.0},
	}

	pick := func(s []int, frac float64) int {
		if frac >= 1.0 {
			return s[len(s)-1]
		}
		idx := int(float64(len(s)) * frac)
		if idx >= len(s) {
			idx = len(s) - 1
		}
		return s[idx]
	}

	fmt.Printf("%-6s  %12s  %12s  %6s\n", "pct", "compressed", "raw", "ratio")
	fmt.Printf("%-6s  %12s  %12s  %6s\n", "---", "----------", "---", "-----")
	for _, p := range percentiles {
		c := pick(compressedSizes, p.frac)
		d := pick(decompressedSizes, p.frac)
		var ratio float64
		if c > 0 {
			ratio = float64(d) / float64(c)
		}
		fmt.Printf("%-6s  %9.1f KB  %9.1f KB  %5.2fx\n",
			p.label, float64(c)/1024, float64(d)/1024, ratio)
	}

	// Bucketed histogram of compressed sizes
	buckets := []int{50, 100, 150, 200, 250, 300, 400, 500, 750, 1000, 2000, 5000}
	counts := make([]int, len(buckets)+1)
	for _, s := range compressedSizes {
		kb := s / 1024
		placed := false
		for i, b := range buckets {
			if kb < b {
				counts[i]++
				placed = true
				break
			}
		}
		if !placed {
			counts[len(buckets)]++
		}
	}
	fmt.Println("\nCompressed size histogram:")
	prev := 0
	for i, b := range buckets {
		fmt.Printf("  [%4d, %4d) KB  %6d  %5.1f%%\n",
			prev, b, counts[i], 100*float64(counts[i])/float64(n))
		prev = b
	}
	fmt.Printf("  [%4d, +inf) KB  %6d  %5.1f%%\n",
		prev, counts[len(buckets)], 100*float64(counts[len(buckets)])/float64(n))
}
