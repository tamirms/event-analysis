package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/fse"
)

// --- Data generation (once) ---

var (
	testOnce       sync.Once
	recordSizes    []uint32
	offsets1x      []int64
	deltas1x       []uint32
	offsets10x     []int64
	deltas10x      []uint32
	dataGenErr     error
	totalBlocks1x  int
	totalBlocks10x int
)

func ensureTestData(t *testing.T) {
	t.Helper()
	testOnce.Do(func() { dataGenErr = generateTestData() })
	if dataGenErr != nil {
		t.Fatalf("data generation failed: %v", dataGenErr)
	}
}

func generateTestData() error {
	// Use loadAllEvents (from testdata_test.go) to avoid re-parsing source.
	dataOnce.Do(func() { dataLoadErr = loadAllEvents() })
	if dataLoadErr != nil {
		return fmt.Errorf("load events: %w", dataLoadErr)
	}

	fmt.Printf("generating index test data from %d events...\n", len(allEvents))

	const batchSize = 128
	var flat []byte
	var compBuf []byte
	eventCount := 0

	for _, ev := range allEvents {
		flat = append(flat, ev...)
		eventCount++
		if eventCount == batchSize {
			compBuf = zstdEncoder.EncodeAll(flat, compBuf[:0])
			recordSizes = append(recordSizes, uint32(len(compBuf)))
			flat = flat[:0]
			eventCount = 0
		}
	}
	if eventCount > 0 {
		compBuf = zstdEncoder.EncodeAll(flat, compBuf[:0])
		recordSizes = append(recordSizes, uint32(len(compBuf)))
	}

	fmt.Printf("  total events: %d, total blocks: %d\n", len(allEvents), len(recordSizes))

	// 1x
	offsets1x = make([]int64, len(recordSizes)+1)
	for i, s := range recordSizes {
		offsets1x[i+1] = offsets1x[i] + int64(s)
	}
	deltas1x = deltaEncode(offsets1x)
	totalBlocks1x = len(recordSizes)

	// 10x
	deltas10x = make([]uint32, len(deltas1x)*10)
	for i := 0; i < 10; i++ {
		copy(deltas10x[i*len(deltas1x):], deltas1x)
	}
	offsets10x = make([]int64, len(deltas10x)+1)
	for i, d := range deltas10x {
		offsets10x[i+1] = offsets10x[i] + int64(d)
	}
	totalBlocks10x = len(deltas10x)

	fmt.Printf("  1x records: %d, 10x records: %d\n", totalBlocks1x, totalBlocks10x)
	return nil
}

// --- helpers ---

func makeBaseOffsets(offs []int64, grpSize int) []int64 {
	nGroups := (len(offs) - 1 + grpSize - 1) / grpSize
	base := make([]int64, nGroups)
	for g := 0; g < nGroups; g++ {
		idx := g * grpSize
		if idx < len(offs) {
			base[g] = offs[idx]
		}
	}
	return base
}

func fmtBytes(b int) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	return fmtKB(float64(b))
}

func sortedInt64(vals []int64) []int64 {
	s := make([]int64, len(vals))
	copy(s, vals)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s
}

func p50i64(s []int64) int64 { return s[len(s)/2] }
func p99i64(s []int64) int64 { return s[int(float64(len(s))*0.99)] }

// truncatedBinaryGroupBits returns the total bits needed to encode `count` residuals
// in [0, R] using truncated binary coding. For n=R+1 possible values:
// k = floor(log2(n)), threshold = 2^(k+1) - n.
// Values < threshold use k bits, values >= threshold use k+1 bits.
func truncatedBinaryGroupBits(residuals []uint32, R uint32) int {
	if R == 0 {
		return 0
	}
	n := R + 1
	k := bits.Len32(n) - 1 // floor(log2(n))
	threshold := (1 << (k + 1)) - n
	totalBits := 0
	for _, v := range residuals {
		if v < threshold {
			totalBits += k
		} else {
			totalBits += k + 1
		}
	}
	return totalBits
}

// truncatedBinaryTotalSize computes total bytes for truncated binary encoding of FOR residuals.
func truncatedBinaryTotalSize(deltas []uint32, grpSize int) (headerBytes, dataBytes, totalBytes int) {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	headerBytes = nGroups * 5 // 1 byte W + 4 bytes min per group (same as FOR)

	totalBits := 0
	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]

		var minDelta, maxDelta uint32
		minDelta = groupDeltas[0]
		maxDelta = groupDeltas[0]
		for _, d := range groupDeltas[1:] {
			if d < minDelta {
				minDelta = d
			}
			if d > maxDelta {
				maxDelta = d
			}
		}
		R := maxDelta - minDelta

		residuals := make([]uint32, len(groupDeltas))
		for i, d := range groupDeltas {
			residuals[i] = d - minDelta
		}
		totalBits += truncatedBinaryGroupBits(residuals, R)
	}

	dataBytes = (totalBits + 7) / 8
	totalBytes = headerBytes + dataBytes + 4 + 10 // +CRC +trailer
	return
}

// --- Benchmark 0: Intra-Group Range Analysis ---

func TestIntraGroupRange(t *testing.T) {
	ensureTestData(t)

	deltas := deltas1x
	fmt.Printf("\n=== Intra-Group Range Analysis (%dK records) ===\n", len(deltas)/1000)
	fmt.Printf("Shows groupMax-groupMin per group → the residual range FOR must encode.\n\n")

	for _, gs := range []int{32, 64, 128, 256, 512} {
		nGroups := (len(deltas) + gs - 1) / gs
		ranges := make([]uint32, nGroups)
		wPrimes := make([]int, nGroups)

		for g := 0; g < nGroups; g++ {
			start := g * gs
			end := start + gs
			if end > len(deltas) {
				end = len(deltas)
			}
			groupDeltas := deltas[start:end]
			mn, mx := groupDeltas[0], groupDeltas[0]
			for _, d := range groupDeltas[1:] {
				if d < mn {
					mn = d
				}
				if d > mx {
					mx = d
				}
			}
			ranges[g] = mx - mn
			wPrimes[g] = bits.Len32(mx - mn)
		}

		// Sort ranges for percentiles
		sortedRanges := make([]uint32, nGroups)
		copy(sortedRanges, ranges)
		sort.Slice(sortedRanges, func(i, j int) bool { return sortedRanges[i] < sortedRanges[j] })

		p50 := sortedRanges[nGroups/2]
		p90 := sortedRanges[int(float64(nGroups)*0.90)]
		p99 := sortedRanges[int(float64(nGroups)*0.99)]
		maxRange := sortedRanges[nGroups-1]

		fmt.Printf("Group size %d (%d groups):\n", gs, nGroups)
		fmt.Printf("  Range (max-min): p50=%d (W'=%d), p90=%d (W'=%d), p99=%d (W'=%d), max=%d (W'=%d)\n",
			p50, bits.Len32(p50), p90, bits.Len32(p90), p99, bits.Len32(p99), maxRange, bits.Len32(maxRange))

		// W' histogram
		wHist := make(map[int]int)
		for _, w := range wPrimes {
			wHist[w]++
		}
		var ws []int
		for w := range wHist {
			ws = append(ws, w)
		}
		sort.Ints(ws)
		fmt.Printf("  W' distribution: ")
		for i, w := range ws {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("W'=%d: %d (%.1f%%)", w, wHist[w], float64(wHist[w])/float64(nGroups)*100)
		}
		fmt.Println()
	}
}

// --- Benchmark 1: Compression by Group Size ---

func TestCompressionByGroupSize(t *testing.T) {
	ensureTestData(t)

	groupSizes := []int{16, 32, 64, 128, 256}

	type dataset struct {
		name   string
		deltas []uint32
		offs   []int64
		n      int
	}
	datasets := []dataset{
		{"1x", deltas1x, offsets1x, totalBlocks1x},
		{"10x", deltas10x, offsets10x, totalBlocks10x},
	}

	for _, ds := range datasets {
		fmt.Printf("\n=== Compression by Group Size (%s, %dK records) ===\n", ds.name, ds.n/1000)
		fmt.Printf("%-12s| %-8s| %-13s| %-11s| %-9s| %s\n",
			"Method", "Groups", "Header Bytes", "Data Bytes", "Total", "vs Global-64")
		fmt.Println("------------|--------|-------------|-----------|---------|------------")

		// Global-W baseline at group size 64
		gHdr, gData, gTotal := GlobalWEncodeSize(ds.deltas, 64)
		fmt.Printf("%-12s| %-8d| %-13s| %-11s| %-9s| %s\n",
			"global-64",
			(ds.n+63)/64,
			fmtBytes(gHdr), fmtKB(float64(gData)), fmtKB(float64(gTotal)),
			"baseline")

		for _, gs := range groupSizes {
			hdr, data, total := PerGroupWEncodeSize(ds.deltas, gs)
			pct := float64(total-gTotal) / float64(gTotal) * 100
			fmt.Printf("%-12s| %-8d| %-13s| %-11s| %-9s| %+.1f%%\n",
				fmt.Sprintf("perW-%d", gs),
				(ds.n+gs-1)/gs,
				fmtBytes(hdr), fmtKB(float64(data)), fmtKB(float64(total)),
				pct)
		}

		// FOR rows — compare against perW-64 as baseline
		_, _, perW64Total := PerGroupWEncodeSize(ds.deltas, 64)
		fmt.Println()
		fmt.Printf("%-12s| %-8s| %-13s| %-11s| %-9s| %s\n",
			"Method", "Groups", "Header Bytes", "Data Bytes", "Total", "vs perW-64")
		fmt.Println("------------|--------|-------------|-----------|---------|------------")
		fmt.Printf("%-12s| %-8d| %-13s| %-11s| %-9s| %s\n",
			"perW-64",
			(ds.n+63)/64,
			fmtBytes((ds.n+63)/64), fmtKB(float64(perW64Total-((ds.n+63)/64)-14)), fmtKB(float64(perW64Total)),
			"baseline")

		for _, gs := range []int{32, 64, 128, 256, 512} {
			hdr, data, total := FOREncodeSize(ds.deltas, gs)
			pct := float64(total-perW64Total) / float64(perW64Total) * 100
			fmt.Printf("%-12s| %-8d| %-13s| %-11s| %-9s| %+.1f%%\n",
				fmt.Sprintf("FOR-%d", gs),
				(ds.n+gs-1)/gs,
				fmtBytes(hdr), fmtKB(float64(data)), fmtKB(float64(total)),
				pct)
		}
	}
}

// --- Benchmark 2: W Distribution Analysis ---

func TestWDistribution(t *testing.T) {
	ensureTestData(t)

	deltas := deltas1x
	fmt.Printf("\n=== W Distribution Analysis (%dK records) ===\n", len(deltas)/1000)

	// Delta distribution
	sorted := make([]uint32, len(deltas))
	copy(sorted, deltas)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	n := len(sorted)
	pcts := []int{50, 90, 95, 99}
	fmt.Println("\nDelta distribution:")
	for _, p := range pcts {
		idx := int(float64(p)/100*float64(n)) - 1
		if idx < 0 {
			idx = 0
		}
		v := sorted[idx]
		fmt.Printf("  p%d: %d  (W=%d)\n", p, v, bits.Len32(v))
	}
	maxVal := sorted[n-1]
	globalW := bits.Len32(maxVal)
	fmt.Printf("  max: %d  (W=%d) ← global W\n", maxVal, globalW)

	// W histogram per group size
	for _, gs := range []int{32, 64, 128} {
		nGroups := (len(deltas) + gs - 1) / gs
		wHist := make(map[int]int)
		wSum := 0
		belowGlobal := 0

		for g := 0; g < nGroups; g++ {
			start := g * gs
			end := start + gs
			if end > len(deltas) {
				end = len(deltas)
			}
			var mx uint32
			for _, d := range deltas[start:end] {
				if d > mx {
					mx = d
				}
			}
			w := bits.Len32(mx)
			wHist[w]++
			wSum += w
			if w < globalW {
				belowGlobal++
			}
		}

		fmt.Printf("\nW histogram (group size %d, %d groups):\n", gs, nGroups)
		fmt.Printf("  mean W: %.1f (global W: %d)\n", float64(wSum)/float64(nGroups), globalW)
		fmt.Printf("  groups below global W: %d (%.1f%%)\n", belowGlobal, float64(belowGlobal)/float64(nGroups)*100)

		// Print histogram sorted by W
		var ws []int
		for w := range wHist {
			ws = append(ws, w)
		}
		sort.Ints(ws)
		for _, w := range ws {
			cnt := wHist[w]
			fmt.Printf("  W=%-3d: %5d groups (%5.1f%%)\n", w, cnt, float64(cnt)/float64(nGroups)*100)
		}
	}
}

// --- Benchmark 3: Encode Speed ---

func TestEncodeSpeed(t *testing.T) {
	ensureTestData(t)

	const iterations = 10000

	// Determine best group size (we'll test 64 and whatever wins from bench 1)
	type encBench struct {
		name string
		fn   func()
	}

	baseOffs64 := makeBaseOffsets(offsets1x, 64)

	benches := []encBench{
		{"global-64", func() { _ = GlobalWEncode(deltas1x, baseOffs64, 64) }},
		{"perW-32", func() { _ = PerGroupWEncode(deltas1x, 32) }},
		{"perW-64", func() { _ = PerGroupWEncode(deltas1x, 64) }},
		{"perW-128", func() { _ = PerGroupWEncode(deltas1x, 128) }},
		{"FOR-32", func() { _ = FOREncode(deltas1x, 32) }},
		{"FOR-64", func() { _ = FOREncode(deltas1x, 64) }},
		{"FOR-128", func() { _ = FOREncode(deltas1x, 128) }},
		{"FOR-256", func() { _ = FOREncode(deltas1x, 256) }},
	}

	fmt.Printf("\n=== Encode Speed (%dK records, %d iterations) ===\n", totalBlocks1x/1000, iterations)
	fmt.Printf("%-13s| %-17s| %s\n", "Method", "Encode p50 (us)", "Encode p99 (us)")
	fmt.Println("-------------|-----------------|----------------")

	for _, b := range benches {
		times := make([]int64, iterations)
		for i := range times {
			start := time.Now()
			b.fn()
			times[i] = time.Since(start).Microseconds()
		}
		s := sortedInt64(times)
		fmt.Printf("%-13s| %-17d| %d\n", b.name, p50i64(s), p99i64(s))
	}
}

// --- Benchmark 4: Full Decode Speed ---

func TestFullDecodeSpeed(t *testing.T) {
	ensureTestData(t)

	const iterations = 10000

	type dataset struct {
		name   string
		deltas []uint32
		offs   []int64
		n      int
	}
	datasets := []dataset{
		{"1x", deltas1x, offsets1x, totalBlocks1x},
		{"10x", deltas10x, offsets10x, totalBlocks10x},
	}

	for _, ds := range datasets {
		fmt.Printf("\n=== Full Decode Speed (%s, %dK records, %d iterations) ===\n", ds.name, ds.n/1000, iterations)
		fmt.Printf("%-13s| %-8s| %-17s| %-17s| %s\n",
			"Method", "Records", "Decode p50 (us)", "Decode p99 (us)", "Allocs")
		fmt.Println("-------------|--------|-----------------|-----------------|-------")

		// Encode all approaches
		baseOffs64 := makeBaseOffsets(ds.offs, 64)
		globalData := GlobalWEncode(ds.deltas, baseOffs64, 64)
		perW64Data := PerGroupWEncode(ds.deltas, 64)
		perW32Data := PerGroupWEncode(ds.deltas, 32)
		for64Data := FOREncode(ds.deltas, 64)
		for32Data := FOREncode(ds.deltas, 32)
		for128Data := FOREncode(ds.deltas, 128)
		for256Data := FOREncode(ds.deltas, 256)

		type decBench struct {
			name string
			fn   func()
		}
		benches := []decBench{
			{"global-64", func() { _ = GlobalWDecode(globalData, ds.n, 64) }},
			{"perW-64", func() { _ = PerGroupWDecode(perW64Data, ds.n, 64) }},
			{"perW-32", func() { _ = PerGroupWDecode(perW32Data, ds.n, 32) }},
			{"FOR-64", func() { _ = FORDecode(for64Data, ds.n, 64) }},
			{"FOR-32", func() { _ = FORDecode(for32Data, ds.n, 32) }},
			{"FOR-128", func() { _ = FORDecode(for128Data, ds.n, 128) }},
			{"FOR-256", func() { _ = FORDecode(for256Data, ds.n, 256) }},
		}

		for _, b := range benches {
			times := make([]int64, iterations)
			for i := range times {
				start := time.Now()
				b.fn()
				times[i] = time.Since(start).Microseconds()
			}
			s := sortedInt64(times)

			// Allocation tracking
			runtime.GC()
			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			b.fn()
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)

			fmt.Printf("%-13s| %-8s| %-17d| %-17d| %d\n",
				b.name, fmt.Sprintf("%dK", ds.n/1000),
				p50i64(s), p99i64(s),
				m2.Mallocs-m1.Mallocs)
		}
	}
}

// --- Benchmark 5: Integrity Verification ---

func TestIntegrityVerification(t *testing.T) {
	ensureTestData(t)

	const iterations = 10000

	fmt.Printf("\n=== Integrity Verification (%dK records) ===\n", totalBlocks1x/1000)

	// Encode all
	baseOffs := makeBaseOffsets(offsets1x, 64)
	globalData := GlobalWEncode(deltas1x, baseOffs, 64)
	perWData := PerGroupWEncode(deltas1x, 64)
	forData := FOREncode(deltas1x, 64)

	// Timing: anchor cross-check
	anchorTimes := make([]int64, iterations)
	for i := range anchorTimes {
		start := time.Now()
		_, _ = GlobalWDecodeWithAnchors(globalData, totalBlocks1x, 64)
		anchorTimes[i] = time.Since(start).Microseconds()
	}
	anchorSorted := sortedInt64(anchorTimes)

	// Timing: perW CRC32C verification
	crcTimes := make([]int64, iterations)
	for i := range crcTimes {
		start := time.Now()
		_, _ = PerGroupWDecodeWithCRC(perWData, totalBlocks1x, 64)
		crcTimes[i] = time.Since(start).Microseconds()
	}
	crcSorted := sortedInt64(crcTimes)

	// Timing: FOR CRC32C verification
	forCrcTimes := make([]int64, iterations)
	for i := range forCrcTimes {
		start := time.Now()
		_, _ = FORDecodeWithCRC(forData, totalBlocks1x, 64)
		forCrcTimes[i] = time.Since(start).Microseconds()
	}
	forCrcSorted := sortedInt64(forCrcTimes)

	// Timing: decode-only (no verification) for comparison
	plainTimes := make([]int64, iterations)
	for i := range plainTimes {
		start := time.Now()
		_ = PerGroupWDecode(perWData, totalBlocks1x, 64)
		plainTimes[i] = time.Since(start).Microseconds()
	}
	plainSorted := sortedInt64(plainTimes)

	fmt.Printf("%-24s| %-17s| %s\n", "Method", "Decode p50 (us)", "Decode p99 (us)")
	fmt.Println("------------------------|-----------------|----------------")
	fmt.Printf("%-24s| %-17d| %d\n", "perW-64 (no verify)", p50i64(plainSorted), p99i64(plainSorted))
	fmt.Printf("%-24s| %-17d| %d\n", "global-64 + anchors", p50i64(anchorSorted), p99i64(anchorSorted))
	fmt.Printf("%-24s| %-17d| %d\n", "perW-64 + CRC32C", p50i64(crcSorted), p99i64(crcSorted))
	fmt.Printf("%-24s| %-17d| %d\n", "FOR-64 + CRC32C", p50i64(forCrcSorted), p99i64(forCrcSorted))

	// Error injection test
	fmt.Println("\nError injection test:")

	// Anchor approach: flip a bit in packed data of group 5
	corruptGlobal := make([]byte, len(globalData))
	copy(corruptGlobal, globalData)
	corruptPos := 5*9 + 50
	if corruptPos < len(corruptGlobal) {
		corruptGlobal[corruptPos] ^= 0x01
	}
	_, anchorValid := GlobalWDecodeWithAnchors(corruptGlobal, totalBlocks1x, 64)
	fmt.Printf("  anchor cross-check detects bit flip: %v\n", !anchorValid)

	// CRC approach: flip a bit in packed data
	corruptPerW := make([]byte, len(perWData))
	copy(corruptPerW, perWData)
	corruptPos = 50
	if corruptPos < len(corruptPerW) {
		corruptPerW[corruptPos] ^= 0x01
	}
	_, crcValid := PerGroupWDecodeWithCRC(corruptPerW, totalBlocks1x, 64)
	fmt.Printf("  CRC32C detects bit flip: %v\n", !crcValid)

	// FOR CRC approach: flip a bit in packed data
	corruptFOR := make([]byte, len(forData))
	copy(corruptFOR, forData)
	corruptPos = 50
	if corruptPos < len(corruptFOR) {
		corruptFOR[corruptPos] ^= 0x01
	}
	_, forCrcValid := FORDecodeWithCRC(corruptFOR, totalBlocks1x, 64)
	fmt.Printf("  FOR CRC32C detects bit flip: %v\n", !forCrcValid)

	// Verify clean data passes
	_, anchorClean := GlobalWDecodeWithAnchors(globalData, totalBlocks1x, 64)
	_, crcClean := PerGroupWDecodeWithCRC(perWData, totalBlocks1x, 64)
	_, forClean := FORDecodeWithCRC(forData, totalBlocks1x, 64)
	fmt.Printf("  anchor clean data valid: %v\n", anchorClean)
	fmt.Printf("  CRC32C clean data valid: %v\n", crcClean)
	fmt.Printf("  FOR CRC32C clean data valid: %v\n", forClean)
}

// --- Recommendation ---

func TestRecommendation(t *testing.T) {
	ensureTestData(t)

	fmt.Println("\n=== Recommendation ===")

	// Size comparison: perW-64 as baseline, FOR candidates
	_, _, perW64Total := PerGroupWEncodeSize(deltas1x, 64)

	// Sweep FOR over all candidate group sizes
	type forCandidate struct {
		gs    int
		hdr   int
		data  int
		total int
	}
	forGroupSizes := []int{32, 64, 128, 256, 512}
	candidates := make([]forCandidate, len(forGroupSizes))
	bestFORTotal := 0
	bestFORIdx := 0
	for i, gs := range forGroupSizes {
		hdr, data, total := FOREncodeSize(deltas1x, gs)
		candidates[i] = forCandidate{gs, hdr, data, total}
		if bestFORTotal == 0 || total < bestFORTotal {
			bestFORTotal = total
			bestFORIdx = i
		}
	}

	// Truncated binary theoretical sizes
	fmt.Printf("\n%-14s| %-10s| %-11s| %-10s| %s\n",
		"Method", "Header", "Data", "Total", "vs perW-64")
	fmt.Println("--------------|----------|-----------|----------|----------")
	fmt.Printf("%-14s| %-10s| %-11s| %-10s| %s\n",
		"perW-64",
		fmtBytes((len(deltas1x)+63)/64),
		fmtKB(float64(perW64Total-((len(deltas1x)+63)/64)-14)),
		fmtKB(float64(perW64Total)),
		"baseline")

	for _, c := range candidates {
		pct := float64(c.total-perW64Total) / float64(perW64Total) * 100
		fmt.Printf("%-14s| %-10s| %-11s| %-10s| %+.1f%%\n",
			fmt.Sprintf("FOR-%d", c.gs),
			fmtBytes(c.hdr), fmtKB(float64(c.data)), fmtKB(float64(c.total)),
			pct)
	}

	// Truncated binary rows
	for _, gs := range forGroupSizes {
		tbHdr, tbData, tbTotal := truncatedBinaryTotalSize(deltas1x, gs)
		pct := float64(tbTotal-perW64Total) / float64(perW64Total) * 100
		fmt.Printf("%-14s| %-10s| %-11s| %-10s| %+.1f%%\n",
			fmt.Sprintf("trunc-%d", gs),
			fmtBytes(tbHdr), fmtKB(float64(tbData)), fmtKB(float64(tbTotal)),
			pct)
	}

	bestFORGS := candidates[bestFORIdx].gs
	forSmaller := bestFORTotal < perW64Total
	fmt.Printf("\nBest FOR: FOR-%d=%s vs perW-64=%s — improvement: %v",
		bestFORGS, fmtKB(float64(bestFORTotal)), fmtKB(float64(perW64Total)), forSmaller)
	if forSmaller {
		pct := float64(perW64Total-bestFORTotal) / float64(perW64Total) * 100
		fmt.Printf(" (%.1f%% smaller)", pct)
	}
	fmt.Println()

	// Decode speed: FOR vs perW-64
	perWData := PerGroupWEncode(deltas1x, 64)
	forData := FOREncode(deltas1x, bestFORGS)

	const decIter = 1000

	perWTimes := make([]int64, decIter)
	for i := range perWTimes {
		start := time.Now()
		_ = PerGroupWDecode(perWData, totalBlocks1x, 64)
		perWTimes[i] = time.Since(start).Microseconds()
	}
	pSorted := sortedInt64(perWTimes)

	forTimes := make([]int64, decIter)
	for i := range forTimes {
		start := time.Now()
		_ = FORDecode(forData, totalBlocks1x, bestFORGS)
		forTimes[i] = time.Since(start).Microseconds()
	}
	fSorted := sortedInt64(forTimes)

	ratio := float64(p50i64(fSorted)) / float64(p50i64(pSorted))
	speedOK := ratio < 2.0
	fmt.Printf("Decode speed: perW-64 p50=%dus, FOR-%d p50=%dus, ratio=%.2fx — <2x: %v\n",
		p50i64(pSorted), bestFORGS, p50i64(fSorted), ratio, speedOK)

	// CRC detection
	forClean := FOREncode(deltas1x, bestFORGS)
	_, crcOK := FORDecodeWithCRC(forClean, totalBlocks1x, bestFORGS)
	fmt.Printf("CRC32C works: %v\n", crcOK)

	allPass := forSmaller && speedOK && crcOK
	fmt.Println()
	if allPass {
		savings := float64(perW64Total-bestFORTotal) / float64(perW64Total) * 100
		fmt.Printf("ACCEPT FOR (group size %d). %.1f%% smaller than perW-64, decode within %.2fx, CRC works.\n",
			bestFORGS, savings, ratio)
	} else {
		reasons := []string{}
		if !forSmaller {
			reasons = append(reasons, "not smaller than perW-64")
		}
		if !speedOK {
			reasons = append(reasons, fmt.Sprintf("decode %.2fx slower", ratio))
		}
		if !crcOK {
			reasons = append(reasons, "CRC failed")
		}
		fmt.Printf("REJECT FOR. Failed: %v\n", reasons)
	}
}

// --- Benchmark: Residual Entropy Analysis ---

func TestResidualEntropy(t *testing.T) {
	ensureTestData(t)

	deltas := deltas1x
	fmt.Printf("\n=== Residual Entropy Analysis (%dK records) ===\n", len(deltas)/1000)

	for _, gs := range []int{64, 128, 256} {
		nGroups := (len(deltas) + gs - 1) / gs

		var totalEntropyBits float64
		var totalTruncBits int
		var totalFORBits int
		var entropyPerGroup []float64
		var logRPerGroup []float64

		for g := 0; g < nGroups; g++ {
			start := g * gs
			end := start + gs
			if end > len(deltas) {
				end = len(deltas)
			}
			groupDeltas := deltas[start:end]
			count := len(groupDeltas)

			// Compute min/max for FOR residuals
			minD, maxD := groupDeltas[0], groupDeltas[0]
			for _, d := range groupDeltas[1:] {
				if d < minD {
					minD = d
				}
				if d > maxD {
					maxD = d
				}
			}
			R := maxD - minD
			w := bits.Len32(R)
			totalFORBits += w * count

			// Compute residuals
			residuals := make([]uint32, count)
			for i, d := range groupDeltas {
				residuals[i] = d - minD
			}

			// Truncated binary bits
			totalTruncBits += truncatedBinaryGroupBits(residuals, R)

			// Shannon entropy
			if R == 0 {
				// All values identical, entropy = 0
				entropyPerGroup = append(entropyPerGroup, 0)
				logRPerGroup = append(logRPerGroup, 0)
				continue
			}

			// Count frequencies
			freq := make(map[uint32]int)
			for _, r := range residuals {
				freq[r]++
			}

			H := 0.0
			n := float64(count)
			for _, f := range freq {
				p := float64(f) / n
				if p > 0 {
					H -= p * math.Log2(p)
				}
			}
			totalEntropyBits += H * n
			entropyPerGroup = append(entropyPerGroup, H)
			logR := math.Log2(float64(R + 1))
			logRPerGroup = append(logRPerGroup, logR)
		}

		// Aggregate stats
		forHdrBytes := nGroups * 5
		forDataBytes := (totalFORBits + 7) / 8
		forTotal := forHdrBytes + forDataBytes + 14 // +CRC+trailer

		truncDataBytes := (totalTruncBits + 7) / 8
		truncTotal := forHdrBytes + truncDataBytes + 14

		entropyDataBytes := int(math.Ceil(totalEntropyBits / 8))
		entropyTotal := forHdrBytes + entropyDataBytes + 14

		// Entropy stats
		sort.Float64s(entropyPerGroup)
		sort.Float64s(logRPerGroup)
		meanH := 0.0
		meanLogR := 0.0
		for i, h := range entropyPerGroup {
			meanH += h
			meanLogR += logRPerGroup[i]
		}
		meanH /= float64(len(entropyPerGroup))
		meanLogR /= float64(len(logRPerGroup))
		p50H := entropyPerGroup[len(entropyPerGroup)/2]

		fmt.Printf("\nGroup size %d (%d groups):\n", gs, nGroups)
		fmt.Printf("  Shannon entropy (bits/value): mean=%.2f, p50=%.2f\n", meanH, p50H)
		fmt.Printf("  Total sizes:\n")
		fmt.Printf("    FOR (fixed-width):    %s data + %s hdr = %s\n",
			fmtKB(float64(forDataBytes)), fmtKB(float64(forHdrBytes)), fmtKB(float64(forTotal)))
		truncPct := float64(forTotal-truncTotal) / float64(forTotal) * 100
		fmt.Printf("    Truncated binary:     %s data + %s hdr = %s  (-%.1f%%)\n",
			fmtKB(float64(truncDataBytes)), fmtKB(float64(forHdrBytes)), fmtKB(float64(truncTotal)), truncPct)
		entropyPct := float64(forTotal-entropyTotal) / float64(forTotal) * 100
		fmt.Printf("    Entropy-optimal:      %s data + %s hdr = %s  (-%.1f%%)\n",
			fmtKB(float64(entropyDataBytes)), fmtKB(float64(forHdrBytes)), fmtKB(float64(entropyTotal)), entropyPct)

		// Uniformity gap
		uniformityGap := meanLogR - meanH
		fmt.Printf("  Uniformity gap: actual H=%.2f vs log2(R+1)=%.2f = %.2f bits/value\n",
			meanH, meanLogR, uniformityGap)
	}
}

// --- Benchmark: zstd on FOR ---

func TestZstdOnFOR(t *testing.T) {
	ensureTestData(t)

	deltas := deltas1x
	fmt.Printf("\n=== zstd on FOR (%dK records) ===\n", len(deltas)/1000)

	for _, gs := range []int{64, 128, 256} {
		forBlob := FOREncode(deltas, gs)
		// Strip trailer (10) and CRC (4) to get just the packed data
		forDataOnly := forBlob[:len(forBlob)-14]
		forZstd := zstdEncoder.EncodeAll(forDataOnly, nil)

		fmt.Printf("\nGroup size %d:\n", gs)
		fmt.Printf("  FOR blob: %s -> zstd: %s (%.2fx ratio)\n",
			fmtKB(float64(len(forDataOnly))),
			fmtKB(float64(len(forZstd))),
			float64(len(forDataOnly))/float64(len(forZstd)))
	}

	// Varint + zstd baseline
	fmt.Printf("\nVarint + zstd baseline:\n")
	varintBuf := make([]byte, len(deltas)*binary.MaxVarintLen32)
	pos := 0
	for _, d := range deltas {
		pos += binary.PutUvarint(varintBuf[pos:], uint64(d))
	}
	varintData := varintBuf[:pos]
	varintZstd := zstdEncoder.EncodeAll(varintData, nil)
	fmt.Printf("  Varint blob: %s -> zstd: %s (%.2fx ratio)\n",
		fmtKB(float64(len(varintData))),
		fmtKB(float64(len(varintZstd))),
		float64(len(varintData))/float64(len(varintZstd)))
}

// --- Benchmark: FSE on FOR residuals ---

func TestFSEOnResiduals(t *testing.T) {
	ensureTestData(t)

	deltas := deltas1x
	fmt.Printf("\n=== FSE on FOR Residuals (%dK records) ===\n", len(deltas)/1000)

	// We test several byte-mapping strategies for 13-bit residuals:
	// 1. Split: interleaved high-byte stream + low-byte stream, FSE each independently
	// 2. Whole-file: all residuals as 2-byte LE pairs, single FSE block
	// 3. Per-group: FSE per group (tests overhead vs compression tradeoff)

	for _, gs := range []int{128, 256, 512} {
		nGroups := (len(deltas) + gs - 1) / gs
		forHdrBytes := nGroups * 5

		// Collect all residuals and per-group residuals
		type groupInfo struct {
			residuals []uint32
			R         uint32
		}
		groups := make([]groupInfo, nGroups)
		var allResiduals []uint32

		for g := 0; g < nGroups; g++ {
			start := g * gs
			end := start + gs
			if end > len(deltas) {
				end = len(deltas)
			}
			groupDeltas := deltas[start:end]

			minD, maxD := groupDeltas[0], groupDeltas[0]
			for _, d := range groupDeltas[1:] {
				if d < minD {
					minD = d
				}
				if d > maxD {
					maxD = d
				}
			}

			residuals := make([]uint32, len(groupDeltas))
			for i, d := range groupDeltas {
				residuals[i] = d - minD
			}
			groups[g] = groupInfo{residuals, maxD - minD}
			allResiduals = append(allResiduals, residuals...)
		}

		// --- Strategy 1: Split high/low byte streams, FSE whole-file ---
		highBytes := make([]byte, len(allResiduals))
		lowBytes := make([]byte, len(allResiduals))
		for i, r := range allResiduals {
			highBytes[i] = byte(r >> 8)
			lowBytes[i] = byte(r & 0xFF)
		}

		var scratchHi, scratchLo fse.Scratch
		highComp, errHi := fse.Compress(highBytes, &scratchHi)
		lowComp, errLo := fse.Compress(lowBytes, &scratchLo)

		splitTotal := 0
		splitDataStr := ""
		if errHi != nil || errLo != nil {
			// If FSE can't compress (e.g., incompressible), fall back to raw
			hiSize := len(highBytes)
			loSize := len(lowBytes)
			if errHi == nil {
				hiSize = len(highComp)
			}
			if errLo == nil {
				loSize = len(lowComp)
			}
			splitTotal = forHdrBytes + hiSize + loSize + 14
			splitDataStr = fmt.Sprintf("hi=%s(err=%v) + lo=%s(err=%v)",
				fmtKB(float64(hiSize)), errHi, fmtKB(float64(loSize)), errLo)
		} else {
			splitData := len(highComp) + len(lowComp)
			splitTotal = forHdrBytes + splitData + 14
			splitDataStr = fmt.Sprintf("hi=%s + lo=%s = %s",
				fmtKB(float64(len(highComp))), fmtKB(float64(len(lowComp))), fmtKB(float64(splitData)))
		}

		// --- Strategy 2: 2-byte LE pairs, single FSE block ---
		pairBytes := make([]byte, len(allResiduals)*2)
		for i, r := range allResiduals {
			pairBytes[i*2] = byte(r & 0xFF)
			pairBytes[i*2+1] = byte(r >> 8)
		}
		var scratchPair fse.Scratch
		pairComp, errPair := fse.Compress(pairBytes, &scratchPair)
		pairTotal := 0
		pairDataStr := ""
		if errPair != nil {
			pairTotal = forHdrBytes + len(pairBytes) + 14
			pairDataStr = fmt.Sprintf("%s (err=%v)", fmtKB(float64(len(pairBytes))), errPair)
		} else {
			pairTotal = forHdrBytes + len(pairComp) + 14
			pairDataStr = fmtKB(float64(len(pairComp)))
		}

		// --- Strategy 3: Per-group FSE on high/low bytes ---
		perGroupTotal := 0
		perGroupFailed := 0
		for _, gi := range groups {
			gHi := make([]byte, len(gi.residuals))
			gLo := make([]byte, len(gi.residuals))
			for i, r := range gi.residuals {
				gHi[i] = byte(r >> 8)
				gLo[i] = byte(r & 0xFF)
			}
			var sHi, sLo fse.Scratch
			cHi, eHi := fse.Compress(gHi, &sHi)
			cLo, eLo := fse.Compress(gLo, &sLo)
			hiSize := len(gHi)
			loSize := len(gLo)
			if eHi == nil {
				hiSize = len(cHi)
			} else {
				perGroupFailed++
			}
			if eLo == nil {
				loSize = len(cLo)
			} else {
				perGroupFailed++
			}
			perGroupTotal += hiSize + loSize
		}
		perGroupTotalWithHdr := forHdrBytes + perGroupTotal + 14

		// --- Strategy 4: zstd on high/low byte streams ---
		highZstd := zstdEncoder.EncodeAll(highBytes, nil)
		lowZstd := zstdEncoder.EncodeAll(lowBytes, nil)
		zstdSplitData := len(highZstd) + len(lowZstd)
		zstdSplitTotal := forHdrBytes + zstdSplitData + 14

		// --- FOR baseline ---
		_, forDataBytes, forTotal := FOREncodeSize(deltas, gs)

		// --- Report ---
		fmt.Printf("\nGroup size %d (%d groups, %d residuals):\n", gs, nGroups, len(allResiduals))
		fmt.Printf("  FOR baseline:       %s data + %s hdr = %s\n",
			fmtKB(float64(forDataBytes)), fmtKB(float64(forHdrBytes)), fmtKB(float64(forTotal)))
		fmt.Printf("  FSE split hi/lo:    %s + %s hdr = %s  (%.1f%% of FOR)\n",
			splitDataStr, fmtKB(float64(forHdrBytes)), fmtKB(float64(splitTotal)),
			float64(splitTotal)/float64(forTotal)*100)
		fmt.Printf("  FSE 2-byte pairs:   %s + %s hdr = %s  (%.1f%% of FOR)\n",
			pairDataStr, fmtKB(float64(forHdrBytes)), fmtKB(float64(pairTotal)),
			float64(pairTotal)/float64(forTotal)*100)
		fmt.Printf("  FSE per-group hi/lo: %s data + %s hdr = %s  (%.1f%% of FOR, %d FSE failures)\n",
			fmtKB(float64(perGroupTotal)), fmtKB(float64(forHdrBytes)), fmtKB(float64(perGroupTotalWithHdr)),
			float64(perGroupTotalWithHdr)/float64(forTotal)*100, perGroupFailed)
		fmt.Printf("  zstd split hi/lo:   hi=%s + lo=%s = %s + %s hdr = %s  (%.1f%% of FOR)\n",
			fmtKB(float64(len(highZstd))), fmtKB(float64(len(lowZstd))),
			fmtKB(float64(zstdSplitData)), fmtKB(float64(forHdrBytes)), fmtKB(float64(zstdSplitTotal)),
			float64(zstdSplitTotal)/float64(forTotal)*100)

		// High-byte distribution analysis
		hiHist := scratchHi.Histogram()
		nonZero := 0
		for _, c := range hiHist {
			if c > 0 {
				nonZero++
			}
		}
		fmt.Printf("  High-byte symbols used: %d/256\n", nonZero)
	}

	// --- Decode speed comparison: FSE high-bytes only (low bytes raw) ---
	fmt.Printf("\n--- Decode speed: FOR vs FSE-hi + raw-lo (group 128) ---\n")

	// Prepare data
	nGroups128 := (len(deltas) + 127) / 128
	var allRes128 []uint32
	for g := 0; g < nGroups128; g++ {
		start := g * 128
		end := start + 128
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]
		minD := groupDeltas[0]
		for _, d := range groupDeltas[1:] {
			if d < minD {
				minD = d
			}
		}
		for _, d := range groupDeltas {
			allRes128 = append(allRes128, d-minD)
		}
	}

	hi128 := make([]byte, len(allRes128))
	lo128 := make([]byte, len(allRes128))
	for i, r := range allRes128 {
		hi128[i] = byte(r >> 8)
		lo128[i] = byte(r & 0xFF)
	}

	var sH fse.Scratch
	compHi, errH := fse.Compress(hi128, &sH)
	if errH != nil {
		fmt.Printf("  FSE hi compression failed: %v, skipping decode bench\n", errH)
		return
	}

	// Size summary for best practical approach: FSE(hi) + raw(lo) + FOR headers
	fseHiRawLoData := len(compHi) + len(lo128)
	forHdrBytes128 := nGroups128 * 5
	fseHiRawLoTotal := forHdrBytes128 + fseHiRawLoData + 14
	_, _, forTotal128 := FOREncodeSize(deltas, 128)
	fmt.Printf("  FSE(hi) + raw(lo): %s + %s = %s data + %s hdr = %s  (%.1f%% of FOR)\n",
		fmtKB(float64(len(compHi))), fmtKB(float64(len(lo128))),
		fmtKB(float64(fseHiRawLoData)), fmtKB(float64(forHdrBytes128)),
		fmtKB(float64(fseHiRawLoTotal)), float64(fseHiRawLoTotal)/float64(forTotal128)*100)

	// FOR decode baseline
	forData128 := FOREncode(deltas, 128)
	const decIter = 5000

	forTimes := make([]int64, decIter)
	for i := range forTimes {
		start := time.Now()
		_ = FORDecode(forData128, len(deltas), 128)
		forTimes[i] = time.Since(start).Microseconds()
	}
	forSorted := sortedInt64(forTimes)

	// FSE decode (hi only — lo bytes are raw/memcpy)
	fseTimes := make([]int64, decIter)
	for i := range fseTimes {
		start := time.Now()
		var dH fse.Scratch
		dH.Out = make([]byte, 0, len(hi128))
		_, _ = fse.Decompress(compHi, &dH)
		// lo128 would just be a memcpy — negligible
		fseTimes[i] = time.Since(start).Microseconds()
	}
	fseSorted := sortedInt64(fseTimes)

	fmt.Printf("  FOR-128 decode:  p50=%dus, p99=%dus\n", p50i64(forSorted), p99i64(forSorted))
	fmt.Printf("  FSE hi decomp:   p50=%dus, p99=%dus  (+ memcpy for lo)\n", p50i64(fseSorted), p99i64(fseSorted))
	ratio := float64(p50i64(fseSorted)) / float64(p50i64(forSorted))
	fmt.Printf("  FSE/FOR ratio:   %.2fx\n", ratio)
}
