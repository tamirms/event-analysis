package main

// Benchmarks comparing our CGO zstd wrapper (see package zstd for rationale)
// against klauspost/compress (pure Go). Requires source event data.
// Use MAX_EVENTS=500000 for fast runs.

import (
	"fmt"
	"sync"
	"testing"

	klauspostzstd "github.com/klauspost/compress/zstd"

	"github.com/tamir/events-analysis/zstd"
)

// zstdBlocks holds pre-built uncompressed blocks for benchmarking.
// Each block is 128 contiguous events concatenated, matching production layout.
var (
	zstdBlocks     [][]byte
	zstdBlocksOnce sync.Once
)

func buildZstdBlocks(b *testing.B) {
	b.Helper()
	zstdBlocksOnce.Do(func() {
		ensureAllEvents(b)
		const blockN = 128
		n := len(allEvents)
		numBlocks := (n + blockN - 1) / blockN
		zstdBlocks = make([][]byte, numBlocks)
		for i := range numBlocks {
			start := i * blockN
			end := start + blockN
			if end > n {
				end = n
			}
			var size int
			for _, ev := range allEvents[start:end] {
				size += len(ev)
			}
			block := make([]byte, 0, size)
			for _, ev := range allEvents[start:end] {
				block = append(block, ev...)
			}
			zstdBlocks[i] = block
		}
		fmt.Printf("zstd bench: %d blocks built (avg %.0f bytes)\n", numBlocks, func() float64 {
			total := 0
			for _, b := range zstdBlocks {
				total += len(b)
			}
			return float64(total) / float64(len(zstdBlocks))
		}())
	})
}

func zstdTotalBytes() int64 {
	var total int64
	for _, blk := range zstdBlocks {
		total += int64(len(blk))
	}
	return total
}

// --- cgo-zstd (CGO wrapper) compress ---

func BenchmarkZstdCgoCompress(b *testing.B) {
	buildZstdBlocks(b)
	c := zstd.NewCompressor()
	defer c.Close()

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	var dst []byte
	for range b.N {
		for _, blk := range zstdBlocks {
			out, err := c.Encode(blk)
			if err != nil {
				b.Fatal(err)
			}
			dst = append(dst[:0], out...)
		}
	}
	benchSink = dst
}

// --- Klauspost (pure Go) compress ---

func BenchmarkZstdKlauspostCompress(b *testing.B) {
	buildZstdBlocks(b)
	enc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	var dst []byte
	for range b.N {
		for _, blk := range zstdBlocks {
			dst = enc.EncodeAll(blk, dst[:0])
		}
	}
	benchSink = dst
}

// --- cgo-zstd decompress ---

func BenchmarkZstdCgoDecompress(b *testing.B) {
	buildZstdBlocks(b)
	c := zstd.NewCompressor()
	compressed := make([][]byte, len(zstdBlocks))
	for i, blk := range zstdBlocks {
		out, err := c.Encode(blk)
		if err != nil {
			b.Fatal(err)
		}
		compressed[i] = make([]byte, len(out))
		copy(compressed[i], out)
	}
	c.Close()

	d := zstd.NewDecompressor()
	defer d.Close()

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	var dst []byte
	for range b.N {
		for _, comp := range compressed {
			var err error
			dst, err = d.Decode(dst, comp)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	benchSink = dst
}

// --- Klauspost decompress ---

func BenchmarkZstdKlauspostDecompress(b *testing.B) {
	buildZstdBlocks(b)
	enc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		b.Fatal(err)
	}
	dec, err := klauspostzstd.NewReader(nil, klauspostzstd.WithDecoderConcurrency(1))
	if err != nil {
		b.Fatal(err)
	}

	compressed := make([][]byte, len(zstdBlocks))
	for i, blk := range zstdBlocks {
		compressed[i] = enc.EncodeAll(blk, nil)
	}

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	var dst []byte
	for range b.N {
		for _, comp := range compressed {
			dst, err = dec.DecodeAll(comp, dst[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	benchSink = dst
}

// --- Parallel compress (simulates streaming pipeline with N workers) ---

func benchCgoCompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	for range b.N {
		ch := make(chan int, len(zstdBlocks))
		for i := range zstdBlocks {
			ch <- i
		}
		close(ch)

		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				c := zstd.NewCompressor()
				defer c.Close()
				var dst []byte
				for i := range ch {
					out, err := c.Encode(zstdBlocks[i])
					if err != nil {
						b.Error(err)
						return
					}
					dst = append(dst[:0], out...)
				}
				_ = dst
			}()
		}
		wg.Wait()
	}
}

func benchKlauspostCompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	encoders := make([]*klauspostzstd.Encoder, workers)
	for i := range workers {
		enc, err := klauspostzstd.NewWriter(nil,
			klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
			klauspostzstd.WithEncoderCRC(true),
		)
		if err != nil {
			b.Fatal(err)
		}
		encoders[i] = enc
	}

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	for range b.N {
		ch := make(chan int, len(zstdBlocks))
		for i := range zstdBlocks {
			ch <- i
		}
		close(ch)

		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			go func() {
				defer wg.Done()
				enc := encoders[w]
				var dst []byte
				for i := range ch {
					dst = enc.EncodeAll(zstdBlocks[i], dst[:0])
				}
				_ = dst
			}()
		}
		wg.Wait()
	}
}

func BenchmarkZstdCgoCompressP8(b *testing.B) { benchCgoCompressParallel(b, 8) }
func BenchmarkZstdKlauspostCompressP8(b *testing.B)   { benchKlauspostCompressParallel(b, 8) }

// --- Parallel decompress ---

func benchCgoDecompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)
	c := zstd.NewCompressor()
	compressed := make([][]byte, len(zstdBlocks))
	for i, blk := range zstdBlocks {
		out, err := c.Encode(blk)
		if err != nil {
			b.Fatal(err)
		}
		compressed[i] = make([]byte, len(out))
		copy(compressed[i], out)
	}
	c.Close()

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	for range b.N {
		ch := make(chan int, len(compressed))
		for i := range compressed {
			ch <- i
		}
		close(ch)

		var firstErr error
		var errOnce sync.Once
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				d := zstd.NewDecompressor()
				defer d.Close()
				var dst []byte
				for i := range ch {
					var err error
					dst, err = d.Decode(dst, compressed[i])
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
				}
				_ = dst
			}()
		}
		wg.Wait()
		if firstErr != nil {
			b.Fatal(firstErr)
		}
	}
}

func benchKlauspostDecompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)
	enc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		b.Fatal(err)
	}
	compressed := make([][]byte, len(zstdBlocks))
	for i, blk := range zstdBlocks {
		compressed[i] = enc.EncodeAll(blk, nil)
	}

	decoders := make([]*klauspostzstd.Decoder, workers)
	for i := range workers {
		dec, err := klauspostzstd.NewReader(nil, klauspostzstd.WithDecoderConcurrency(1))
		if err != nil {
			b.Fatal(err)
		}
		decoders[i] = dec
	}
	b.Cleanup(func() {
		for _, dec := range decoders {
			dec.Close()
		}
	})

	b.SetBytes(zstdTotalBytes())
	b.ResetTimer()
	for range b.N {
		ch := make(chan int, len(compressed))
		for i := range compressed {
			ch <- i
		}
		close(ch)

		var firstErr error
		var errOnce sync.Once
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			go func() {
				defer wg.Done()
				dec := decoders[w]
				var dst []byte
				for i := range ch {
					var decErr error
					dst, decErr = dec.DecodeAll(compressed[i], dst[:0])
					if decErr != nil {
						errOnce.Do(func() { firstErr = decErr })
						return
					}
				}
				_ = dst
			}()
		}
		wg.Wait()
		if firstErr != nil {
			b.Fatal(firstErr)
		}
	}
}

func BenchmarkZstdCgoDecompressP8(b *testing.B) { benchCgoDecompressParallel(b, 8) }
func BenchmarkZstdKlauspostDecompressP8(b *testing.B)   { benchKlauspostDecompressParallel(b, 8) }

// --- Compression ratio comparison ---

func TestZstdCompressionRatio(t *testing.T) {
	if err := loadAllEvents(); err != nil {
		t.Fatal(err)
	}

	const blockN = 128
	n := len(allEvents)
	numBlocks := (n + blockN - 1) / blockN

	blocks := make([][]byte, numBlocks)
	var totalRaw int64
	for i := range numBlocks {
		start := i * blockN
		end := start + blockN
		if end > n {
			end = n
		}
		var size int
		for _, ev := range allEvents[start:end] {
			size += len(ev)
		}
		block := make([]byte, 0, size)
		for _, ev := range allEvents[start:end] {
			block = append(block, ev...)
		}
		blocks[i] = block
		totalRaw += int64(len(block))
	}

	// cgo-zstd (C libzstd level 3 with checksum).
	c := zstd.NewCompressor()
	var rcTotal int64
	for _, blk := range blocks {
		out, err := c.Encode(blk)
		if err != nil {
			t.Fatal(err)
		}
		rcTotal += int64(len(out))
	}
	c.Close()

	// Klauspost (pure Go, level 3 with CRC).
	klEnc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	var klTotal int64
	for _, blk := range blocks {
		klTotal += int64(len(klEnc.EncodeAll(blk, nil)))
	}

	fmt.Printf("Compression ratio comparison (level 3, %d blocks, %d events):\n", numBlocks, n)
	fmt.Printf("  Raw:         %s\n", fmtKB(float64(totalRaw)))
	fmt.Printf("  cgo-zstd: %s (%.2fx)\n", fmtKB(float64(rcTotal)), float64(totalRaw)/float64(rcTotal))
	fmt.Printf("  klauspost:   %s (%.2fx)\n", fmtKB(float64(klTotal)), float64(totalRaw)/float64(klTotal))
}
