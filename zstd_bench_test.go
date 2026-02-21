package main

import (
	"fmt"
	"sync"
	"testing"

	datadogzstd "github.com/DataDog/zstd"
	klauspostzstd "github.com/klauspost/compress/zstd"
)

const zstdLevel = 3 // matched level for fair comparison

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

// --- Klauspost compress ---

func BenchmarkZstdKlauspostCompress(b *testing.B) {
	buildZstdBlocks(b)

	enc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		b.Fatal(err)
	}

	var totalBytes int64
	for _, blk := range zstdBlocks {
		totalBytes += int64(len(blk))
	}

	var dst []byte
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for range b.N {
		for _, blk := range zstdBlocks {
			dst = enc.EncodeAll(blk, dst[:0])
		}
	}
	benchSink = dst
}

// --- DataDog compress ---

func BenchmarkZstdDataDogCompress(b *testing.B) {
	buildZstdBlocks(b)

	ctx := datadogzstd.NewCtx()

	var totalBytes int64
	for _, blk := range zstdBlocks {
		totalBytes += int64(len(blk))
	}

	var dst []byte
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for range b.N {
		for _, blk := range zstdBlocks {
			var err error
			dst, err = ctx.CompressLevel(dst[:0], blk, zstdLevel)
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

	// Pre-compress all blocks.
	compressed := make([][]byte, len(zstdBlocks))
	var totalBytes int64
	for i, blk := range zstdBlocks {
		compressed[i] = enc.EncodeAll(blk, nil)
		totalBytes += int64(len(blk))
	}

	var dst []byte
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for range b.N {
		for _, c := range compressed {
			dst, err = dec.DecodeAll(c, dst[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	benchSink = dst
}

// --- DataDog decompress ---

func BenchmarkZstdDataDogDecompress(b *testing.B) {
	buildZstdBlocks(b)

	ctx := datadogzstd.NewCtx()

	// Pre-compress all blocks using DataDog at level 3.
	compressed := make([][]byte, len(zstdBlocks))
	var totalBytes int64
	for i, blk := range zstdBlocks {
		var err error
		compressed[i], err = ctx.CompressLevel(nil, blk, zstdLevel)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(blk))
	}

	var dst []byte
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for range b.N {
		for _, c := range compressed {
			var err error
			dst, err = ctx.Decompress(dst[:0], c)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	benchSink = dst
}

// --- Parallel compress (simulates streaming pipeline with N workers) ---

func benchZstdKlauspostCompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	var totalBytes int64
	for _, blk := range zstdBlocks {
		totalBytes += int64(len(blk))
	}

	// Pre-create encoders (amortize init cost, matches real pipeline usage).
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

	b.SetBytes(totalBytes)
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

func benchZstdDataDogCompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	var totalBytes int64
	for _, blk := range zstdBlocks {
		totalBytes += int64(len(blk))
	}

	// Pre-create contexts (not goroutine-safe, one per worker).
	contexts := make([]datadogzstd.Ctx, workers)
	for i := range workers {
		contexts[i] = datadogzstd.NewCtx()
	}

	b.SetBytes(totalBytes)
	b.ResetTimer()
	for range b.N {
		ch := make(chan int, len(zstdBlocks))
		for i := range zstdBlocks {
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
				ctx := contexts[w]
				var dst []byte
				for i := range ch {
					var err error
					dst, err = ctx.CompressLevel(dst[:0], zstdBlocks[i], zstdLevel)
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

func BenchmarkZstdKlauspostCompressP8(b *testing.B)  { benchZstdKlauspostCompressParallel(b, 8) }
func BenchmarkZstdKlauspostCompressP16(b *testing.B) { benchZstdKlauspostCompressParallel(b, 16) }
func BenchmarkZstdDataDogCompressP8(b *testing.B)    { benchZstdDataDogCompressParallel(b, 8) }
func BenchmarkZstdDataDogCompressP16(b *testing.B)   { benchZstdDataDogCompressParallel(b, 16) }

// --- Parallel decompress ---

func benchZstdKlauspostDecompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	enc, err := klauspostzstd.NewWriter(nil,
		klauspostzstd.WithEncoderLevel(klauspostzstd.SpeedDefault),
		klauspostzstd.WithEncoderCRC(true),
	)
	if err != nil {
		b.Fatal(err)
	}

	compressed := make([][]byte, len(zstdBlocks))
	var totalBytes int64
	for i, blk := range zstdBlocks {
		compressed[i] = enc.EncodeAll(blk, nil)
		totalBytes += int64(len(blk))
	}

	// Pre-create decoders (one per worker, matches real usage).
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

	b.SetBytes(totalBytes)
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

func benchZstdDataDogDecompressParallel(b *testing.B, workers int) {
	buildZstdBlocks(b)

	ctx := datadogzstd.NewCtx()

	compressed := make([][]byte, len(zstdBlocks))
	var totalBytes int64
	for i, blk := range zstdBlocks {
		var err error
		compressed[i], err = ctx.CompressLevel(nil, blk, zstdLevel)
		if err != nil {
			b.Fatal(err)
		}
		totalBytes += int64(len(blk))
	}

	// Pre-create contexts (not goroutine-safe, one per worker).
	contexts := make([]datadogzstd.Ctx, workers)
	for i := range workers {
		contexts[i] = datadogzstd.NewCtx()
	}

	b.SetBytes(totalBytes)
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
				dCtx := contexts[w]
				var dst []byte
				for i := range ch {
					var err error
					dst, err = dCtx.Decompress(dst[:0], compressed[i])
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

func BenchmarkZstdKlauspostDecompressP8(b *testing.B)  { benchZstdKlauspostDecompressParallel(b, 8) }
func BenchmarkZstdKlauspostDecompressP16(b *testing.B) { benchZstdKlauspostDecompressParallel(b, 16) }
func BenchmarkZstdDataDogDecompressP8(b *testing.B)    { benchZstdDataDogDecompressParallel(b, 8) }
func BenchmarkZstdDataDogDecompressP16(b *testing.B)   { benchZstdDataDogDecompressParallel(b, 16) }

// --- Compression ratio comparison ---

func TestZstdCompressionRatio(t *testing.T) {
	if err := loadAllEvents(); err != nil {
		t.Fatal(err)
	}

	const blockN = 128
	n := len(allEvents)
	numBlocks := (n + blockN - 1) / blockN

	// Build blocks.
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

	// Klauspost level 3.
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

	// DataDog level 3.
	ddCtx := datadogzstd.NewCtx()
	var ddTotal int64
	for _, blk := range blocks {
		c, err := ddCtx.CompressLevel(nil, blk, zstdLevel)
		if err != nil {
			t.Fatal(err)
		}
		ddTotal += int64(len(c))
	}

	fmt.Printf("Compression ratio comparison (level %d, %d blocks):\n", zstdLevel, numBlocks)
	fmt.Printf("  Raw:      %s\n", fmtKB(float64(totalRaw)))
	fmt.Printf("  Klauspost: %s (%.2fx)\n", fmtKB(float64(klTotal)), float64(totalRaw)/float64(klTotal))
	fmt.Printf("  DataDog:   %s (%.2fx)\n", fmtKB(float64(ddTotal)), float64(totalRaw)/float64(ddTotal))
}
