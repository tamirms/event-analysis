package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

// Writer accumulates (ordinal, key) tuples and builds an MPHF+packfile index.
// Not safe for concurrent use — all calls to Add must be serialized.
type Writer struct {
	bitmaps map[[16]byte]*roaring.Bitmap
}

// WriterOption configures Writer behavior.
type WriterOption func(*writerConfig)

type writerConfig struct {
	capacity int
}

// WithCapacityHint pre-sizes the internal map for n expected unique keys.
func WithCapacityHint(n int) WriterOption {
	return func(c *writerConfig) { c.capacity = n }
}

// NewWriter creates a new bitmap index writer.
func NewWriter(opts ...WriterOption) *Writer {
	cfg := writerConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return &Writer{
		bitmaps: make(map[[16]byte]*roaring.Bitmap, cfg.capacity),
	}
}

// Add records that the event at ordinal has the given key.
// The key should be pre-composed via ComposeKey if field disambiguation is needed.
func (w *Writer) Add(ordinal uint32, key []byte) {
	var hk [16]byte
	streamhash.PreHashInPlace(key, hk[:])
	bm := w.bitmaps[hk]
	if bm == nil {
		bm = roaring.New()
		w.bitmaps[hk] = bm
	}
	bm.Add(ordinal)
}

// Finish builds the MPHF and packfile, writing them to the given paths.
func (w *Writer) Finish(ctx context.Context, mphfPath, dataPath string) error {
	totalKeys := len(w.bitmaps)
	if totalKeys == 0 {
		return fmt.Errorf("bitmapindex: no keys to index")
	}

	// Phase 2a: Collect and sort MPHF keys.
	sortedKeys := make([][16]byte, 0, totalKeys)
	for mk := range w.bitmaps {
		sortedKeys = append(sortedKeys, mk)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return bytes.Compare(sortedKeys[i][:], sortedKeys[j][:]) < 0
	})

	// Phase 2b: Build rank-only MPHF from sorted keys.
	builder, err := streamhash.NewBuilder(ctx, mphfPath, uint64(totalKeys),
		streamhash.WithWorkers(4),
	)
	if err != nil {
		return fmt.Errorf("bitmapindex: create MPHF builder: %w", err)
	}
	for _, hk := range sortedKeys {
		if err := builder.AddKey(hk[:], 0); err != nil {
			builder.Close()
			return fmt.Errorf("bitmapindex: add key to MPHF: %w", err)
		}
	}
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("bitmapindex: finish MPHF: %w", err)
	}

	// Phase 2c: Open MPHF, assign bitmaps to rank slots.
	idx, err := streamhash.Open(mphfPath)
	if err != nil {
		return fmt.Errorf("bitmapindex: open MPHF for ranking: %w", err)
	}

	type rankedEntry struct {
		fingerprint [4]byte
		bitmap      *roaring.Bitmap
	}
	ranked := make([]rankedEntry, totalKeys)

	for _, hk := range sortedKeys {
		rank, err := idx.Query(hk[:])
		if err != nil {
			idx.Close()
			return fmt.Errorf("bitmapindex: query MPHF for rank: %w", err)
		}
		ranked[rank] = rankedEntry{
			fingerprint: [4]byte(hk[:4]),
			bitmap:      w.bitmaps[hk],
		}
	}
	idx.Close()

	// Free the original map — bitmaps are now owned by ranked[].
	w.bitmaps = nil

	// Phase 2d: Parallel record preparation (RunOptimize + serialize + compress + CRC32C).
	records := make([][]byte, totalKeys)
	ch := make(chan int, totalKeys)
	for i := range totalKeys {
		ch <- i
	}
	close(ch)

	numWorkers := runtime.NumCPU()
	if numWorkers > totalKeys {
		numWorkers = totalKeys
	}

	var wg sync.WaitGroup
	var firstErr atomic.Pointer[error]
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			enc := zstd.NewCompressor()
			defer enc.Close()

			for i := range ch {
				entry := &ranked[i]
				entry.bitmap.RunOptimize()
				raw, err := entry.bitmap.ToBytes()
				if err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				entry.bitmap = nil // free bitmap memory

				rec := make([]byte, 0, fingerprintSize+1+len(raw)+checksumSize)
				rec = append(rec, entry.fingerprint[:]...)
				if len(raw) >= 256 {
					// Encode returns scratch-backed slice; append copies before next Encode.
					compressed := enc.Encode(raw)
					if len(compressed) < len(raw) {
						rec = append(rec, flagCompressed)
						rec = append(rec, compressed...)
						// Append CRC32C over the full record.
						crc := crc32.Checksum(rec, crc32cTable)
						var crcBuf [4]byte
						binary.LittleEndian.PutUint32(crcBuf[:], crc)
						rec = append(rec, crcBuf[:]...)
						records[i] = rec
						continue
					}
				}
				rec = append(rec, 0x00)
				rec = append(rec, raw...)
				// Append CRC32C over the full record.
				crc := crc32.Checksum(rec, crc32cTable)
				var crcBuf [4]byte
				binary.LittleEndian.PutUint32(crcBuf[:], crc)
				rec = append(rec, crcBuf[:]...)
				records[i] = rec
			}
		}()
	}
	wg.Wait()

	if ep := firstErr.Load(); ep != nil {
		return fmt.Errorf("bitmapindex: record preparation: %w", *ep)
	}

	// Phase 2e: Sequential packfile write.
	pw, err := packfile.Create(dataPath, packfile.WriterOptions{})
	if err != nil {
		return fmt.Errorf("bitmapindex: create packfile: %w", err)
	}
	for _, rec := range records {
		if err := pw.Append(rec); err != nil {
			pw.Abort()
			return fmt.Errorf("bitmapindex: append record: %w", err)
		}
	}
	if _, err := pw.Finish(); err != nil {
		return fmt.Errorf("bitmapindex: finish packfile: %w", err)
	}

	return nil
}
