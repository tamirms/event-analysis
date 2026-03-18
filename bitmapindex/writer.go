package bitmapindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"

	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/packfile"
)

const defaultBatchSize = 128

// WriterOptions configures Writer behavior.
type WriterOptions struct {
	BatchSize    int  // bitmaps per packfile record; 0 → 128
	CapacityHint int  // pre-sizes internal map; 0 → no hint
	Compress     bool // zstd-compress batches; default false (CRC32C integrity only)
	ContentHash  bool // compute SHA-256 content hash over rank-ordered entries
}

// Writer accumulates (ordinal, key) tuples and builds an MPHF+packfile index.
// Not safe for concurrent use — all calls to Add must be serialized.
type Writer struct {
	mphfPath    string
	dataPath    string
	bitmaps     map[[16]byte]*roaring.Bitmap
	batchSize   int
	compress    bool
	contentHash bool
}

// NewWriter creates a new bitmap index writer.
func NewWriter(mphfPath, dataPath string, opts WriterOptions) *Writer {
	bs := opts.BatchSize
	if bs <= 0 {
		bs = defaultBatchSize
	}
	hint := max(opts.CapacityHint, 0)
	return &Writer{
		mphfPath:    mphfPath,
		dataPath:    dataPath,
		bitmaps:     make(map[[16]byte]*roaring.Bitmap, hint),
		batchSize:   bs,
		compress:    opts.Compress,
		contentHash: opts.ContentHash,
	}
}

// Add indexes all fields of ev (ContractID and up to 4 topics) under the given ordinal.
func (w *Writer) Add(ev *event.Event, id uint32) {
	var buf [256]byte
	if ev.ContractID != nil {
		w.add(composeKey(buf[:0], FieldContractID, ev.ContractID), id)
	}
	topicCount := min(len(ev.Topics), int(fieldCount)-int(FieldTopic0))
	for i := range topicCount {
		w.add(composeKey(buf[:0], FieldTopic0+Field(i), ev.Topics[i]), id)
	}
}

func (w *Writer) add(key []byte, ordinal uint32) {
	var hk [16]byte
	streamhash.PreHashInPlace(key, hk[:])
	bm := w.bitmaps[hk]
	if bm == nil {
		bm = roaring.New()
		w.bitmaps[hk] = bm
	}
	bm.Add(ordinal)
}

// Finish builds the MPHF and packfile, writing them to the configured paths.
func (w *Writer) Finish(ctx context.Context) (err error) {
	mphfPath := w.mphfPath
	dataPath := w.dataPath
	totalKeys := len(w.bitmaps)
	if totalKeys == 0 {
		return fmt.Errorf("bitmapindex: no keys to index")
	}
	if totalKeys > math.MaxUint32 {
		return fmt.Errorf("bitmapindex: key count %d exceeds uint32 max", totalKeys)
	}

	// Collect and sort MPHF keys.
	sortedKeys := slices.SortedFunc(maps.Keys(w.bitmaps),
		func(a, b [16]byte) int { return bytes.Compare(a[:], b[:]) })

	// Build MPHF.
	builder, err := streamhash.NewBuilder(ctx, mphfPath, uint64(totalKeys),
		streamhash.WithWorkers(4),
	)
	if err != nil {
		return fmt.Errorf("bitmapindex: create MPHF builder: %w", err)
	}

	defer func() {
		if err != nil {
			os.Remove(mphfPath)
		}
	}()

	for _, hk := range sortedKeys {
		if err := builder.AddKey(hk[:], 0); err != nil {
			return errors.Join(
				fmt.Errorf("bitmapindex: add key to MPHF: %w", err),
				builder.Close(),
			)
		}
	}
	if err := builder.Finish(); err != nil {
		return fmt.Errorf("bitmapindex: finish MPHF: %w", err)
	}

	// Open MPHF and assign bitmaps to rank slots.
	idx, err := streamhash.Open(mphfPath)
	if err != nil {
		return fmt.Errorf("bitmapindex: open MPHF for ranking: %w", err)
	}
	defer idx.Close()

	type rankedEntry struct {
		fingerprint [4]byte
		bitmap      *roaring.Bitmap
	}
	ranked := make([]rankedEntry, totalKeys)

	for _, hk := range sortedKeys {
		rank, err := idx.Query(hk[:])
		if err != nil {
			return fmt.Errorf("bitmapindex: query MPHF for rank: %w", err)
		}
		ranked[rank] = rankedEntry{
			fingerprint: [4]byte(hk[:4]),
			bitmap:      w.bitmaps[hk],
		}
	}

	// Free the original map.
	w.bitmaps = nil

	// Per-bitmap preparation (RunOptimize + serialize).
	type preparedBitmap struct {
		fingerprint [4]byte
		data        []byte
	}
	prepared := make([]preparedBitmap, totalKeys)

	for i := range totalKeys {
		entry := &ranked[i]
		entry.bitmap.RunOptimize()
		raw, err := entry.bitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("bitmapindex: serialize bitmap %d: %w", i, err)
		}
		entry.bitmap = nil
		prepared[i] = preparedBitmap{
			fingerprint: entry.fingerprint,
			data:        raw,
		}
	}

	// Write to packfile using the new item-level API.
	format := packfile.Compressed
	if !w.compress {
		format = packfile.Uncompressed
	}
	pw, err := packfile.Create(dataPath, packfile.WriterOptions{
		RecordSize:  w.batchSize,
		Format:      format,
		ContentHash: w.contentHash,
	})
	if err != nil {
		return fmt.Errorf("bitmapindex: create packfile: %w", err)
	}

	for i := range totalKeys {
		if err := pw.Append(prepared[i].fingerprint[:], prepared[i].data); err != nil {
			return errors.Join(
				fmt.Errorf("bitmapindex: append item %d: %w", i, err),
				pw.Close(),
			)
		}
		prepared[i].data = nil // free as we go
	}

	if err := pw.Finish(nil); err != nil {
		return errors.Join(
			fmt.Errorf("bitmapindex: finish packfile: %w", err),
			pw.Close(),
		)
	}

	return nil
}
