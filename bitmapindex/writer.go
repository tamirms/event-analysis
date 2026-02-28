package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"

	"crypto/sha256"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"

	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/record"
	"github.com/tamir/events-analysis/zstd"
)

const (
	defaultBatchSize = 128
	// maxBatchSize caps the batch size — larger batches make no practical
	// sense for bitmaps. The metadata format supports u32 but we keep
	// the cap at 65535.
	maxBatchSize = 1<<16 - 1
)

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

// NewWriter creates a new bitmap index writer. mphfPath and dataPath are the
// output paths for the MPHF index and packfile data files respectively.
func NewWriter(mphfPath, dataPath string, opts WriterOptions) *Writer {
	bs := opts.BatchSize
	if bs <= 0 {
		bs = defaultBatchSize
	}
	if bs > maxBatchSize {
		bs = maxBatchSize
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

// add records that the event at ordinal has the given composed key.
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

	// Cleanup partial MPHF file on any subsequent failure.
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

	// Free the original map — bitmaps are now owned by ranked[].
	w.bitmaps = nil

	// Per-bitmap preparation (RunOptimize + serialize).
	type preparedBitmap struct {
		fingerprint [4]byte
		data        []byte // raw serialized bitmap (uncompressed)
	}
	prepared := make([]preparedBitmap, totalKeys)

	for i := range totalKeys {
		entry := &ranked[i]
		entry.bitmap.RunOptimize()
		raw, err := entry.bitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("bitmapindex: serialize bitmap %d: %w", i, err)
		}
		entry.bitmap = nil // free bitmap memory
		prepared[i] = preparedBitmap{
			fingerprint: entry.fingerprint,
			data:        raw,
		}
	}

	// Compute content hash over rank-ordered entries if enabled.
	var contentHash []byte
	if w.contentHash {
		hasher := sha256.New()
		var lenBuf [4]byte
		// Each logical entry is fingerprint + data, hashed as one length-prefixed unit.
		// Write length prefix, fingerprint, and data as separate Write calls to avoid
		// copying each entry into a temporary buffer.
		for i := range totalKeys {
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(fingerprintSize+len(prepared[i].data)))
			hasher.Write(lenBuf[:])
			hasher.Write(prepared[i].fingerprint[:])
			hasher.Write(prepared[i].data)
		}
		contentHash = hasher.Sum(nil)
	}

	// Group into batch records and write to packfile.
	batchSize := w.batchSize
	numBatches := (totalKeys + batchSize - 1) / batchSize

	pw, err := packfile.Create(dataPath, packfile.WriterOptions{})
	if err != nil {
		return fmt.Errorf("bitmapindex: create packfile: %w", err)
	}

	var flags uint32
	if !w.compress {
		flags |= record.FlagNoCompression
	}
	if w.contentHash {
		flags |= record.FlagContentHash
	}

	var compressor *zstd.Compressor
	if w.compress {
		compressor = zstd.NewCompressor()
		defer compressor.Close()
	}

	// Estimate initial capacity from the first batch.
	firstEnd := min(batchSize, totalKeys)
	initCap := 0
	for i := range firstEnd {
		initCap += fingerprintSize + len(prepared[i].data)
	}
	batchBuf := make([]byte, 0, initCap+32)
	sizes := make([]uint32, 0, batchSize)
	for b := range numBatches {
		start := b * batchSize
		end := min(start+batchSize, totalKeys)

		// Estimate capacity: colocated entries + trailing FOR.
		dataSize := 0
		for i := start; i < end; i++ {
			dataSize += fingerprintSize + len(prepared[i].data)
		}
		batchBuf = batchBuf[:0]
		if cap(batchBuf) < dataSize+32 {
			batchBuf = make([]byte, 0, dataSize+32)
		}
		sizes = sizes[:0]

		// Colocated entries: [fp0 bm0][fp1 bm1]...
		for i := start; i < end; i++ {
			entrySize := fingerprintSize + len(prepared[i].data)
			batchBuf = append(batchBuf, prepared[i].fingerprint[:]...)
			batchBuf = append(batchBuf, prepared[i].data...)
			sizes = append(sizes, uint32(entrySize))

			prepared[i].data = nil // free as we go
		}

		// Append trailing FOR-encoded sizes.
		batchBuf = append(batchBuf, intpack.EncodeTrailingGroup(sizes)...)

		if w.compress {
			compressed, err := compressor.Encode(batchBuf)
			if err != nil {
				return errors.Join(
					fmt.Errorf("bitmapindex: compress batch %d: %w", b, err),
					pw.Abort(),
				)
			}
			batchBuf = append(batchBuf[:0], compressed...)
		} else {
			batchBuf = binary.LittleEndian.AppendUint32(batchBuf, packfile.CRC32C(batchBuf))
		}

		if err := pw.Append(batchBuf); err != nil {
			return errors.Join(
				fmt.Errorf("bitmapindex: append batch %d: %w", b, err),
				pw.Abort(),
			)
		}
	}

	meta := record.EncodeMetadata(totalKeys, batchSize, flags)
	if contentHash != nil {
		meta = append(meta, contentHash...)
	}

	if _, err := pw.Finish(meta); err != nil {
		return errors.Join(
			fmt.Errorf("bitmapindex: finish packfile: %w", err),
			pw.Abort(),
		)
	}

	return nil
}
