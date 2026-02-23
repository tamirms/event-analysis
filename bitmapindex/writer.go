package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"
	"golang.org/x/exp/maps"

	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

const defaultBatchSize = 128

// WriterOptions configures Writer behavior.
type WriterOptions struct {
	BatchSize    int // bitmaps per packfile record; 0 → 128
	CapacityHint int // pre-sizes internal map; 0 → no hint
	Concurrency  int // parallel compression goroutines; 0 or 1 → serial
}

// Writer accumulates (ordinal, key) tuples and builds an MPHF+packfile index.
// Not safe for concurrent use — all calls to Add must be serialized.
type Writer struct {
	bitmaps     map[[16]byte]*roaring.Bitmap
	batchSize   int
	concurrency int
}

// NewWriter creates a new bitmap index writer.
func NewWriter(opts WriterOptions) *Writer {
	bs := opts.BatchSize
	if bs <= 0 {
		bs = defaultBatchSize
	}
	if bs > 65535 {
		bs = 65535
	}
	hint := opts.CapacityHint
	if hint < 0 {
		hint = 0
	}
	conc := opts.Concurrency
	if conc < 1 {
		conc = 1
	}
	return &Writer{
		bitmaps:     make(map[[16]byte]*roaring.Bitmap, hint),
		batchSize:   bs,
		concurrency: conc,
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

// Finish builds the MPHF and packfile, writing them to the given paths.
func (w *Writer) Finish(ctx context.Context, mphfPath, dataPath string) (err error) {
	totalKeys := len(w.bitmaps)
	if totalKeys == 0 {
		return fmt.Errorf("bitmapindex: no keys to index")
	}

	// Collect and sort MPHF keys.
	sortedKeys := maps.Keys(w.bitmaps)
	sort.Slice(sortedKeys, func(i, j int) bool {
		return bytes.Compare(sortedKeys[i][:], sortedKeys[j][:]) < 0
	})

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

	// Per-bitmap preparation (RunOptimize + serialize + compress).
	type preparedBitmap struct {
		fingerprint [4]byte
		flags       byte
		data        []byte // compressed or raw bitmap payload
	}
	prepared := make([]preparedBitmap, totalKeys)

	prepareBitmap := func(i int, enc *zstd.Compressor) error {
		entry := &ranked[i]
		entry.bitmap.RunOptimize()
		raw, err := entry.bitmap.ToBytes()
		if err != nil {
			return err
		}
		entry.bitmap = nil // free bitmap memory

		pb := preparedBitmap{fingerprint: entry.fingerprint}
		if len(raw) >= 256 {
			compressed, encErr := enc.Encode(raw)
			if encErr != nil {
				return encErr
			}
			if len(compressed) < len(raw) {
				pb.flags = flagCompressed
				pb.data = make([]byte, len(compressed))
				copy(pb.data, compressed)
				prepared[i] = pb
				return nil
			}
		}
		pb.data = raw
		prepared[i] = pb
		return nil
	}

	numWorkers := min(w.concurrency, totalKeys)

	if numWorkers <= 1 {
		// Serial path.
		enc := zstd.NewCompressor(zstd.WithoutChecksum())
		defer enc.Close()
		for i := range totalKeys {
			if err := prepareBitmap(i, enc); err != nil {
				return fmt.Errorf("bitmapindex: record preparation: %w", err)
			}
		}
	} else {
		// Parallel path.
		ch := make(chan int, totalKeys)
		for i := range totalKeys {
			ch <- i
		}
		close(ch)

		prepCtx, prepCancel := context.WithCancel(ctx)
		defer prepCancel()

		var wg sync.WaitGroup
		var errOnce sync.Once
		var firstErr error
		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				enc := zstd.NewCompressor(zstd.WithoutChecksum())
				defer enc.Close()

				for i := range ch {
					if prepCtx.Err() != nil {
						return
					}
					if err := prepareBitmap(i, enc); err != nil {
						errOnce.Do(func() { firstErr = err })
						prepCancel()
						return
					}
				}
			}()
		}
		wg.Wait()

		if firstErr != nil {
			return fmt.Errorf("bitmapindex: record preparation: %w", firstErr)
		}
	}

	// Group into batch records and write to packfile.
	batchSize := w.batchSize
	numBatches := (totalKeys + batchSize - 1) / batchSize

	pw, err := packfile.Create(dataPath, packfile.WriterOptions{})
	if err != nil {
		return fmt.Errorf("bitmapindex: create packfile: %w", err)
	}

	// Encode metadata: [totalKeys:u32][batchSize:u16][flags:u16]
	var meta [metadataSize]byte
	binary.LittleEndian.PutUint32(meta[0:4], uint32(totalKeys))
	binary.LittleEndian.PutUint16(meta[4:6], uint16(batchSize))
	binary.LittleEndian.PutUint16(meta[6:8], 0) // flags: reserved
	pw.SetMetadata(meta[:])

	var batchBuf []byte
	for b := range numBatches {
		start := b * batchSize
		end := min(start+batchSize, totalKeys)
		count := end - start

		// Estimate capacity: header + data.
		headerSize := 2 + count*fingerprintSize + count + count*4
		dataSize := 0
		for i := start; i < end; i++ {
			dataSize += len(prepared[i].data)
		}
		batchBuf = batchBuf[:0]
		if cap(batchBuf) < headerSize+dataSize+checksumSize {
			batchBuf = make([]byte, 0, headerSize+dataSize+checksumSize)
		}

		// Count.
		batchBuf = binary.LittleEndian.AppendUint16(batchBuf, uint16(count))

		// Fingerprints.
		for i := start; i < end; i++ {
			batchBuf = append(batchBuf, prepared[i].fingerprint[:]...)
		}

		// Flags.
		for i := start; i < end; i++ {
			batchBuf = append(batchBuf, prepared[i].flags)
		}

		// Sizes.
		for i := start; i < end; i++ {
			batchBuf = binary.LittleEndian.AppendUint32(batchBuf, uint32(len(prepared[i].data)))
		}

		// Data.
		for i := start; i < end; i++ {
			batchBuf = append(batchBuf, prepared[i].data...)
		}

		// CRC32C over the entire batch (excluding CRC itself).
		crc := crc32.Checksum(batchBuf, crc32cTable)
		batchBuf = binary.LittleEndian.AppendUint32(batchBuf, crc)

		if err := pw.Append(batchBuf); err != nil {
			return errors.Join(
				fmt.Errorf("bitmapindex: append batch %d: %w", b, err),
				pw.Abort(),
			)
		}

		// Free prepared data for this batch.
		for i := start; i < end; i++ {
			prepared[i].data = nil
		}
	}

	if _, err := pw.Finish(); err != nil {
		return fmt.Errorf("bitmapindex: finish packfile: %w", err)
	}

	return nil
}
