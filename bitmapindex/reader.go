package bitmapindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"
	streamerrors "github.com/tamirms/streamhash/errors"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

var ErrKeyNotFound = errors.New("bitmapindex: key not found")

const (
	defaultConcurrency        = 8
	knownFlags         uint16 = 0
)

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for LookupKeys.
// Default is 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Reader provides point lookups from an MPHF+packfile bitmap index.
// Thread-safe for concurrent Lookup and LookupKeys calls after Open.
type Reader struct {
	mphf        *streamhash.Index
	pack        *packfile.Reader
	batchSize   int // from metadata
	concurrency int // for LookupKeys worker pool
}

// Open opens a two-file bitmap index for querying.
// mphfPath is the streamhash MPHF file, dataPath is the packfile
// with batch bitmap records. Both files are opened in parallel
// so the wall-clock cost is a single IOP even on cold cache.
func Open(mphfPath, dataPath string, opts ...ReaderOption) (_ *Reader, err error) {
	type mphfResult struct {
		idx *streamhash.Index
		err error
	}
	mphfCh := make(chan mphfResult, 1)
	go func() {
		f, err := os.Open(mphfPath)
		if err != nil {
			mphfCh <- mphfResult{err: fmt.Errorf("bitmapindex: open MPHF: %w", err)}
			return
		}
		data, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			mphfCh <- mphfResult{err: fmt.Errorf("bitmapindex: read MPHF: %w", err)}
			return
		}
		idx, err := streamhash.OpenBytes(data)
		if err != nil {
			mphfCh <- mphfResult{err: fmt.Errorf("bitmapindex: parse MPHF: %w", err)}
			return
		}
		mphfCh <- mphfResult{idx: idx}
	}()

	pack, packErr := packfile.Open(dataPath)
	res := <-mphfCh

	// Handle open errors.
	if packErr != nil {
		if res.idx != nil {
			res.idx.Close()
		}
		return nil, fmt.Errorf("bitmapindex: open packfile: %w", packErr)
	}
	if res.err != nil {
		pack.Close()
		return nil, res.err
	}

	// Both resources open. Defer cleanup for any subsequent validation error.
	defer func() {
		if err != nil {
			res.idx.Close()
			pack.Close()
		}
	}()

	// Parse and validate metadata.
	meta := pack.Metadata()
	if len(meta) < metadataSize {
		return nil, fmt.Errorf("bitmapindex: metadata too short: %d bytes (want >= %d)", len(meta), metadataSize)
	}

	totalKeys := int(binary.LittleEndian.Uint32(meta[0:4]))
	batchSize := int(binary.LittleEndian.Uint16(meta[4:6]))
	flags := binary.LittleEndian.Uint16(meta[6:8])

	if flags&^knownFlags != 0 {
		return nil, fmt.Errorf("bitmapindex: unknown metadata flags: %04x", flags)
	}
	if totalKeys == 0 {
		return nil, fmt.Errorf("bitmapindex: invalid totalKeys %d in metadata", totalKeys)
	}
	if batchSize == 0 {
		return nil, fmt.Errorf("bitmapindex: invalid batchSize %d in metadata", batchSize)
	}

	expectedBatches := (totalKeys + batchSize - 1) / batchSize
	if expectedBatches != pack.RecordCount() {
		return nil, fmt.Errorf("bitmapindex: metadata says %d keys / %d batchSize = %d batches, but packfile has %d records",
			totalKeys, batchSize, expectedBatches, pack.RecordCount())
	}

	r := &Reader{
		mphf:        res.idx,
		pack:        pack,
		batchSize:   batchSize,
		concurrency: defaultConcurrency,
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}
	return r, nil
}

// Lookup returns the roaring bitmap for the given field/key, or ErrKeyNotFound
// if the key is not in the index (fingerprint mismatch).
func (r *Reader) Lookup(f Field, key []byte) (*roaring.Bitmap, error) {
	var buf [256]byte
	composed := composeKey(buf[:0], f, key)

	var hk [16]byte
	streamhash.PreHashInPlace(composed, hk[:])

	rank, err := r.mphf.Query(hk[:])
	if err != nil {
		if errors.Is(err, streamerrors.ErrNotFound) || errors.Is(err, streamerrors.ErrFingerprintMismatch) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("bitmapindex: MPHF query: %w", err)
	}

	batchIdx := int(rank) / r.batchSize
	localIdx := int(rank) % r.batchSize

	lb := getBuf()
	defer putBuf(lb)

	lb.rec, err = r.pack.ReadRecordInto(batchIdx, lb.rec)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: read batch %d for rank %d: %w", batchIdx, rank, err)
	}

	batch, err := parseBatch(lb.rec, batchIdx)
	if err != nil {
		return nil, err
	}

	return extractBitmap(batch, localIdx, hk[:], lb)
}

// LookupKeys returns bitmaps for multiple keys with parallel I/O.
// The returned slice is parallel to keys: nil entries indicate not-found keys.
// Duplicate keys produce independent identical bitmaps.
func (r *Reader) LookupKeys(ctx context.Context, keys []FieldKey) ([]*roaring.Bitmap, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	out := make([]*roaring.Bitmap, len(keys))

	// MPHF query all keys and group by batch index.
	type foundKey struct {
		outIdx   int
		hk       [16]byte
		batchIdx int
		localIdx int
	}
	var found []foundKey

	var buf [256]byte
	for i, key := range keys {
		composed := composeKey(buf[:0], key.Field, key.Key)

		var hk [16]byte
		streamhash.PreHashInPlace(composed, hk[:])

		rank, err := r.mphf.Query(hk[:])
		if err != nil {
			if errors.Is(err, streamerrors.ErrNotFound) || errors.Is(err, streamerrors.ErrFingerprintMismatch) {
				continue
			}
			return nil, fmt.Errorf("bitmapindex: MPHF query key %d: %w", i, err)
		}

		found = append(found, foundKey{
			outIdx:   i,
			hk:       hk,
			batchIdx: int(rank) / r.batchSize,
			localIdx: int(rank) % r.batchSize,
		})
	}

	if len(found) == 0 {
		return out, nil
	}

	// Sort by batch index so keys hitting the same batch are adjacent.
	sort.Slice(found, func(i, j int) bool {
		return found[i].batchIdx < found[j].batchIdx
	})

	// Group into per-batch work items.
	type batchWork struct {
		batchIdx int
		keys     []foundKey
	}
	var work []batchWork
	prevBatch := -1
	for i := range found {
		if found[i].batchIdx != prevBatch {
			work = append(work, batchWork{batchIdx: found[i].batchIdx})
			prevBatch = found[i].batchIdx
		}
		w := &work[len(work)-1]
		w.keys = append(w.keys, found[i])
	}

	numBatches := len(work)

	// Parallel I/O: fixed worker pool with atomic work stealing.
	numWorkers := min(numBatches, r.concurrency)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var nextBatch atomic.Int64
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	wg.Add(numWorkers)
	for range numWorkers {
		go func() {
			defer wg.Done()
			lb := getBuf()
			defer putBuf(lb)
			defer func() {
				if rv := recover(); rv != nil {
					errOnce.Do(func() { firstErr = fmt.Errorf("bitmapindex: panic in LookupKeys worker: %v", rv) })
					cancel()
				}
			}()

			for {
				bi := int(nextBatch.Add(1)) - 1
				if bi >= numBatches || ctx.Err() != nil {
					return
				}

				bw := &work[bi]
				var err error
				lb.rec, err = r.pack.ReadRecordInto(bw.batchIdx, lb.rec)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("bitmapindex: read batch %d: %w", bw.batchIdx, err)
					})
					cancel()
					return
				}

				batch, err := parseBatch(lb.rec, bw.batchIdx)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					cancel()
					return
				}

				for _, ki := range bw.keys {
					bm, err := extractBitmap(batch, ki.localIdx, ki.hk[:], lb)
					if err != nil {
						if errors.Is(err, ErrKeyNotFound) {
							continue // fingerprint false positive
						}
						errOnce.Do(func() { firstErr = err })
						cancel()
						return
					}
					out[ki.outIdx] = bm
				}
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// Close releases all resources.
func (r *Reader) Close() error {
	mErr := r.mphf.Close()
	pErr := r.pack.Close()
	return errors.Join(mErr, pErr)
}

// --- Internal helpers ---

// lookupBuf holds reusable buffers for batch reads and decompression.
type lookupBuf struct {
	rec       []byte
	decompBuf []byte
	dec       *zstd.Decompressor
}

var bufPool = sync.Pool{
	New: func() any {
		dec := zstd.NewDecompressor()
		lb := &lookupBuf{
			rec: make([]byte, 0, 32*1024),
			dec: dec,
		}
		runtime.SetFinalizer(lb, func(b *lookupBuf) { b.dec.Close() })
		return lb
	},
}

func getBuf() *lookupBuf {
	return bufPool.Get().(*lookupBuf)
}

func putBuf(b *lookupBuf) {
	b.rec = b.rec[:0]
	b.decompBuf = b.decompBuf[:0]
	bufPool.Put(b)
}

// batchRecord holds parsed offsets into a CRC-verified batch record.
type batchRecord struct {
	count     int
	rec       []byte // CRC-verified, CRC stripped
	fpBase    int
	flagsBase int
	sizesBase int
	dataBase  int
}

// parseBatch verifies the CRC32C of rec and parses the batch header.
func parseBatch(rec []byte, batchIdx int) (batchRecord, error) {
	if len(rec) < 2+fingerprintSize+1+4+checksumSize {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch record too short at batch %d: %d bytes", batchIdx, len(rec))
	}

	storedCRC := binary.LittleEndian.Uint32(rec[len(rec)-checksumSize:])
	computedCRC := crc32.Checksum(rec[:len(rec)-checksumSize], crc32cTable)
	if storedCRC != computedCRC {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch CRC32C mismatch at batch %d: stored=%08x computed=%08x", batchIdx, storedCRC, computedCRC)
	}
	rec = rec[:len(rec)-checksumSize]

	count := int(binary.LittleEndian.Uint16(rec[:2]))
	headerSize := 2 + count*fingerprintSize + count + count*4
	if len(rec) < headerSize {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch header truncated at batch %d", batchIdx)
	}

	return batchRecord{
		count:     count,
		rec:       rec,
		fpBase:    2,
		flagsBase: 2 + count*fingerprintSize,
		sizesBase: 2 + count*fingerprintSize + count,
		dataBase:  headerSize,
	}, nil
}

// extractBitmap extracts and deserializes the bitmap at localIdx within batch.
// hk is the 16-byte pre-hash; the first 4 bytes are the fingerprint.
func extractBitmap(batch batchRecord, localIdx int, hk []byte, lb *lookupBuf) (*roaring.Bitmap, error) {
	if localIdx >= batch.count {
		return nil, fmt.Errorf("bitmapindex: local index %d >= batch count %d", localIdx, batch.count)
	}

	// Verify fingerprint.
	fpOff := batch.fpBase + localIdx*fingerprintSize
	if batch.rec[fpOff] != hk[0] || batch.rec[fpOff+1] != hk[1] ||
		batch.rec[fpOff+2] != hk[2] || batch.rec[fpOff+3] != hk[3] {
		return nil, ErrKeyNotFound
	}

	flags := batch.rec[batch.flagsBase+localIdx]
	dataSize := int(binary.LittleEndian.Uint32(batch.rec[batch.sizesBase+localIdx*4:]))

	// Compute data offset by summing sizes of preceding bitmaps.
	dataOff := batch.dataBase
	for i := range localIdx {
		dataOff += int(binary.LittleEndian.Uint32(batch.rec[batch.sizesBase+i*4:]))
	}

	if dataOff+dataSize > len(batch.rec) {
		return nil, fmt.Errorf("bitmapindex: bitmap data overflow at local %d in batch", localIdx)
	}

	data := batch.rec[dataOff : dataOff+dataSize]

	if flags&flagCompressed != 0 {
		decoded, err := lb.dec.Decode(lb.decompBuf[:0], data)
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: decompress bitmap at local %d: %w", localIdx, err)
		}
		lb.decompBuf = decoded
		data = decoded
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap at local %d: %w", localIdx, err)
	}

	return bm, nil
}
