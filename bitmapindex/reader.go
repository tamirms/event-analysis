package bitmapindex

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for LookupKeys. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Reader provides point lookups from an MPHF+packfile bitmap index.
// Thread-safe for concurrent Lookup and LookupKeys calls after Open.
type Reader struct {
	mphf        *streamhash.Index
	pack        *packfile.Reader
	batchSize   int // from metadata
	totalKeys   int // from metadata
	concurrency int // for LookupKeys worker pool
}

// Open opens a two-file bitmap index for querying.
// mphfPath is the streamhash MPHF file, dataPath is the packfile
// with batch bitmap records. Both files are opened in parallel
// so the wall-clock cost is a single IOP even on cold cache.
func Open(mphfPath, dataPath string, opts ...ReaderOption) (*Reader, error) {
	// Open both files in parallel.
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

	pack, err := packfile.Open(dataPath)
	if err != nil {
		// Wait for MPHF goroutine to finish and clean up.
		res := <-mphfCh
		if res.idx != nil {
			res.idx.Close()
		}
		return nil, fmt.Errorf("bitmapindex: open packfile: %w", err)
	}

	res := <-mphfCh
	if res.err != nil {
		pack.Close()
		return nil, res.err
	}

	// Parse and validate metadata.
	meta := pack.Metadata()
	if len(meta) < metadataSize {
		res.idx.Close()
		pack.Close()
		return nil, fmt.Errorf("bitmapindex: metadata too short: %d bytes (want >= %d)", len(meta), metadataSize)
	}

	totalKeys := int(binary.LittleEndian.Uint32(meta[0:4]))
	batchSize := int(binary.LittleEndian.Uint16(meta[4:6]))
	flags := binary.LittleEndian.Uint16(meta[6:8])

	if flags&^knownFlags != 0 {
		res.idx.Close()
		pack.Close()
		return nil, fmt.Errorf("bitmapindex: unknown metadata flags: %04x", flags)
	}
	if totalKeys <= 0 {
		res.idx.Close()
		pack.Close()
		return nil, fmt.Errorf("bitmapindex: invalid totalKeys %d in metadata", totalKeys)
	}
	if batchSize <= 0 {
		res.idx.Close()
		pack.Close()
		return nil, fmt.Errorf("bitmapindex: invalid batchSize %d in metadata", batchSize)
	}

	expectedBatches := (totalKeys + batchSize - 1) / batchSize
	if expectedBatches != pack.RecordCount() {
		res.idx.Close()
		pack.Close()
		return nil, fmt.Errorf("bitmapindex: metadata says %d keys / %d batchSize = %d batches, but packfile has %d records",
			totalKeys, batchSize, expectedBatches, pack.RecordCount())
	}

	r := &Reader{
		mphf:        res.idx,
		pack:        pack,
		batchSize:   batchSize,
		totalKeys:   totalKeys,
		concurrency: 8,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r, nil
}

// bufPool reuses pread + decompression buffers across lookups.
var bufPool = sync.Pool{
	New: func() any {
		dec := zstd.NewDecompressor()
		lb := &lookupBuf{
			rec: make([]byte, 0, 32*1024), // 32KB for batch records
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

// Lookup returns the roaring bitmap for the given key, or ErrKeyNotFound
// if the key is not in the index (fingerprint mismatch).
// The key should be pre-composed via ComposeKey if field disambiguation is needed.
func (r *Reader) Lookup(key []byte) (*roaring.Bitmap, error) {
	// Hash key for MPHF query.
	var hk [16]byte
	streamhash.PreHashInPlace(key, hk[:])

	// Query MPHF for rank.
	rank, err := r.mphf.Query(hk[:])
	if err != nil {
		if errors.Is(err, streamerrors.ErrNotFound) || errors.Is(err, streamerrors.ErrFingerprintMismatch) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("bitmapindex: MPHF query: %w", err)
	}

	// Compute batch index and position within batch.
	batchIdx := int(rank) / r.batchSize
	localIdx := int(rank) % r.batchSize

	// Read batch record from packfile.
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

// keyInfo holds MPHF query results for a single key in LookupKeys.
type keyInfo struct {
	outIdx   int      // index in the output slice
	hk       [16]byte // pre-hash for fingerprint check
	localIdx int      // position within batch
}

// batchWork groups keys that map to the same batch.
type batchWork struct {
	batchIdx int
	keys     []keyInfo
}

// LookupKeys returns bitmaps for multiple keys with parallel I/O.
// The returned slice is parallel to keys: nil entries indicate not-found keys.
// Duplicate keys produce independent identical bitmaps.
func (r *Reader) LookupKeys(ctx context.Context, keys [][]byte) ([]*roaring.Bitmap, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	out := make([]*roaring.Bitmap, len(keys))

	// Phase 1 (CPU): MPHF query all keys.
	type foundKey struct {
		outIdx   int
		hk       [16]byte
		batchIdx int
		localIdx int
	}
	var found []foundKey

	for i, key := range keys {
		var hk [16]byte
		streamhash.PreHashInPlace(key, hk[:])

		rank, err := r.mphf.Query(hk[:])
		if err != nil {
			if errors.Is(err, streamerrors.ErrNotFound) || errors.Is(err, streamerrors.ErrFingerprintMismatch) {
				continue // not found → leave out[i] nil
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

	// Phase 2 (CPU): Group found keys by batchIdx.
	sort.Slice(found, func(i, j int) bool {
		return found[i].batchIdx < found[j].batchIdx
	})

	var work []batchWork
	prevBatch := -1
	for _, fk := range found {
		if fk.batchIdx != prevBatch {
			work = append(work, batchWork{batchIdx: fk.batchIdx})
			prevBatch = fk.batchIdx
		}
		w := &work[len(work)-1]
		w.keys = append(w.keys, keyInfo{
			outIdx:   fk.outIdx,
			hk:       fk.hk,
			localIdx: fk.localIdx,
		})
	}

	numBatches := len(work)

	// Phase 3 (I/O): Fixed worker pool with atomic work stealing.
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
							continue // fingerprint false positive → leave nil
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
