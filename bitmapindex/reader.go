package bitmapindex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"
	streamerrors "github.com/tamirms/streamhash/errors"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/record"
)

var ErrKeyNotFound = errors.New("bitmapindex: key not found")

const defaultConcurrency = 8

// ReaderOption configures Reader behavior.
type ReaderOption func(*readerConfig)

type readerConfig struct {
	expectedLookups int // drives MPHF prefetch decision; 0 → always mmap
	concurrency     int // max goroutines for LookupKeys
}

// WithExpectedLookups hints how many lookups will be performed.
// When the expected lookups exceed the number of 256KB I/Os needed to
// read the full MPHF, Open reads it entirely into memory (one sequential
// read). Otherwise it mmaps the file for on-demand page faults.
// Default (0) always mmaps.
func WithExpectedLookups(n int) ReaderOption {
	return func(cfg *readerConfig) { cfg.expectedLookups = n }
}

// WithConcurrency sets the max parallel goroutines for LookupKeys.
// Values less than 1 are clamped to 1. Default is 8.
func WithConcurrency(n int) ReaderOption {
	return func(cfg *readerConfig) { cfg.concurrency = n }
}

type mphfResult struct {
	idx *streamhash.Index
	err error
}

// Reader provides point lookups from an MPHF+packfile bitmap index.
// Thread-safe for concurrent Lookup and LookupKeys calls after Open.
//
// Open returns immediately; MPHF loading and packfile opening happen
// in background goroutines. Each dependency is waited on at the exact
// point it's needed in the query path.
type Reader struct {
	pack        *packfile.Reader // from packfile.Open (non-blocking)
	concurrency int

	// MPHF — loaded async:
	mphfCh    <-chan mphfResult
	mphfOnce  sync.Once
	mphfIndex *streamhash.Index
	mphfErr   error

	// Packfile metadata — validated on first access:
	packOnce       sync.Once
	totalKeys      int
	batchSize      int
	compressed     bool
	contentHash    [32]byte
	hasContentHash bool
	packErr        error

	// Idempotent close:
	closeOnce sync.Once
	closeErr  error
}

func (r *Reader) waitMPHF() (*streamhash.Index, error) {
	r.mphfOnce.Do(func() {
		res := <-r.mphfCh
		r.mphfIndex = res.idx
		r.mphfErr = res.err
	})
	return r.mphfIndex, r.mphfErr
}

func (r *Reader) waitPackfile() error {
	r.packOnce.Do(func() {
		meta, err := r.pack.Metadata() // blocks on packfile.waitOpen
		if err != nil {
			r.packErr = err
			return
		}

		totalItems, recordSize, flags, err := record.DecodeMetadata(meta)
		if err != nil {
			r.packErr = fmt.Errorf("bitmapindex: %w", err)
			return
		}

		if flags&^record.KnownFlags != 0 {
			r.packErr = fmt.Errorf("bitmapindex: unknown metadata flags: %08x", flags)
			return
		}

		r.totalKeys = totalItems
		r.batchSize = recordSize
		r.compressed = flags&record.FlagNoCompression == 0
		r.contentHash, r.hasContentHash = record.ContentHash(meta, flags)

		if r.totalKeys == 0 {
			r.packErr = fmt.Errorf("bitmapindex: invalid totalKeys %d in metadata", r.totalKeys)
			return
		}
		if r.batchSize == 0 {
			r.packErr = fmt.Errorf("bitmapindex: invalid batchSize %d in metadata", r.batchSize)
			return
		}

		rc, err := r.pack.RecordCount()
		if err != nil {
			r.packErr = err
			return
		}
		expectedBatches := (r.totalKeys + r.batchSize - 1) / r.batchSize
		if expectedBatches != rc {
			r.packErr = fmt.Errorf("bitmapindex: metadata says %d keys / %d batchSize = %d batches, but packfile has %d records",
				r.totalKeys, r.batchSize, expectedBatches, rc)
		}
	})
	return r.packErr
}

// Open opens a two-file bitmap index for querying.
// Returns immediately; MPHF loading and packfile opening happen in
// background goroutines. Close must always be called.
func Open(mphfPath, dataPath string, opts ...ReaderOption) *Reader {
	cfg := readerConfig{concurrency: defaultConcurrency}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}

	mphfCh := make(chan mphfResult, 1)
	go func() {
		// Single send point: loadMPHF returns the result, panic recovery
		// overrides it. Exactly one value is sent regardless of outcome.
		var result mphfResult
		defer func() {
			if rv := recover(); rv != nil {
				if result.idx != nil {
					result.idx.Close()
				}
				result = mphfResult{err: fmt.Errorf("bitmapindex: panic in MPHF load: %v", rv)}
			}
			mphfCh <- result
		}()
		result = loadMPHF(mphfPath, cfg.expectedLookups)
	}()

	return &Reader{
		pack:        packfile.Open(dataPath), // non-blocking
		concurrency: cfg.concurrency,
		mphfCh:      mphfCh,
	}
}

// loadMPHF performs all synchronous I/O for loading the MPHF index.
// The file is closed via defer on all paths (including panics).
func loadMPHF(path string, expectedLookups int) mphfResult {
	f, err := os.Open(path)
	if err != nil {
		return mphfResult{err: fmt.Errorf("bitmapindex: open MPHF: %w", err)}
	}
	defer f.Close()

	// Decide whether to prefetch (sequential read into memory) or mmap
	// (on-demand page faults). When expected lookups exceed the number of
	// 256KB I/Os needed to read the full file, prefetching amortises to
	// less than one random IOP per lookup and is strictly better.
	stat, err := f.Stat()
	if err != nil {
		return mphfResult{err: fmt.Errorf("bitmapindex: stat MPHF: %w", err)}
	}
	const ebsIOPSize = 256 * 1024
	iopsToReadAll := (stat.Size() + ebsIOPSize - 1) / ebsIOPSize // ceil(fileSize / ebsIOPSize)
	prefetch := int64(expectedLookups)+1 > iopsToReadAll

	var idx *streamhash.Index
	if prefetch {
		data, err := io.ReadAll(f)
		if err != nil {
			return mphfResult{err: fmt.Errorf("bitmapindex: read MPHF: %w", err)}
		}
		idx, err = streamhash.OpenBytes(data)
		if err != nil {
			return mphfResult{err: fmt.Errorf("bitmapindex: parse MPHF: %w", err)}
		}
	} else {
		idx, err = streamhash.OpenFile(f)
		if err != nil {
			return mphfResult{err: fmt.Errorf("bitmapindex: mmap MPHF: %w", err)}
		}
	}
	return mphfResult{idx: idx}
}

// Lookup returns the roaring bitmap for the given field/key, or ErrKeyNotFound
// if the key is not in the index (fingerprint mismatch).
func (r *Reader) Lookup(f Field, key []byte) (*roaring.Bitmap, error) {
	mphf, err := r.waitMPHF() // wait for MPHF only
	if err != nil {
		return nil, err
	}

	var buf [256]byte
	composed := composeKey(buf[:0], f, key)

	var hk [16]byte
	streamhash.PreHashInPlace(composed, hk[:])

	rank, err := mphf.Query(hk[:])
	if err != nil {
		if errors.Is(err, streamerrors.ErrNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("bitmapindex: MPHF query: %w", err)
	}

	if err := r.waitPackfile(); err != nil { // NOW wait for packfile
		return nil, err
	}

	batchIdx := int(rank) / r.batchSize
	localIdx := int(rank) % r.batchSize

	rd := record.Get()
	defer record.Put(rd)

	n := record.ItemsInRecord(r.totalKeys, r.batchSize, batchIdx)
	if err := rd.ReadAndDecode(r.pack, batchIdx, n, r.compressed); err != nil {
		return nil, fmt.Errorf("bitmapindex: decode batch %d for rank %d: %w", batchIdx, rank, err)
	}

	return lookupEntry(rd, localIdx, hk[:])
}

// LookupKeys returns bitmaps for multiple keys with parallel I/O.
// The returned slice is parallel to keys: nil entries indicate not-found keys.
// Duplicate keys produce independent identical bitmaps.
func (r *Reader) LookupKeys(ctx context.Context, keys []FieldKey) ([]*roaring.Bitmap, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	mphf, err := r.waitMPHF() // wait for MPHF only
	if err != nil {
		return nil, err
	}

	out := make([]*roaring.Bitmap, len(keys))

	// MPHF query all keys — packfile may still be loading during this phase.
	type foundKey struct {
		outIdx   int
		hk       [16]byte
		rank     uint64
		batchIdx int
		localIdx int
	}
	found := make([]foundKey, 0, len(keys))

	var buf [256]byte
	for i, key := range keys {
		composed := composeKey(buf[:0], key.Field, key.Key)

		var hk [16]byte
		streamhash.PreHashInPlace(composed, hk[:])

		rank, err := mphf.Query(hk[:])
		if err != nil {
			if errors.Is(err, streamerrors.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("bitmapindex: MPHF query key %d: %w", i, err)
		}

		found = append(found, foundKey{
			outIdx: i,
			hk:     hk,
			rank:   rank,
		})
	}

	if len(found) == 0 {
		return out, nil
	}

	if err := r.waitPackfile(); err != nil { // NOW wait for packfile
		return nil, err
	}

	// Decompose ranks using batchSize now that packfile metadata is ready.
	for i := range found {
		found[i].batchIdx = int(found[i].rank) / r.batchSize
		found[i].localIdx = int(found[i].rank) % r.batchSize
	}

	// Sort by batch index so keys hitting the same batch are adjacent.
	slices.SortFunc(found, func(a, b foundKey) int {
		return a.batchIdx - b.batchIdx
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

	// Extract sorted batch indices for ReadScattered.
	batchIndices := make([]int, len(work))
	for i := range work {
		batchIndices[i] = work[i].batchIdx
	}

	err = r.pack.ReadScattered(ctx, batchIndices, r.concurrency,
		func(inputPos int, data []byte) error {
			rd := record.Get()
			defer record.Put(rd)
			bw := &work[inputPos]

			n := record.ItemsInRecord(r.totalKeys, r.batchSize, bw.batchIdx)
			if err := rd.Decode(data, n, r.compressed); err != nil {
				return fmt.Errorf("bitmapindex: decode batch %d: %w", bw.batchIdx, err)
			}
			for _, ki := range bw.keys {
				bm, err := lookupEntry(rd, ki.localIdx, ki.hk[:])
				if err != nil {
					if errors.Is(err, ErrKeyNotFound) {
						continue
					}
					return err
				}
				out[ki.outIdx] = bm
			}
			return nil
		})

	if err != nil {
		return nil, err
	}

	return out, nil
}

// ContentHash returns the SHA-256 content hash stored in metadata, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	if err := r.waitPackfile(); err != nil {
		return [32]byte{}, false, err
	}
	return r.contentHash, r.hasContentHash, nil
}

// Verify recomputes the SHA-256 content hash by reading all records and
// comparing to the stored hash. Returns nil if no hash is stored or if the
// hash matches. Only depends on waitPackfile(), not waitMPHF().
func (r *Reader) Verify(ctx context.Context) error {
	if err := r.waitPackfile(); err != nil {
		return err
	}
	if !r.hasContentHash {
		return nil
	}

	hasher := record.NewContentHasher(r.batchSize)
	numBatches := (r.totalKeys + r.batchSize - 1) / r.batchSize

	rd := record.Get()
	defer record.Put(rd)

	batchIdx := 0
	for recData, rangeErr := range r.pack.ReadRecords(0, numBatches) {
		if rangeErr != nil {
			return rangeErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		n := record.ItemsInRecord(r.totalKeys, r.batchSize, batchIdx)
		if err := rd.Decode(recData, n, r.compressed); err != nil {
			return err
		}

		for i := range n {
			hasher.Add(rd.Entry(i))
		}
		batchIdx++
	}

	computed := hasher.Sum()
	if computed != r.contentHash {
		return fmt.Errorf("bitmapindex: %w: expected %x, got %x",
			record.ErrContentHashMismatch, r.contentHash, computed)
	}
	return nil
}

// Close releases all resources. Safe to call multiple times.
// Must always be called, even if no query methods were called.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.mphfOnce.Do(func() {
			res := <-r.mphfCh
			r.mphfIndex = res.idx
			r.mphfErr = res.err
		})
		if r.mphfIndex != nil {
			r.closeErr = r.mphfIndex.Close()
		}
		r.closeErr = errors.Join(r.closeErr, r.pack.Close())
	})
	return r.closeErr
}

// --- Internal helpers ---

// lookupEntry extracts and deserializes the bitmap at localIdx within
// a decoded batch. hk is the 16-byte pre-hash; the first 4 bytes are
// the fingerprint. Returns ErrKeyNotFound on fingerprint mismatch.
func lookupEntry(rd *record.Decoder, localIdx int, hk []byte) (*roaring.Bitmap, error) {
	entry := rd.Entry(localIdx)

	// Verify fingerprint (first 4 bytes of entry).
	if len(entry) < fingerprintSize ||
		entry[0] != hk[0] || entry[1] != hk[1] ||
		entry[2] != hk[2] || entry[3] != hk[3] {
		return nil, ErrKeyNotFound
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(entry[fingerprintSize:]); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap at local %d: %w", localIdx, err)
	}
	return bm, nil
}
