package bitmapindex

import (
	"cmp"
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
)

var ErrKeyNotFound = errors.New("bitmapindex: key not found")

// ReaderOption configures Reader behavior.
type ReaderOption func(*readerConfig)

type readerConfig struct {
	expectedLookups int // drives MPHF prefetch decision; 0 → always mmap
	concurrency     int // max goroutines for LookupKeys; 0 → packfile default
}

// WithExpectedLookups hints how many lookups will be performed.
func WithExpectedLookups(n int) ReaderOption {
	return func(cfg *readerConfig) { cfg.expectedLookups = n }
}

// WithConcurrency sets the max parallel goroutines for LookupKeys.
// Values less than 1 are clamped to 1. Default is 8.
func WithConcurrency(n int) ReaderOption {
	return func(cfg *readerConfig) { cfg.concurrency = n }
}

// Reader provides point lookups from an MPHF+packfile bitmap index.
// Thread-safe for concurrent Lookup and LookupKeys calls after Open.
type Reader struct {
	pr       *packfile.Reader
	waitMPHF func() (*streamhash.Index, error) // blocks until MPHF load completes

	// Idempotent close:
	closeOnce sync.Once
	closeErr  error
}

// Open opens a two-file bitmap index for querying.
// Returns immediately; MPHF loading and packfile opening happen in
// background goroutines. Close must always be called.
func Open(mphfPath, dataPath string, opts ...ReaderOption) *Reader {
	var cfg readerConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var prOpts []packfile.ReaderOption
	if cfg.concurrency != 0 {
		prOpts = append(prOpts, packfile.WithConcurrency(cfg.concurrency))
	}

	type loadResult struct {
		idx *streamhash.Index
		err error
	}
	ch := make(chan loadResult, 1)
	go func() {
		var res loadResult
		defer func() {
			if rv := recover(); rv != nil {
				if res.idx != nil {
					res.idx.Close()
				}
				res = loadResult{err: fmt.Errorf("bitmapindex: panic in MPHF load: %v", rv)}
			}
			ch <- res
		}()
		res.idx, res.err = loadMPHF(mphfPath, cfg.expectedLookups)
	}()

	return &Reader{
		pr: packfile.Open(dataPath, prOpts...),
		waitMPHF: sync.OnceValues(func() (*streamhash.Index, error) {
			res := <-ch
			return res.idx, res.err
		}),
	}
}

// loadMPHF performs all synchronous I/O for loading the MPHF index.
func loadMPHF(path string, expectedLookups int) (*streamhash.Index, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: open MPHF: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: stat MPHF: %w", err)
	}
	const ebsIOPSize = 256 * 1024
	iopsToReadAll := (stat.Size() + ebsIOPSize - 1) / ebsIOPSize
	prefetch := int64(expectedLookups)+1 > iopsToReadAll

	if prefetch {
		data, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: read MPHF: %w", err)
		}
		idx, err := streamhash.OpenBytes(data)
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: parse MPHF: %w", err)
		}
		return idx, nil
	}
	idx, err := streamhash.OpenFile(f)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: mmap MPHF: %w", err)
	}
	return idx, nil
}

// Lookup returns the roaring bitmap for the given field/key, or ErrKeyNotFound.
func (r *Reader) Lookup(f Field, key []byte) (*roaring.Bitmap, error) {
	mphf, err := r.waitMPHF()
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

	var bm *roaring.Bitmap
	if err := r.pr.ReadItem(int(rank), func(entry []byte) error {
		var err error
		bm, err = verifyAndDeserialize(entry, hk[:])
		return err
	}); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("bitmapindex: read rank %d: %w", rank, err)
	}
	return bm, nil
}

// LookupKeys returns bitmaps for multiple keys with parallel I/O.
func (r *Reader) LookupKeys(ctx context.Context, keys []FieldKey) ([]*roaring.Bitmap, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	mphf, err := r.waitMPHF()
	if err != nil {
		return nil, err
	}

	out := make([]*roaring.Bitmap, len(keys))

	type foundKey struct {
		outIdx int
		hk     [16]byte
		rank   uint64
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

		found = append(found, foundKey{outIdx: i, hk: hk, rank: rank})
	}

	if len(found) == 0 {
		return out, nil
	}

	// Sort by rank to get sorted item indices for ReadItems.
	slices.SortFunc(found, func(a, b foundKey) int {
		return cmp.Compare(a.rank, b.rank)
	})

	// Deduplicate ranks for ReadItems (found is sorted by rank).
	indices := make([]int, 0, len(found))
	// groupStart[g] = first index in found[] for the g-th unique rank.
	groupStart := make([]int, 0, len(found))
	for i := range found {
		if i == 0 || found[i].rank != found[i-1].rank {
			groupStart = append(groupStart, i)
			indices = append(indices, int(found[i].rank))
		}
	}

	// Process items via callback — avoids per-item copy.
	// fn is called concurrently; each outIdx is unique, so writes to out[] are safe.
	if err := r.pr.ReadItems(ctx, indices, func(idx int, data []byte) error {
		start := groupStart[idx]
		end := len(found)
		if idx+1 < len(groupStart) {
			end = groupStart[idx+1]
		}
		for k := start; k < end; k++ {
			bm, err := verifyAndDeserialize(data, found[k].hk[:])
			if err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					continue
				}
				return err
			}
			out[found[k].outIdx] = bm
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return out, nil
}

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	return r.pr.ContentHash()
}

// Verify recomputes the SHA-256 content hash and compares to stored hash.
func (r *Reader) Verify(ctx context.Context) error {
	return r.pr.Verify(ctx)
}

// Close releases all resources. Safe to call multiple times.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		idx, _ := r.waitMPHF()
		if idx != nil {
			r.closeErr = idx.Close()
		}
		r.closeErr = errors.Join(r.closeErr, r.pr.Close())
	})
	return r.closeErr
}

// --- Internal helpers ---

// verifyAndDeserialize checks the fingerprint and deserializes the bitmap.
func verifyAndDeserialize(entry []byte, hk []byte) (*roaring.Bitmap, error) {
	if len(entry) < fingerprintSize ||
		entry[0] != hk[0] || entry[1] != hk[1] ||
		entry[2] != hk[2] || entry[3] != hk[3] {
		return nil, ErrKeyNotFound
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(entry[fingerprintSize:]); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap: %w", err)
	}
	return bm, nil
}
