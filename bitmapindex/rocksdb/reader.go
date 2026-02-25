package rocksdb

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/rocksdbutil"
)

var _ bitmapindex.IndexReader = (*Reader)(nil)

// Reader provides bitmap lookups from a RocksDB bitmap index.
// Open returns immediately; the DB is opened asynchronously.
type Reader struct {
	dbPath      string
	concurrency int

	once    sync.Once
	db      *grocksdb.DB
	opts    *grocksdb.Options
	ro      *grocksdb.ReadOptions
	openErr error

	closeOnce sync.Once
}

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for LookupKeys.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Open opens a RocksDB bitmap index for reading. Returns immediately;
// the DB is opened asynchronously in a background goroutine.
func Open(dbPath string, opts ...ReaderOption) *Reader {
	r := &Reader{dbPath: dbPath, concurrency: 8}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}
	go r.doOpen()
	return r
}

func (r *Reader) openDB() {
	db, dbOpts, err := rocksdbutil.OpenReadOnly(r.dbPath)
	if err != nil {
		r.openErr = err
		return
	}
	r.db = db
	r.opts = dbOpts
	r.ro = rocksdbutil.NewReadOptions()
}

func (r *Reader) doOpen()        { r.once.Do(r.openDB) }
func (r *Reader) waitOpen() error { r.once.Do(r.openDB); return r.openErr }

func (r *Reader) Lookup(f bitmapindex.Field, key []byte) (*roaring.Bitmap, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}

	val, err := r.db.GetPinned(r.ro, compositeKey(f, key))
	if err != nil {
		return nil, fmt.Errorf("rocksdb bitmapindex: get: %w", err)
	}
	defer val.Destroy()

	if !val.Exists() {
		return nil, bitmapindex.ErrKeyNotFound
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(val.Data()); err != nil {
		return nil, fmt.Errorf("rocksdb bitmapindex: deserialize bitmap: %w", err)
	}
	return bm, nil
}

func (r *Reader) LookupKeys(ctx context.Context, keys []bitmapindex.FieldKey) ([]*roaring.Bitmap, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	if err := r.waitOpen(); err != nil {
		return nil, err
	}

	// Build composite keys and track original indices.
	type indexedKey struct {
		origIdx      int
		compositeKey []byte
	}
	indexed := make([]indexedKey, len(keys))
	for i, k := range keys {
		indexed[i] = indexedKey{origIdx: i, compositeKey: compositeKey(k.Field, k.Key)}
	}

	// Sort by composite key for sorted_input=true.
	sort.Slice(indexed, func(i, j int) bool {
		return bytes.Compare(indexed[i].compositeKey, indexed[j].compositeKey) < 0
	})

	sortedKeys := make([][]byte, len(indexed))
	for i, ik := range indexed {
		sortedKeys[i] = ik.compositeKey
	}

	cf := r.db.GetDefaultColumnFamily()

	// Use a separate ReadOptions with async I/O for batch reads.
	ro := rocksdbutil.NewReadOptions()
	ro.SetAsyncIO(true)
	defer ro.Destroy()

	// Collect deserialized bitmaps per sorted position.
	bitmaps := make([]*roaring.Bitmap, len(sortedKeys))
	concurrency := min(r.concurrency, len(sortedKeys))

	if concurrency <= 1 {
		vals, err := r.db.BatchedMultiGetCF(ro, cf, true, sortedKeys...)
		if err != nil {
			return nil, fmt.Errorf("rocksdb bitmapindex: batched multi get: %w", err)
		}
		for i, val := range vals {
			if val.Exists() {
				bm := roaring.New()
				if err := bm.UnmarshalBinary(val.Data()); err != nil {
					vals.Destroy()
					return nil, fmt.Errorf("rocksdb bitmapindex: deserialize bitmap for key %d: %w", indexed[i].origIdx, err)
				}
				bitmaps[i] = bm
			}
		}
		vals.Destroy()
	} else {
		var (
			wg       sync.WaitGroup
			errOnce  sync.Once
			firstErr error
		)
		perWorker := (len(sortedKeys) + concurrency - 1) / concurrency
		for w := range concurrency {
			lo := w * perWorker
			hi := min(lo+perWorker, len(sortedKeys))
			if lo >= len(sortedKeys) {
				break
			}
			wg.Go(func() {
				vals, err := r.db.BatchedMultiGetCF(ro, cf, true, sortedKeys[lo:hi]...)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				for i, val := range vals {
					if val.Exists() {
						bm := roaring.New()
						if err := bm.UnmarshalBinary(val.Data()); err != nil {
							errOnce.Do(func() { firstErr = err })
							vals.Destroy()
							return
						}
						bitmaps[lo+i] = bm
					}
				}
				vals.Destroy()
			})
		}
		wg.Wait()
		if firstErr != nil {
			return nil, fmt.Errorf("rocksdb bitmapindex: batched multi get: %w", firstErr)
		}
	}

	// Scatter results back to original positions.
	out := make([]*roaring.Bitmap, len(keys))
	for i, bm := range bitmaps {
		out[indexed[i].origIdx] = bm
	}
	return out, nil
}

func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.waitOpen()
		if r.ro != nil {
			r.ro.Destroy()
		}
		if r.db != nil {
			r.db.Close()
		}
		if r.opts != nil {
			r.opts.Destroy()
		}
	})
	return nil
}
