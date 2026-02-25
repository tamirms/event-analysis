package rocksdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"sync"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/rocksdbutil"
)

var _ eventstore.StoreReader = (*Reader)(nil)

// Reader reads events from a RocksDB eventstore.
// Open returns immediately; the DB is opened asynchronously.
type Reader struct {
	dbPath      string
	concurrency int

	once    sync.Once
	db      *grocksdb.DB
	opts    *grocksdb.Options
	ro      *grocksdb.ReadOptions
	openErr error

	countOnce sync.Once
	nEvents   int
	countErr  error

	closeOnce sync.Once
}

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadIndices.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// Open opens a RocksDB eventstore for reading. Returns immediately;
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

func (r *Reader) doOpen()      { r.once.Do(r.openDB) }
func (r *Reader) waitOpen() error { r.once.Do(r.openDB); return r.openErr }

func (r *Reader) EventCount() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	r.countOnce.Do(func() {
		it := r.db.NewIterator(r.ro)
		defer it.Close()
		it.SeekToLast()
		if !it.Valid() {
			r.nEvents = 0
			return
		}
		key := it.Key()
		defer key.Free()
		if key.Size() != 4 {
			r.countErr = fmt.Errorf("rocksdb eventstore: last key has unexpected size %d", key.Size())
			return
		}
		r.nEvents = int(binary.BigEndian.Uint32(key.Data())) + 1
	})
	return r.nEvents, r.countErr
}

func (r *Reader) ReadEvent(index int) ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}
	n, err := r.EventCount()
	if err != nil {
		return nil, err
	}
	if index < 0 || index >= n {
		return nil, eventstore.ErrIndexRange
	}
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], uint32(index))
	val, err := r.db.GetPinned(r.ro, key[:])
	if err != nil {
		return nil, fmt.Errorf("rocksdb eventstore: get: %w", err)
	}
	defer val.Destroy()
	if !val.Exists() {
		return nil, eventstore.ErrIndexRange
	}
	result := make([]byte, len(val.Data()))
	copy(result, val.Data())
	return result, nil
}

func (r *Reader) ReadEvents(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}
		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}
		n, err := r.EventCount()
		if err != nil {
			yield(nil, err)
			return
		}
		if start < 0 || count < 0 || start > n || count > n-start {
			panic(fmt.Sprintf("rocksdb eventstore: ReadEvents(%d, %d) out of range [0, %d)",
				start, count, n))
		}
		var key [4]byte
		binary.BigEndian.PutUint32(key[:], uint32(start))
		it := r.db.NewIterator(r.ro)
		defer it.Close()
		it.Seek(key[:])
		for i := range count {
			if !it.Valid() {
				yield(nil, fmt.Errorf("rocksdb eventstore: iterator exhausted at offset %d", i))
				return
			}
			if !yield(it.ValueSlice().Data(), nil) {
				return
			}
			it.Next()
		}
		if err := it.Err(); err != nil {
			yield(nil, err)
		}
	}
}

func (r *Reader) ReadIndices(ctx context.Context, indices []int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if len(indices) == 0 {
			return
		}
		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}
		n, err := r.EventCount()
		if err != nil {
			yield(nil, err)
			return
		}
		// Validate bounds and sorted+unique invariant.
		for i, idx := range indices {
			if idx < 0 || idx >= n {
				panic(fmt.Sprintf("rocksdb eventstore: ReadIndices index %d out of range [0, %d)",
					idx, n))
			}
			if i > 0 && indices[i] <= indices[i-1] {
				panic(fmt.Sprintf("rocksdb eventstore: ReadIndices indices not sorted/unique at position %d: %d <= %d",
					i, indices[i], indices[i-1]))
			}
		}

		keys := make([][]byte, len(indices))
		keyBuf := make([]byte, len(indices)*4)
		for i, idx := range indices {
			keys[i] = keyBuf[i*4 : i*4+4]
			binary.BigEndian.PutUint32(keys[i], uint32(idx))
		}

		// Use a separate ReadOptions with async I/O for scattered reads.
		ro := rocksdbutil.NewReadOptions()
		ro.SetAsyncIO(true)
		defer ro.Destroy()

		cf := r.db.GetDefaultColumnFamily()

		// Collect results into a flat slice, then yield in order.
		results := make([][]byte, len(keys))
		concurrency := min(r.concurrency, len(keys))

		if concurrency <= 1 {
			vals, err := r.db.BatchedMultiGetCF(ro, cf, true, keys...)
			if err != nil {
				yield(nil, fmt.Errorf("rocksdb eventstore: batched multi get: %w", err))
				return
			}
			for i, val := range vals {
				if val.Exists() {
					results[i] = make([]byte, len(val.Data()))
					copy(results[i], val.Data())
				}
			}
			vals.Destroy()
		} else {
			var (
				wg       sync.WaitGroup
				errOnce  sync.Once
				firstErr error
			)
			perWorker := (len(keys) + concurrency - 1) / concurrency
			for w := range concurrency {
				lo := w * perWorker
				hi := min(lo+perWorker, len(keys))
				if lo >= len(keys) {
					break
				}
				wg.Go(func() {
					vals, err := r.db.BatchedMultiGetCF(ro, cf, true, keys[lo:hi]...)
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					for i, val := range vals {
						if val.Exists() {
							results[lo+i] = make([]byte, len(val.Data()))
							copy(results[lo+i], val.Data())
						}
					}
					vals.Destroy()
				})
			}
			wg.Wait()
			if firstErr != nil {
				yield(nil, fmt.Errorf("rocksdb eventstore: batched multi get: %w", firstErr))
				return
			}
		}

		for i, data := range results {
			if ctx.Err() != nil {
				yield(nil, ctx.Err())
				return
			}
			if data == nil {
				if !yield(nil, fmt.Errorf("rocksdb eventstore: key %d not found (possible DB corruption)", indices[i])) {
					return
				}
				continue
			}
			if !yield(data, nil) {
				return
			}
		}
	}
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
