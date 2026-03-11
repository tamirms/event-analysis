package packfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"sync"
)

// Checkpoint captures the durable state of a LiveWriter after Sync.
// The caller persists this externally (e.g. to RocksDB) alongside a replay
// cursor. On recovery, OpenLive restores the Writer from this state.
type Checkpoint struct {
	Offsets    []int64      // one per flushed record
	EndOfData  int64        // byte offset of end of last flushed record
	Digests    []byte       // serialHasher.digests (content hash state)
	RecordSize int          // validated on recovery
	Format     RecordFormat // validated on recovery
}

// TotalItems returns the number of items represented by flushed records.
func (cp Checkpoint) TotalItems() int {
	return len(cp.Offsets) * cp.RecordSize
}

var errLiveWriterClosed = errors.New("packfile: LiveWriter is closed")

// LiveWriter supports incremental packfile construction with concurrent
// reads on flushed data and crash recovery via Checkpoint.
//
// Writes (Append, Sync, Freeze) hold an exclusive lock.
// Reads (TotalItems, ReadItem, ReadRange, ReadItems, ContentHash, Verify)
// hold a shared lock, allowing concurrent reads but blocking writes.
type LiveWriter struct {
	mu       sync.RWMutex
	w        *Writer
	readFile *os.File // shared read fd, opened eagerly
	reader   *Reader  // flushed-records reader, rebuilt after each flush
}

// checkOpen returns an error if the LiveWriter has been closed. Caller must hold mu.
func (lw *LiveWriter) checkOpen() error {
	if lw.readFile == nil {
		return errLiveWriterClosed
	}
	return nil
}

// CreateLive starts a new live packfile at path. The file is written to
// directly. Concurrency must be 0 (serial). By default, fails if the
// file already exists; set Overwrite to replace an existing file.
func CreateLive(path string, opts WriterOptions) (*LiveWriter, error) {
	if opts.Concurrency > 0 {
		return nil, errors.New("packfile: CreateLive does not support Concurrency > 0")
	}
	recordSize, err := resolveRecordSize(opts)
	if err != nil {
		return nil, err
	}

	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	if opts.Overwrite {
		flags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	w := newSerialWriter(f, path, recordSize, opts)

	rf, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &LiveWriter{w: w, readFile: rf}, nil
}

// OpenLive recovers a LiveWriter from a Checkpoint. The file is truncated
// to cp.EndOfData, fsynced, and the Writer state is reconstructed.
func OpenLive(path string, cp Checkpoint, opts WriterOptions) (*LiveWriter, error) {
	if opts.Concurrency > 0 {
		return nil, errors.New("packfile: OpenLive does not support Concurrency > 0")
	}
	if opts.Overwrite {
		return nil, errors.New("packfile: OpenLive does not support Overwrite")
	}
	recordSize, err := resolveRecordSize(opts)
	if err != nil {
		return nil, err
	}
	if cp.RecordSize != recordSize {
		return nil, fmt.Errorf("packfile: checkpoint RecordSize %d != opts RecordSize %d", cp.RecordSize, recordSize)
	}
	if cp.Format != opts.Format {
		return nil, fmt.Errorf("packfile: checkpoint Format %v != opts Format %v", cp.Format, opts.Format)
	}
	if opts.ContentHash && len(cp.Digests)%sha256.Size != 0 {
		return nil, fmt.Errorf("packfile: checkpoint Digests length %d not a multiple of %d", len(cp.Digests), sha256.Size)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(cp.EndOfData); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}

	w := newSerialWriter(f, path, recordSize, opts)
	w.pos = cp.EndOfData
	w.total = len(cp.Offsets) * recordSize
	w.lastSyncPos = cp.EndOfData
	w.offsets = slices.Clone(cp.Offsets)
	if w.serialHasher != nil {
		w.serialHasher.digests = bytes.Clone(cp.Digests)
	}

	rf, err := os.Open(path)
	if err != nil {
		f.Close()
		return nil, err
	}

	lw := &LiveWriter{w: w, readFile: rf}
	if len(cp.Offsets) > 0 {
		lw.rebuildReader()
	}
	return lw, nil
}

// Append adds a single logical item. Flushes a record when RecordSize
// items accumulate. Thread-safe.
func (lw *LiveWriter) Append(parts ...[]byte) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	nBefore := len(lw.w.offsets)
	if err := lw.w.Append(parts...); err != nil {
		return err
	}
	if len(lw.w.offsets) > nBefore {
		lw.rebuildReader()
	}
	return nil
}

// Sync fsyncs the file and returns a Checkpoint reflecting exactly what
// is durable. Only full (flushed) records are checkpointed.
func (lw *LiveWriter) Sync() (Checkpoint, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if lw.w.closed {
		return Checkpoint{}, errors.New("packfile: LiveWriter is closed")
	}

	if err := lw.w.file.Sync(); err != nil {
		return Checkpoint{}, err
	}

	cp := Checkpoint{
		Offsets:    slices.Clone(lw.w.offsets),
		EndOfData:  lw.w.pos,
		RecordSize: lw.w.recordSize,
		Format:     lw.w.format,
	}
	if lw.w.serialHasher != nil {
		cp.Digests = bytes.Clone(lw.w.serialHasher.digests)
	}

	return cp, nil
}

// TotalItems returns the number of visible items (flushed + pending).
func (lw *LiveWriter) TotalItems() (int, error) {
	lw.mu.RLock()
	defer lw.mu.RUnlock()
	if err := lw.checkOpen(); err != nil {
		return 0, err
	}
	return lw.totalItems(), nil
}

// ContentHash returns the SHA-256 content hash over items added so far.
// Before Freeze, this is a snapshot of the incrementally-computed hash.
// After Freeze, this is the final hash written to the trailer.
// Returns (zero, false, nil) if content hashing is not enabled.
func (lw *LiveWriter) ContentHash() ([32]byte, bool, error) {
	lw.mu.RLock()
	defer lw.mu.RUnlock()
	if err := lw.checkOpen(); err != nil {
		return [32]byte{}, false, err
	}
	if lw.w.closed {
		return lw.w.finalHash, lw.w.hasFinalHash, nil
	}
	if lw.w.serialHasher == nil {
		return [32]byte{}, false, nil
	}
	return lw.w.serialHasher.Snapshot(), true, nil
}

// Verify recomputes the SHA-256 content hash by reading all items and
// compares it to the stored hash. Only valid after Freeze.
func (lw *LiveWriter) Verify(ctx context.Context) error {
	lw.mu.RLock()
	defer lw.mu.RUnlock()
	if err := lw.checkOpen(); err != nil {
		return err
	}
	if !lw.w.closed {
		return errors.New("packfile: cannot verify before Freeze")
	}
	if !lw.w.hasFinalHash {
		return nil
	}
	return verifyContentHash(ctx, lw.reader, lw.w.recordSize, lw.w.finalHash)
}

// ReadItem reads a single item by index and passes it to fn.
func (lw *LiveWriter) ReadItem(index int, fn func([]byte) error) error {
	lw.mu.RLock()
	defer lw.mu.RUnlock()

	if err := lw.checkOpen(); err != nil {
		return err
	}

	total := lw.totalItems()
	if index < 0 || index >= total {
		return ErrIndexRange
	}

	flushedN := lw.flushedItems()
	if index < flushedN {
		return lw.reader.ReadItem(index, fn)
	}
	return fn(lw.pendingSlice(index - flushedN))
}

// ReadRange returns an iterator over count contiguous items starting at start.
func (lw *LiveWriter) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}
		lw.mu.RLock()
		defer lw.mu.RUnlock()

		if err := lw.checkOpen(); err != nil {
			yield(nil, err)
			return
		}

		total := lw.totalItems()
		if start < 0 || count < 0 || start > total || count > total-start {
			panic(fmt.Sprintf("packfile: LiveWriter.ReadRange(%d, %d) out of range [0, %d)",
				start, count, total))
		}

		end := start + count
		flushedN := lw.flushedItems()

		// Flushed portion.
		if start < flushedN {
			flushedCount := min(end, flushedN) - start
			for item, err := range lw.reader.ReadRange(start, flushedCount) {
				if !yield(item, err) || err != nil {
					return
				}
			}
		}

		// Pending portion.
		for i := max(start, flushedN); i < end; i++ {
			if !yield(lw.pendingSlice(i-flushedN), nil) {
				return
			}
		}
	}
}

// ReadItems reads items at scattered indices with parallel I/O for flushed
// items and sequential access for pending items.
// indices must be sorted ascending with no duplicates.
func (lw *LiveWriter) ReadItems(ctx context.Context, indices []int, fn func(pos int, entry []byte) error) error {
	if len(indices) == 0 {
		return nil
	}

	lw.mu.RLock()
	defer lw.mu.RUnlock()

	if err := lw.checkOpen(); err != nil {
		return err
	}

	total := lw.totalItems()
	flushedN := lw.flushedItems()

	// Validate bounds and sorted+unique invariant.
	for i, idx := range indices {
		if idx < 0 || idx >= total {
			panic(fmt.Sprintf("packfile: LiveWriter.ReadItems index %d out of range [0, %d)", idx, total))
		}
		if i > 0 && indices[i] <= indices[i-1] {
			panic(fmt.Sprintf("packfile: LiveWriter.ReadItems indices not sorted/unique at position %d: %d <= %d",
				i, indices[i], indices[i-1]))
		}
	}

	// Indices are sorted; binary search for the flushed/pending boundary.
	splitPos, _ := slices.BinarySearch(indices, flushedN)

	// Flushed batch via Reader (parallel pread + coalescing).
	// Positions are preserved: indices[:splitPos] is a prefix of the original slice.
	if splitPos > 0 {
		if err := lw.reader.ReadItems(ctx, indices[:splitPos], fn); err != nil {
			return err
		}
	}

	// Pending batch.
	for i := splitPos; i < len(indices); i++ {
		if err := fn(i, lw.pendingSlice(indices[i]-flushedN)); err != nil {
			return err
		}
	}

	return nil
}

// Freeze finalizes the packfile: flushes any partial record, writes the
// index + trailer, fsyncs, and closes the write fd. After Freeze, the file
// is a standard packfile readable via Open. Reads on the LiveWriter continue
// to work until Close is called.
func (lw *LiveWriter) Freeze(appData []byte) error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if err := lw.w.Finish(appData); err != nil {
		return err
	}
	lw.rebuildReader()
	return nil
}

// Close releases resources. If Freeze was called, the read fd is closed
// and the file remains as a valid packfile. If Freeze was not called,
// the file is removed. Safe to call multiple times. Idiomatic usage:
//
//	lw, _ := CreateLive(path, opts)
//	defer lw.Close()
//	// ... Append, Sync ...
//	return lw.Freeze(nil)
func (lw *LiveWriter) Close() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.reader = nil
	lw.closeReadFile()
	return lw.w.Close()
}

// totalItems returns flushed + pending count. Caller must hold mu.
func (lw *LiveWriter) totalItems() int {
	return lw.w.total
}

// flushedItems returns the number of flushed items. Caller must hold mu.
func (lw *LiveWriter) flushedItems() int {
	return lw.w.total - len(lw.w.sizes)
}

// pendingSlice returns the raw bytes of the pending item at pendingIdx.
// Caller must hold mu.
func (lw *LiveWriter) pendingSlice(pendingIdx int) []byte {
	off := 0
	for i := range pendingIdx {
		off += int(lw.w.sizes[i])
	}
	return lw.w.buf[off : off+int(lw.w.sizes[pendingIdx])]
}

// rebuildReader creates a new Reader from current flushed state.
// Caller must hold mu exclusively.
func (lw *LiveWriter) rebuildReader() {
	offsets := slices.Clone(lw.w.offsets)
	if !lw.w.closed {
		// During live writes: append end-of-data sentinel.
		// After Finish: w.offsets already includes it.
		offsets = append(offsets, lw.w.pos)
	}

	lw.reader = newReaderFromState(openResult{
		file:       lw.readFile,
		offsets:    offsets,
		totalItems: lw.flushedItems(),
		recordSize: lw.w.recordSize,
		trailer:    Trailer{Format: lw.w.format, RecordSize: uint32(lw.w.recordSize)},
	})
}

func (lw *LiveWriter) closeReadFile() {
	if lw.readFile != nil {
		lw.readFile.Close()
		lw.readFile = nil
	}
}
