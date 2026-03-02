package packfile

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"iter"
	"os"
	"sync"
	"sync/atomic"
)

const (
	readBufSize         = 1 << 20    // 1MB
	speculativeReadSize = 256 * 1024 // 256KB speculative read for Open
	defaultConcurrency  = 8
)

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readBufSize)
		return &b
	},
}

// openResult holds everything produced by doOpen.
type openResult struct {
	file    ReadAtCloser
	trailer Trailer
	offsets []int64
	appData []byte

	// Decoded from trailer for internal use (int casts of uint32 trailer fields).
	totalItems int
	recordSize int

	err error
}

// Reader provides random access to items in a packfile.
// Safe for concurrent use by multiple goroutines.
//
// Open returns immediately; all file I/O runs in a background goroutine.
// Public methods block (via waitOpen) until the goroutine completes.
// Close must always be called to release resources.
type Reader struct {
	ch        <-chan openResult
	once      sync.Once
	closeOnce sync.Once

	openResult  // embedded: file, trailer, offsets, metadata fields
	concurrency int

	openErr  error
	closeErr error
}

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadItems.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption {
	return func(r *Reader) { r.concurrency = n }
}

// drain receives the goroutine result and populates all Reader fields.
func (r *Reader) drain() {
	res := <-r.ch
	r.openErr = res.err
	res.err = nil
	r.openResult = res
}

// waitOpen blocks until the background Open goroutine has finished.
func (r *Reader) waitOpen() error {
	r.once.Do(r.drain)
	return r.openErr
}

// Open returns a Reader immediately. All file I/O (open, stat, speculative
// read, trailer parse, index decode, app data read) runs in a background
// goroutine. Open never fails; errors are deferred to the first method that
// needs the result. Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader {
	r := &Reader{concurrency: defaultConcurrency}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}

	ch := make(chan openResult, 1)
	r.ch = ch
	go func() {
		defer func() {
			if rv := recover(); rv != nil {
				ch <- openResult{err: fmt.Errorf("packfile: panic in Open: %v", rv)}
			}
		}()
		ch <- doOpen(path)
	}()
	return r
}

// doOpen performs all synchronous I/O for opening a packfile.
// On error it closes the file and returns openResult{err: ...}.
func doOpen(path string) openResult {
	f, err := os.Open(path)
	if err != nil {
		return openResult{err: err}
	}
	cleanup := true
	defer func() {
		if cleanup {
			f.Close()
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return openResult{err: err}
	}
	fileSize := fi.Size()

	if fileSize < trailerSize {
		return openResult{err: ErrSize}
	}

	// Speculative read: last min(speculativeReadSize, fileSize) bytes.
	speculativeSize := min(int64(speculativeReadSize), fileSize)
	speculativeOff := fileSize - speculativeSize
	speculativeBuf := make([]byte, speculativeSize)
	if _, err := f.ReadAt(speculativeBuf, speculativeOff); err != nil {
		return openResult{err: err}
	}

	// Parse 64-byte trailer from end of speculativeBuf.
	tb := speculativeBuf[len(speculativeBuf)-trailerSize:]

	m := binary.LittleEndian.Uint32(tb[0:])
	if m != magic {
		return openResult{err: ErrMagic}
	}
	v := tb[4]
	if v != version {
		return openResult{err: ErrVersion}
	}
	flags := tb[5]
	recordCount := int(binary.LittleEndian.Uint32(tb[6:]))
	totalItems := int(binary.LittleEndian.Uint32(tb[10:]))
	recordSize := int(binary.LittleEndian.Uint32(tb[14:]))
	indexSize := int(binary.LittleEndian.Uint32(tb[18:]))
	appDataSize := int(binary.LittleEndian.Uint32(tb[22:]))
	var contentHash [32]byte
	copy(contentHash[:], tb[26:58])
	storedCRC := binary.LittleEndian.Uint32(tb[60:])

	// Validate flags.
	if flags&^knownFlags != 0 {
		return openResult{err: fmt.Errorf("packfile: unknown trailer flags: %02x", flags)}
	}
	hasContentHash := flags&flagContentHash != 0
	var format RecordFormat
	switch {
	case flags&flagNoCompression == 0:
		format = Compressed
	case flags&flagNoCRC != 0:
		format = Raw
	default:
		format = Uncompressed
	}
	if !hasContentHash {
		contentHash = [32]byte{}
	}

	indexBase := fileSize - int64(trailerSize) - int64(appDataSize) - int64(indexSize)
	if indexBase < 0 {
		return openResult{err: ErrSize}
	}

	// Tail = index + appData + trailer.
	tailSize := int64(indexSize) + int64(appDataSize) + int64(trailerSize)

	var indexBuf []byte
	var appData []byte

	if tailSize <= speculativeSize {
		// Everything is in speculativeBuf.
		tailStart := len(speculativeBuf) - int(tailSize)

		indexBuf = make([]byte, indexSize+7) // +7 for safe 8-byte overshoot
		copy(indexBuf, speculativeBuf[tailStart:tailStart+indexSize])

		if appDataSize > 0 {
			appData = make([]byte, appDataSize)
			adStart := tailStart + indexSize
			copy(appData, speculativeBuf[adStart:adStart+appDataSize])
		}
	} else {
		// Index + appData too large for speculativeBuf — single fallback read.
		readSize := indexSize + appDataSize
		buf := make([]byte, readSize+7) // +7 for safe 8-byte overshoot in DecodeGroup
		if readSize > 0 {
			if _, err := f.ReadAt(buf[:readSize], indexBase); err != nil {
				return openResult{err: err}
			}
		}

		indexBuf = buf[:indexSize+7]
		if appDataSize > 0 {
			appData = make([]byte, appDataSize)
			copy(appData, buf[indexSize:indexSize+appDataSize])
		}
	}

	// CRC verification: CRC32C(appData || trailer[0:60]) == storedCRC.
	crc := crc32.New(crc32cTable)
	if appDataSize > 0 {
		crc.Write(appData)
	}
	crc.Write(tb[:60])
	if crc.Sum32() != storedCRC {
		return openResult{err: ErrChecksum}
	}

	offsets, err := decodeIndex(indexBuf, recordCount, indexSize, indexBase)
	if err != nil {
		return openResult{err: err}
	}

	// Validate recordSize.
	if recordSize <= 0 && recordCount > 0 {
		return openResult{err: fmt.Errorf("packfile: invalid recordSize %d in trailer", recordSize)}
	}

	// Cross-validate: number of records implied by totalItems must match recordCount.
	if recordSize > 0 {
		expectedRecords := (totalItems + recordSize - 1) / recordSize
		if totalItems == 0 {
			expectedRecords = 0
		}
		if expectedRecords != recordCount {
			return openResult{err: fmt.Errorf("packfile: trailer says %d items / %d recordSize = %d records, but packfile has %d records",
				totalItems, recordSize, expectedRecords, recordCount)}
		}
	}

	res := openResult{
		file: f,
		trailer: Trailer{
			Version:        v,
			RecordCount:    uint32(recordCount),
			TotalItems:     uint32(totalItems),
			RecordSize:     uint32(recordSize),
			IndexSize:      uint32(indexSize),
			AppDataSize:    uint32(appDataSize),
			ContentHash:    contentHash,
			Format:         format,
			HasContentHash: hasContentHash,
			Checksum:       storedCRC,
		},
		offsets:    offsets,
		appData:    appData,
		totalItems: totalItems,
		recordSize: recordSize,
	}

	// Default recordSize for empty files (writer stores the configured value,
	// e.g. 128, but a hand-crafted file could have 0). Patch both the internal
	// field and the Trailer to keep them consistent.
	if res.recordSize == 0 {
		res.recordSize = 1
		res.trailer.RecordSize = 1
	}

	cleanup = false
	return res
}

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error) {
	if err := r.waitOpen(); err != nil {
		return 0, err
	}
	return r.totalItems, nil
}

// getDecoder returns a pooled decoder configured for this reader's format.
func (r *Reader) getDecoder() *decoder {
	rd := getDecoder()
	rd.format = r.trailer.Format
	rd.totalItems = r.totalItems
	rd.recordSize = r.recordSize
	return rd
}

// ReadItem reads a single item by global index.
// Returns ErrIndexRange if index is out of [0, TotalItems).
// The caller owns the returned slice.
func (r *Reader) ReadItem(index int) ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}
	if index < 0 || index >= r.totalItems {
		return nil, ErrIndexRange
	}

	recordIdx := index / r.recordSize
	localIdx := index % r.recordSize

	rd := r.getDecoder()
	defer putDecoder(rd)

	start, end := r.offsets[recordIdx], r.offsets[recordIdx+1]
	size := int(end - start)
	if cap(rd.scratch) < size {
		rd.scratch = make([]byte, size)
	} else {
		rd.scratch = rd.scratch[:size]
	}
	if _, err := r.file.ReadAt(rd.scratch, start); err != nil {
		return nil, err
	}
	if err := rd.Decode(rd.scratch, recordIdx); err != nil {
		return nil, err
	}
	return rd.EntryCopy(localIdx), nil
}

// ReadRange returns an iterator over count contiguous items starting at start.
// Consecutive records are coalesced into single ReadAt calls using a pooled
// 1MB buffer, minimizing I/O syscalls for large ranges.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		if start < 0 || count < 0 || start > r.totalItems || count > r.totalItems-start {
			panic(fmt.Sprintf("packfile: ReadRange(%d, %d) out of range [0, %d)",
				start, count, r.totalItems))
		}

		firstRecord := start / r.recordSize
		lastRecord := (start + count - 1) / r.recordSize
		end := start + count // one past last item

		rd := r.getDecoder()
		defer putDecoder(rd)

		bp := readBufPool.Get().(*[]byte)
		buf := *bp
		defer readBufPool.Put(bp)

		globalIdx := start

		// yieldRecord decodes a record and yields the items within the
		// [globalIdx, end) range. Returns false if the consumer broke early.
		yieldRecord := func(recData []byte, recIdx int) bool {
			if err := rd.Decode(recData, recIdx); err != nil {
				yield(nil, err)
				return false
			}
			recStart := recIdx * r.recordSize
			lo := globalIdx - recStart
			hi := min(len(rd.sizes), end-recStart)
			for i := lo; i < hi; i++ {
				if !yield(rd.Entry(i), nil) {
					return false
				}
				globalIdx++
			}
			return true
		}

		// Batch consecutive records into single ReadAt calls.
		batchStart := firstRecord
		for batchStart <= lastRecord {
			batchEnd := batchStart + 1
			for batchEnd <= lastRecord && r.offsets[batchEnd+1]-r.offsets[batchStart] <= int64(len(buf)) {
				batchEnd++
			}

			// If a single record exceeds the buffer, allocate one-off.
			recBytes := r.offsets[batchStart+1] - r.offsets[batchStart]
			if recBytes > int64(len(buf)) {
				oneOff := make([]byte, recBytes)
				if _, err := r.file.ReadAt(oneOff, r.offsets[batchStart]); err != nil {
					yield(nil, err)
					return
				}
				if !yieldRecord(oneOff, batchStart) {
					return
				}
				batchStart++
				continue
			}

			// Read batch.
			batchBytes := r.offsets[batchEnd] - r.offsets[batchStart]
			readBuf := buf[:batchBytes]
			if _, err := r.file.ReadAt(readBuf, r.offsets[batchStart]); err != nil {
				yield(nil, err)
				return
			}

			for j := batchStart; j < batchEnd; j++ {
				lo := r.offsets[j] - r.offsets[batchStart]
				hi := r.offsets[j+1] - r.offsets[batchStart]
				if !yieldRecord(readBuf[lo:hi], j) {
					return
				}
			}

			batchStart = batchEnd
		}
	}
}

// ReadItems reads items at scattered indices with parallel I/O + decode,
// yielding results in index order. Each yielded []byte is valid only until
// the next iteration — copy if you need to retain it.
//
// Indices are grouped by record, then consecutive records are partitioned
// into I/O batches (≤ 1MB each) upfront. Workers claim batches via a simple
// atomic counter — no CAS coalescing needed.
//
// indices must be sorted ascending with no duplicates.
// Panics if any index is out of [0, TotalItems) or if indices are not sorted/unique.
func (r *Reader) ReadItems(ctx context.Context, indices []int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if len(indices) == 0 {
			return
		}

		if err := r.waitOpen(); err != nil {
			yield(nil, err)
			return
		}

		// Validate bounds and sorted+unique invariant.
		for i, idx := range indices {
			if idx < 0 || idx >= r.totalItems {
				panic(fmt.Sprintf("packfile: ReadItems index %d out of range [0, %d)",
					idx, r.totalItems))
			}
			if i > 0 && indices[i] <= indices[i-1] {
				panic(fmt.Sprintf("packfile: ReadItems indices not sorted/unique at position %d: %d <= %d",
					i, indices[i], indices[i-1]))
			}
		}

		// Partition indices into I/O batches in a single pass. Consecutive
		// records whose total bytes ≤ readBufSize share a batch (single
		// ReadAt). Non-consecutive records, buffer overflow, or exceeding
		// the per-batch item cap start a new batch. The cap ensures enough
		// batches for full worker utilization.
		type ioBatch struct {
			idxStart    int // first position in indices[]
			idxEnd      int // one past last position in indices[]
			firstRecord int // first record in batch (for ReadAt offset)
			lastRecord  int // last record in batch (for ReadAt end)
		}
		maxPerBatch := max(1, (len(indices)+r.concurrency-1)/r.concurrency)
		batches := make([]ioBatch, 0, r.concurrency)
		batchIdxStart := 0
		firstRec := indices[0] / r.recordSize
		lastRec := firstRec
		batchBytes := r.offsets[firstRec+1] - r.offsets[firstRec]

		for i := 1; i < len(indices); i++ {
			rec := indices[i] / r.recordSize
			if rec == lastRec {
				continue // same record, extend batch
			}
			recBytes := r.offsets[rec+1] - r.offsets[rec]
			if rec == lastRec+1 &&
				batchBytes+recBytes <= int64(readBufSize) &&
				i-batchIdxStart < maxPerBatch {
				batchBytes += recBytes
				lastRec = rec
			} else {
				batches = append(batches, ioBatch{batchIdxStart, i, firstRec, lastRec})
				batchIdxStart = i
				firstRec = rec
				lastRec = rec
				batchBytes = recBytes
			}
		}
		batches = append(batches, ioBatch{batchIdxStart, len(indices), firstRec, lastRec})

		// Early check: if context is already canceled, return the error immediately.
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		// Batch-level pipeline. Workers send one result
		// per I/O batch (not per record), eliminating per-record channel
		// ops, decoder pool round-trips, and reorder map operations.
		type batchResult struct {
			batchIdx int
			items    [][]byte
			err      error
		}

		numWorkers := min(len(batches), r.concurrency)
		resultCh := make(chan batchResult, numWorkers)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Workers: claim batches via atomic counter, read + decode entire
		// batch, copy requested items, send one result per batch.
		var nextBatch atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for range numWorkers {
			go func() {
				defer wg.Done()
				rd := r.getDecoder()
				defer putDecoder(rd)
				bp := readBufPool.Get().(*[]byte)
				buf := *bp
				defer func() { *bp = buf; readBufPool.Put(bp) }()

				for {
					bi := int(nextBatch.Add(1)) - 1
					if bi >= len(batches) || ctx.Err() != nil {
						return
					}
					batch := batches[bi]

					readStart := r.offsets[batch.firstRecord]
					readEnd := r.offsets[batch.lastRecord+1]
					readSize := readEnd - readStart

					var readBuf []byte
					if readSize <= int64(len(buf)) {
						readBuf = buf[:readSize]
					} else {
						readBuf = make([]byte, readSize)
					}

					if _, err := r.file.ReadAt(readBuf, readStart); err != nil {
						resultCh <- batchResult{batchIdx: bi, err: err}
						return
					}

					// Decode records on demand, copy requested items.
					items := make([][]byte, 0, batch.idxEnd-batch.idxStart)
					prevRec := -1
					for k := batch.idxStart; k < batch.idxEnd; k++ {
						rec, localIdx := indices[k]/r.recordSize, indices[k]%r.recordSize
						if rec != prevRec {
							recOff := r.offsets[rec] - readStart
							recEnd := r.offsets[rec+1] - readStart
							if err := rd.Decode(readBuf[recOff:recEnd], rec); err != nil {
								resultCh <- batchResult{batchIdx: bi, err: err}
								return
							}
							prevRec = rec
						}
						items = append(items, rd.EntryCopy(localIdx))
					}
					resultCh <- batchResult{batchIdx: bi, items: items}
				}
			}()
		}
		go func() { wg.Wait(); close(resultCh) }()

		// Consumer: inline reorder at batch granularity.
		pending := make(map[int]batchResult)
		nextIdx := 0
		for res := range resultCh {
			if res.err != nil {
				cancel()
				yield(nil, res.err)
				for range resultCh {} // drain so workers can exit
				return
			}
			pending[res.batchIdx] = res
			for br, ok := pending[nextIdx]; ok; br, ok = pending[nextIdx] {
				delete(pending, nextIdx)
				for _, item := range br.items {
					if !yield(item, nil) {
						cancel()
						for range resultCh {} // drain so workers can exit
						return
					}
				}
				nextIdx++
			}
		}
	}
}

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error) {
	if err := r.waitOpen(); err != nil {
		return [32]byte{}, false, err
	}
	return r.trailer.ContentHash, r.trailer.HasContentHash, nil
}

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error) {
	if err := r.waitOpen(); err != nil {
		return nil, err
	}
	return r.appData, nil
}

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches.
func (r *Reader) Verify(ctx context.Context) error {
	if err := r.waitOpen(); err != nil {
		return err
	}
	if !r.trailer.HasContentHash {
		return nil
	}

	hasher := newContentHasher(r.recordSize)
	i := 0
	for item, err := range r.ReadRange(0, r.totalItems) {
		if err != nil {
			return err
		}
		hasher.Add(item)
		i++
		if i%r.recordSize == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	computed := hasher.Sum()
	if computed != r.trailer.ContentHash {
		return fmt.Errorf("packfile: %w: expected %x, got %x",
			ErrContentHashMismatch, r.trailer.ContentHash, computed)
	}
	return nil
}

// Trailer returns the parsed trailer.
func (r *Reader) Trailer() (Trailer, error) {
	if err := r.waitOpen(); err != nil {
		return Trailer{}, err
	}
	return r.trailer, nil
}

// Close releases all resources. Safe to call multiple times.
// Must always be called, even if no query methods were called.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.once.Do(r.drain) // drain goroutine, populate all fields
		if r.file != nil {
			r.closeErr = r.file.Close()
		}
	})
	return r.closeErr
}
