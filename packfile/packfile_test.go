package packfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func writeTestPackfile(t *testing.T, items [][]byte, opts WriterOptions) string {
	t.Helper()
	if opts.RecordSize == 0 {
		opts.RecordSize = 1 // one item per record = raw record behavior
	}
	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := w.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	return path
}

func makeItems(n, size int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		item := make([]byte, size)
		rand.Read(item)
		items[i] = item
	}
	return items
}

func readItemCopy(t *testing.T, r *Reader, index int) []byte {
	t.Helper()
	var got []byte
	if err := r.ReadItem(index, func(entry []byte) error {
		got = append([]byte(nil), entry...)
		return nil
	}); err != nil {
		t.Fatalf("ReadItem(%d): %v", index, err)
	}
	return got
}

func TestRoundTrip(t *testing.T) {
	items := makeItems(500, 1024)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != len(items) {
		t.Fatalf("TotalItems = %d, want %d", tc, len(items))
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestEmptyFile(t *testing.T) {
	path := writeTestPackfile(t, nil, WriterOptions{})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 0 {
		t.Fatalf("TotalItems = %d, want 0", tc)
	}

	err = r.ReadItem(0, func([]byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadItem(0) on empty: got %v, want ErrIndexRange", err)
	}
}

func TestSingleItem(t *testing.T) {
	items := makeItems(1, 256)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 1 {
		t.Fatalf("TotalItems = %d, want 1", tc)
	}

	if got := readItemCopy(t, r, 0); !bytes.Equal(got, items[0]) {
		t.Fatal("data mismatch")
	}
}

func TestRecordSize1NoTrailingWaste(t *testing.T) {
	// With RecordSize=1 and Uncompressed format, each record should be exactly
	// itemSize + 4 (CRC32C). With trailing FOR it would be itemSize + 6 + 4.
	// Verify the on-disk record size to confirm the trailing group is skipped.
	const itemSize = 256
	items := makeItems(10, itemSize)

	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 1, Format: Uncompressed})

	r := Open(path)
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.RecordCount != 10 {
		t.Fatalf("RecordCount = %d, want 10", trailer.RecordCount)
	}

	// Each uncompressed record: item (256B) + CRC32C (4B) = 260B.
	// If trailing FOR were present it would be 256 + 6 + 4 = 266B.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	dataSize := fi.Size() - int64(trailer.IndexSize) - int64(trailer.AppDataSize) - trailerSize
	perRecord := dataSize / int64(trailer.RecordCount)
	wantPerRecord := int64(itemSize + 4) // item + CRC32C, no trailing FOR
	if perRecord != wantPerRecord {
		t.Fatalf("per-record size = %d, want %d (no trailing FOR group)", perRecord, wantPerRecord)
	}

	// Verify roundtrip.
	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestRawFormat(t *testing.T) {
	// With Raw format, records are stored as-is: no CRC, no compression.
	// Per-record size should be exactly itemSize (no CRC overhead).
	const itemSize = 256
	items := makeItems(10, itemSize)

	path := writeTestPackfile(t, items, WriterOptions{
		RecordSize: 1,
		Format:     Raw,
	})

	r := Open(path)
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.Format != Raw {
		t.Fatalf("trailer.Format = %v, want Raw", trailer.Format)
	}

	// Each raw record: item (256B) only. No CRC32C (4B) overhead.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	dataSize := fi.Size() - int64(trailer.IndexSize) - int64(trailer.AppDataSize) - trailerSize
	perRecord := dataSize / int64(trailer.RecordCount)
	wantPerRecord := int64(itemSize) // raw: no CRC
	if perRecord != wantPerRecord {
		t.Fatalf("per-record size = %d, want %d (raw, no CRC)", perRecord, wantPerRecord)
	}

	// Verify roundtrip.
	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestMultiItemRecords(t *testing.T) {
	// 300 items with recordSize=128 = 2 full records + 44 in partial.
	items := makeItems(300, 512)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 128})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 300 {
		t.Fatalf("TotalItems = %d, want 300", tc)
	}

	// Verify all items.
	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestReadRange(t *testing.T) {
	items := makeItems(50, 2048)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 10})

	r := Open(path)
	defer r.Close()

	// Full range.
	j := 0
	for item, err := range r.ReadRange(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[j]) {
			t.Fatalf("ReadRange[%d]: data mismatch", j)
		}
		j++
	}
	if j != 50 {
		t.Fatalf("ReadRange yielded %d items, want 50", j)
	}

	// Partial range crossing record boundary.
	j = 0
	for item, err := range r.ReadRange(8, 5) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[8+j]) {
			t.Fatalf("ReadRange partial [%d]: data mismatch", j)
		}
		j++
	}
	if j != 5 {
		t.Fatalf("ReadRange partial yielded %d, want 5", j)
	}

	// Early break.
	j = 0
	for _, err := range r.ReadRange(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		j++
		if j == 3 {
			break
		}
	}
	if j != 3 {
		t.Fatalf("Early break: got %d iterations, want 3", j)
	}

	// Empty range.
	j = 0
	for _, err := range r.ReadRange(0, 0) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("Empty ReadRange yielded %d, want 0", j)
	}
}

func TestReadItems(t *testing.T) {
	items := makeItems(300, 512)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 128})

	r := Open(path)
	defer r.Close()

	// Indices spanning multiple records.
	indices := []int{0, 1, 127, 128, 200, 299}
	got := make([][]byte, len(indices))
	err := r.ReadItems(context.Background(), indices, func(pos int, entry []byte) error {
		got[pos] = append([]byte(nil), entry...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for j, idx := range indices {
		if !bytes.Equal(got[j], items[idx]) {
			t.Fatalf("ReadItems[%d] (item %d): data mismatch", j, idx)
		}
	}
}

func TestReadItemsDuplicatesPanic(t *testing.T) {
	items := makeItems(100, 100)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 128})

	r := Open(path)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate indices")
		}
	}()
	r.ReadItems(context.Background(), []int{5, 5, 10}, func(int, []byte) error { return nil })
}

func TestReadItemsUnsortedPanic(t *testing.T) {
	items := makeItems(300, 100)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 128})

	r := Open(path)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsorted indices")
		}
	}()
	r.ReadItems(context.Background(), []int{10, 5, 20}, func(int, []byte) error { return nil })
}

func TestReadItemsEmpty(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	called := false
	err := r.ReadItems(context.Background(), nil, func(int, []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("empty ReadItems should not call fn")
	}
}

func TestIndexIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	// Read file, corrupt a byte in the index section, write back.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Parse trailer to find indexBase.
	trailerStart := len(data) - trailerSize
	indexSize := int(binary.LittleEndian.Uint32(data[trailerStart+18:]))
	appDataSize := int(binary.LittleEndian.Uint32(data[trailerStart+22:]))
	indexStart := trailerStart - appDataSize - indexSize
	if indexStart >= 0 && indexStart < trailerStart {
		data[indexStart] ^= 0xFF
	}

	corruptPath := filepath.Join(t.TempDir(), "corrupt.pack")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath)
	defer r.Close()
	_, err = r.TotalItems()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open corrupt index: got %v, want ErrChecksum", err)
	}
}

func TestTrailerIntegrity(t *testing.T) {
	items := makeItems(10, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the magic bytes in the trailer.
	trailerStart := len(data) - trailerSize
	data[trailerStart] ^= 0xFF

	corruptPath := filepath.Join(t.TempDir(), "corrupt_trailer.pack")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath)
	defer r.Close()
	_, err = r.TotalItems()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupt trailer: got %v, want ErrCorrupt", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	items := makeItems(100, 512)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 128})

	r := Open(path)
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := range items {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var got []byte
			if err := r.ReadItem(idx, func(entry []byte) error {
				got = append([]byte(nil), entry...)
				return nil
			}); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, items[idx]) {
				errs <- errors.New("data mismatch")
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
}

func TestAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "atomic.pack")

	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// File should not exist at final path yet.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file exists at final path before Finish")
	}

	if err := w.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}

	// Now file should exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not at final path after Finish: %v", err)
	}
}

func TestAbortCleansUp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "abort.pack")

	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}

	tmpPath := w.tmpPath
	if err := w.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	// Tmp file should exist.
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("tmp file not found: %v", err)
	}

	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}

	// Tmp file should be gone.
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatal("tmp file still exists after Abort")
	}

	// Final path should not exist.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file exists at final path after Abort")
	}
}

func TestReadItemOutOfRange(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	err := r.ReadItem(-1, func([]byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadItem(-1): got %v, want ErrIndexRange", err)
	}

	err = r.ReadItem(5, func([]byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadItem(5): got %v, want ErrIndexRange", err)
	}

	err = r.ReadItem(100, func([]byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadItem(100): got %v, want ErrIndexRange", err)
	}
}

func TestReadRangePanic(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	assertPanics(t, "negative start", func() { for range r.ReadRange(-1, 1) {} })
	assertPanics(t, "negative count", func() { for range r.ReadRange(0, -1) {} })
	assertPanics(t, "out of range", func() { for range r.ReadRange(3, 5) {} })
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/file.pack")
	defer r.Close()

	err := r.ReadItem(0, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestCloseBeforeRead(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	err1 := r.Close()
	err2 := r.Close()
	if err1 != err2 {
		t.Fatalf("double Close: first=%v, second=%v", err1, err2)
	}
}

// --- Content Hash Tests ---

func TestContentHashRoundTrip(t *testing.T) {
	items := makeItems(500, 200)
	path := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r := Open(path)
	defer r.Close()

	hash, ok, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected content hash to be present")
	}
	if hash == ([32]byte{}) {
		t.Fatal("expected non-zero hash")
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestContentHashDeterministic(t *testing.T) {
	items := makeItems(500, 200)
	path1 := writeTestPackfile(t, items, WriterOptions{ContentHash: true})
	path2 := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r1 := Open(path1)
	defer r1.Close()
	r2 := Open(path2)
	defer r2.Close()

	hash1, _, err := r1.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	hash2, _, err := r2.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if hash1 != hash2 {
		t.Fatalf("hashes differ: %x vs %x", hash1, hash2)
	}
}

func TestContentHashDisabled(t *testing.T) {
	items := makeItems(100, 200)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	_, ok, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no content hash when disabled")
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify should return nil when no hash stored: %v", err)
	}
}

func TestContentHashCorruption(t *testing.T) {
	items := makeItems(200, 200)
	path := writeTestPackfile(t, items, WriterOptions{ContentHash: true})

	r := Open(path)
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify before corruption: %v", err)
	}
	r.Close()

	fileData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Content hash is at trailer bytes [26:58]. Corrupt it and recompute CRC
	// so the test validates hash mismatch, not CRC failure.
	trailerStart := len(fileData) - trailerSize
	fileData[trailerStart+26] ^= 0xFF

	// Recompute trailer CRC over trailer[0:60] only.
	binary.LittleEndian.PutUint32(fileData[trailerStart+60:],
		CRC32C(fileData[trailerStart:trailerStart+60]))

	corruptedPath := filepath.Join(t.TempDir(), "corrupted.pack")
	if err := os.WriteFile(corruptedPath, fileData, 0644); err != nil {
		t.Fatal(err)
	}

	r = Open(corruptedPath)
	defer r.Close()

	verifyErr := r.Verify(context.Background())
	if verifyErr == nil {
		t.Fatal("expected error from corrupted hash")
	}
	if !errors.Is(verifyErr, ErrContentHashMismatch) {
		t.Fatalf("expected ErrContentHashMismatch, got: %v", verifyErr)
	}
}

func TestContentHashWithConcurrency(t *testing.T) {
	items := makeItems(500, 200)

	serialPath := writeTestPackfile(t, items, WriterOptions{ContentHash: true})
	parallelPath := writeTestPackfile(t, items, WriterOptions{ContentHash: true, Concurrency: 4})

	r1 := Open(serialPath)
	defer r1.Close()
	r2 := Open(parallelPath)
	defer r2.Close()

	hash1, _, err := r1.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	hash2, _, err := r2.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if hash1 != hash2 {
		t.Fatalf("serial vs parallel hashes differ: %x vs %x", hash1, hash2)
	}

	if err := r2.Verify(context.Background()); err != nil {
		t.Fatalf("Verify parallel: %v", err)
	}
}

func TestContentHashUncompressed(t *testing.T) {
	items := makeItems(500, 200)
	path := writeTestPackfile(t, items, WriterOptions{ContentHash: true, Format: Uncompressed})

	r := Open(path)
	defer r.Close()

	hash, ok, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected content hash to be present")
	}
	if hash == ([32]byte{}) {
		t.Fatal("expected non-zero hash")
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestUncompressedRoundTrip(t *testing.T) {
	items := makeItems(500, 200)
	path := writeTestPackfile(t, items, WriterOptions{Format: Uncompressed, Concurrency: 4})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 500 {
		t.Fatalf("TotalItems = %d, want 500", tc)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestParallelCompressionRoundTrip(t *testing.T) {
	items := makeItems(500, 200)
	path := writeTestPackfile(t, items, WriterOptions{Concurrency: 4})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 500 {
		t.Fatalf("TotalItems = %d, want 500", tc)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestSmallRecordSize(t *testing.T) {
	items := makeItems(20, 200)
	path := writeTestPackfile(t, items, WriterOptions{RecordSize: 3})

	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 20 {
		t.Fatalf("TotalItems = %d, want 20", tc)
	}

	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestContentHashNonDefaultRecordSize(t *testing.T) {
	items := makeItems(500, 200)

	for _, recordSize := range []int{64, 256, 500} {
		t.Run(fmt.Sprintf("RecordSize=%d", recordSize), func(t *testing.T) {
			path := writeTestPackfile(t, items, WriterOptions{RecordSize: recordSize, ContentHash: true})

			r := Open(path)
			defer r.Close()

			hash, ok, err := r.ContentHash()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("expected content hash to be present")
			}
			if hash == ([32]byte{}) {
				t.Fatal("expected non-zero hash")
			}

			if err := r.Verify(context.Background()); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

func TestFileTooShort(t *testing.T) {
	// A file shorter than 64 bytes (trailer size) must trigger ErrSize.
	dir := t.TempDir()
	path := filepath.Join(dir, "short.pack")

	if err := os.WriteFile(path, make([]byte, 63), 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()

	_, err := r.TotalItems()
	if !errors.Is(err, ErrSize) {
		t.Fatalf("expected ErrSize for short file, got %v", err)
	}
}

func TestTrailer(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	// With RecordSize=1, each item = one record.
	if trailer.RecordCount != 5 {
		t.Fatalf("RecordCount = %d, want 5", trailer.RecordCount)
	}
	if trailer.TotalItems != 5 {
		t.Fatalf("TotalItems = %d, want 5", trailer.TotalItems)
	}
	if trailer.RecordSize != 1 {
		t.Fatalf("RecordSize = %d, want 1", trailer.RecordSize)
	}
	if trailer.AppDataSize != 0 {
		t.Fatalf("AppDataSize = %d, want 0", trailer.AppDataSize)
	}
	if trailer.Format != Compressed {
		t.Fatalf("expected Format=Compressed, got %v", trailer.Format)
	}
	if trailer.HasContentHash {
		t.Fatal("expected HasContentHash=false")
	}
}

func TestAppDataRoundTrip(t *testing.T) {
	appData := []byte("hello-app-data-1234567890")

	dir := t.TempDir()
	path := filepath.Join(dir, "appdata.pack")

	w, err := Create(path, WriterOptions{RecordSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	items := makeItems(5, 100)
	for _, item := range items {
		if err := w.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, appData) {
		t.Fatalf("AppData mismatch: got %q, want %q", got, appData)
	}

	// Verify items still readable.
	for i, want := range items {
		if got := readItemCopy(t, r, i); !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.AppDataSize != uint32(len(appData)) {
		t.Fatalf("trailer.AppDataSize = %d, want %d", trailer.AppDataSize, len(appData))
	}
}

func TestAppDataCorruption(t *testing.T) {
	// App data has no packfile-level integrity protection — the trailer CRC covers
	// only trailer[0:60]. Corruption in app data is undetected by packfile; callers
	// are responsible for their own app data integrity checks.
	appData := []byte("important-metadata")

	dir := t.TempDir()
	path := filepath.Join(dir, "appdata.pack")

	w, err := Create(path, WriterOptions{RecordSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("item")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}

	fileData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the app data section.
	trailerStart := len(fileData) - trailerSize
	appDataSz := int(binary.LittleEndian.Uint32(fileData[trailerStart+22:]))
	if appDataSz == 0 {
		t.Fatal("expected non-zero appDataSize")
	}
	fileData[trailerStart-appDataSz] ^= 0xFF

	corruptedPath := filepath.Join(t.TempDir(), "corrupted.pack")
	if err := os.WriteFile(corruptedPath, fileData, 0644); err != nil {
		t.Fatal(err)
	}

	// Packfile opens successfully — app data corruption is not detected.
	r := Open(corruptedPath)
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatalf("unexpected error opening file with corrupted app data: %v", err)
	}
	if string(got) == string(appData) {
		t.Error("expected corrupted app data to differ from original")
	}
}

func TestAppDataEmpty(t *testing.T) {
	items := makeItems(5, 100)
	path := writeTestPackfile(t, items, WriterOptions{})

	r := Open(path)
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil app data, got %d bytes", len(got))
	}
}
