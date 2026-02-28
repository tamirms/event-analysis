package packfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func writeTestPackfile(t *testing.T, records [][]byte, opts WriterOptions) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	return path
}

func makeRecords(n, size int) [][]byte {
	records := make([][]byte, n)
	for i := range records {
		rec := make([]byte, size)
		rand.Read(rec)
		records[i] = rec
	}
	return records
}

func TestRoundTrip(t *testing.T) {
	records := makeRecords(500, 1024)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != len(records) {
		t.Fatalf("RecordCount = %d, want %d", rc, len(records))
	}

	for i, want := range records {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestEmptyFile(t *testing.T) {
	path := writeTestPackfile(t, nil, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Fatalf("RecordCount = %d, want 0", rc)
	}

	_, err = r.ReadRecord(0, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(0) on empty: got %v, want ErrIndexRange", err)
	}
}

func TestSingleRecord(t *testing.T) {
	records := makeRecords(1, 256)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RecordCount = %d, want 1", rc)
	}

	got, err := r.ReadRecord(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, records[0]) {
		t.Fatal("data mismatch")
	}
}

func TestPartialLastGroup(t *testing.T) {
	// 200 records = 1 full group (128) + 72 in partial group.
	records := makeRecords(200, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 200 {
		t.Fatalf("RecordCount = %d, want 200", rc)
	}

	// Verify all records.
	for i, want := range records {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestLargeRecords(t *testing.T) {
	// Records > 1MB to exceed ReadRecords batch buffer.
	records := [][]byte{
		make([]byte, 2*1024*1024), // 2MB
		make([]byte, 500),         // small
		make([]byte, 1500*1024),   // 1.5MB
	}
	for _, r := range records {
		rand.Read(r)
	}

	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// Point reads.
	for i, want := range records {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}

	// ReadRecords with large records.
	j := 0
	for raw, err := range r.ReadRecords(0, 3) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[j]) {
			t.Fatalf("ReadRecords[%d]: data mismatch", j)
		}
		j++
	}
	if j != 3 {
		t.Fatalf("ReadRecords yielded %d records, want 3", j)
	}
}

func TestIndexIntegrity(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	// Read file, corrupt a byte in the index section, write back.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Index section starts after records, ends before metadata+trailer.
	// For 10 records of 100 bytes each, records end at byte 1000.
	// Flip a bit in the first byte after the records.
	indexStart := 10 * 100
	if indexStart < len(data)-trailerSize {
		data[indexStart] ^= 0xFF
	}

	corruptPath := filepath.Join(t.TempDir(), "corrupt.pack")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(corruptPath)
	defer r.Close()
	_, err = r.RecordCount()
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open corrupt index: got %v, want ErrChecksum", err)
	}
}

func TestTrailerIntegrity(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

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
	_, err = r.RecordCount()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open corrupt trailer: got %v, want ErrCorrupt", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	records := makeRecords(100, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	for i := range records {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			got, err := r.ReadRecord(idx, nil)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, records[idx]) {
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

func TestReadRecordsIterator(t *testing.T) {
	records := makeRecords(50, 2048)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// Full range.
	j := 0
	for raw, err := range r.ReadRecords(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[j]) {
			t.Fatalf("ReadRecords[%d]: data mismatch", j)
		}
		j++
	}
	if j != 50 {
		t.Fatalf("ReadRecords yielded %d records, want 50", j)
	}

	// Partial range.
	j = 0
	for raw, err := range r.ReadRecords(10, 5) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, records[10+j]) {
			t.Fatalf("ReadRecords partial [%d]: data mismatch", j)
		}
		j++
	}
	if j != 5 {
		t.Fatalf("ReadRecords partial yielded %d, want 5", j)
	}

	// Early break.
	j = 0
	for _, err := range r.ReadRecords(0, 50) {
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
	for _, err := range r.ReadRecords(0, 0) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("Empty ReadRecords yielded %d, want 0", j)
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
	if _, err := w.Finish(nil); err != nil {
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

func TestMetadataRoundTrip(t *testing.T) {
	meta := []byte("chunk-meta:version=1,first_ledger=420000")
	records := makeRecords(5, 100)

	// Write with metadata passed to Finish.
	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(meta); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()

	got, err := r.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, meta) {
		t.Fatalf("Metadata mismatch: got %q, want %q", got, meta)
	}
}

func TestVariableSizeRecords(t *testing.T) {
	// Records with varying sizes to exercise FOR compression.
	records := make([][]byte, 300)
	for i := range records {
		size := 5000 + (i % 200) // 5000-5199 bytes
		rec := make([]byte, size)
		rand.Read(rec)
		records[i] = rec
	}

	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	for i, want := range records {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch (len got=%d, want=%d)", i, len(got), len(want))
		}
	}
}

func TestUniformSizeRecords(t *testing.T) {
	// All records same size — exercises width=0→1 path.
	records := makeRecords(256, 1000)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	for i, want := range records {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestReadRecordOutOfRange(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	_, err := r.ReadRecord(-1, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(-1): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadRecord(5, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(5): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadRecord(100, nil)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadRecord(100): got %v, want ErrIndexRange", err)
	}
}

func TestReadRecordsPanic(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	assertPanics := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		f()
	}

	assertPanics("negative index", func() { for range r.ReadRecords(-1, 1) {} })
	assertPanics("negative count", func() { for range r.ReadRecords(0, -1) {} })
	assertPanics("out of range", func() { for range r.ReadRecords(3, 5) {} })
}

func TestSpeculativeReadFallback(t *testing.T) {
	// Create enough small records so the FOR index exceeds 256KB,
	// forcing Open to fall back to a separate read for the index.
	// Each FOR group (128 records) uses 5 + (w*128+7)/8 bytes.
	// With sizes 10-59, w=6 → ~101 bytes/group.
	// 256KB / 101 ≈ 2596 groups → ~332K records to exceed.
	n := 350_000
	records := make([][]byte, n)
	for i := range records {
		size := 10 + (i % 50) // variable sizes to exercise FOR encoding
		rec := make([]byte, size)
		for j := range rec {
			rec[j] = byte(i + j)
		}
		records[i] = rec
	}

	path := filepath.Join(t.TempDir(), "test.pack")
	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish([]byte("large-index-test")); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != n {
		t.Fatalf("RecordCount = %d, want %d", rc, n)
	}

	meta, err := r.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(meta, []byte("large-index-test")) {
		t.Fatalf("Metadata mismatch")
	}

	// Verify the index actually exceeds speculative read size.
	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.IndexSize <= 256*1024 {
		t.Fatalf("IndexSize = %d, expected > 256KB to exercise fallback", trailer.IndexSize)
	}

	// Verify all records via ReadRecords iterator.
	j := 0
	for got, err := range r.ReadRecords(0, n) {
		if err != nil {
			t.Fatalf("ReadRecords[%d]: %v", j, err)
		}
		if !bytes.Equal(got, records[j]) {
			t.Fatalf("ReadRecords[%d]: data mismatch (got %d bytes, want %d)", j, len(got), len(records[j]))
		}
		j++
	}
	if j != n {
		t.Fatalf("ReadRecords yielded %d, want %d", j, n)
	}
}

func TestSpeculativeReadFallbackNoMetadata(t *testing.T) {
	// Same as above but with no metadata — exercises the fallback path
	// where the index overshoot bytes are zeros (from make) rather than
	// metadata bytes.
	n := 350_000
	records := make([][]byte, n)
	for i := range records {
		size := 10 + (i % 50)
		rec := make([]byte, size)
		for j := range rec {
			rec[j] = byte(i + j)
		}
		records[i] = rec
	}

	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	rc, err := r.RecordCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != n {
		t.Fatalf("RecordCount = %d, want %d", rc, n)
	}

	meta, err := r.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 0 {
		t.Fatalf("Metadata should be empty, got %d bytes", len(meta))
	}

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if trailer.IndexSize <= 256*1024 {
		t.Fatalf("IndexSize = %d, expected > 256KB to exercise fallback", trailer.IndexSize)
	}

	// Spot-check records near group boundaries (128-record groups in FOR index).
	for _, i := range []int{0, 1, 127, 128, 129, 255, 256, n/2, n - 2, n - 1} {
		got, err := r.ReadRecord(i, nil)
		if err != nil {
			t.Fatalf("ReadRecord(%d): %v", i, err)
		}
		if !bytes.Equal(got, records[i]) {
			t.Fatalf("ReadRecord(%d): data mismatch", i)
		}
	}
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/file.pack")
	defer r.Close()

	_, err := r.ReadRecord(0, nil)
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestCloseBeforeRead(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	records := makeRecords(5, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	err1 := r.Close()
	err2 := r.Close()
	if err1 != err2 {
		t.Fatalf("double Close: first=%v, second=%v", err1, err2)
	}
}

// --- partitionRuns tests ---

func TestPartitionRuns(t *testing.T) {
	tests := []struct {
		name      string
		indices   []int
		maxRunLen int
		want      []workItem
	}{
		{"empty", nil, 100, nil},
		{"single", []int{5}, 100, []workItem{{0, 5, 1}}},
		{"all_consecutive", []int{3, 4, 5, 6, 7}, 100, []workItem{{0, 3, 5}}},
		{"all_scattered", []int{0, 5, 10, 20}, 100, []workItem{
			{0, 0, 1}, {1, 5, 1}, {2, 10, 1}, {3, 20, 1},
		}},
		{"mixed", []int{0, 1, 2, 5, 6, 10}, 100, []workItem{
			{0, 0, 3}, {3, 5, 2}, {5, 10, 1},
		}},
		{"two_elements_consecutive", []int{7, 8}, 100, []workItem{{0, 7, 2}}},
		{"two_elements_scattered", []int{3, 9}, 100, []workItem{{0, 3, 1}, {1, 9, 1}}},
		// Run splitting: 10 consecutive indices split into chunks of 3.
		{"split_consecutive", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 3, []workItem{
			{0, 0, 3}, {3, 3, 3}, {6, 6, 3}, {9, 9, 1},
		}},
		// Run splitting: only long runs are split, short runs and isolated indices are unchanged.
		{"split_mixed", []int{0, 1, 2, 3, 4, 10, 11, 20}, 3, []workItem{
			{0, 0, 3}, {3, 3, 2}, {5, 10, 2}, {7, 20, 1},
		}},
		// maxRunLen=1 degenerates to all-scattered.
		{"split_max1", []int{0, 1, 2}, 1, []workItem{
			{0, 0, 1}, {1, 1, 1}, {2, 2, 1},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partitionRuns(tt.indices, tt.maxRunLen)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("item[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// --- ReadScattered tests ---

func TestReadScatteredAllConsecutive(t *testing.T) {
	records := makeRecords(100, 1024)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	indices := make([]int, 50)
	for i := range indices {
		indices[i] = 10 + i // 10..59
	}

	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 4,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d: data mismatch", idx)
		}
	}
}

func TestReadScatteredMixed(t *testing.T) {
	records := makeRecords(100, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// Mix of consecutive runs and isolated indices.
	indices := []int{0, 1, 2, 10, 20, 21, 22, 23, 50, 99}

	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 4,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d (pos %d): data mismatch", idx, i)
		}
	}
}

func TestReadScatteredAllScattered(t *testing.T) {
	records := makeRecords(100, 256)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	indices := []int{0, 10, 30, 50, 70, 99}

	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 4,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d (pos %d): data mismatch", idx, i)
		}
	}
}

func TestReadScatteredSingle(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	var got []byte
	err := r.ReadScattered(context.Background(), []int{5}, 4,
		func(inputPos int, data []byte) error {
			got = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, records[5]) {
		t.Fatal("data mismatch")
	}
}

func TestReadScatteredEmpty(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	err := r.ReadScattered(context.Background(), nil, 4,
		func(inputPos int, data []byte) error {
			t.Fatal("should not be called")
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	err = r.ReadScattered(context.Background(), []int{}, 4,
		func(inputPos int, data []byte) error {
			t.Fatal("should not be called")
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadScatteredHighConcurrency(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	indices := []int{1, 5, 8}
	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 100,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d: data mismatch", idx)
		}
	}
}

func TestReadScatteredConcurrency(t *testing.T) {
	records := makeRecords(100, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// All scattered so each is a separate work item.
	indices := make([]int, 20)
	for i := range indices {
		indices[i] = i * 5
	}

	// Verify that all records are read correctly with concurrency.
	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 4,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d (pos %d): data mismatch", idx, i)
		}
	}
}

func TestReadScatteredContextCancel(t *testing.T) {
	records := makeRecords(100, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	indices := make([]int, 50)
	for i := range indices {
		indices[i] = i * 2
	}

	err := r.ReadScattered(ctx, indices, 4,
		func(inputPos int, data []byte) error {
			return nil
		})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestReadScatteredProcessError(t *testing.T) {
	records := makeRecords(100, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	// All scattered to maximize work items.
	indices := make([]int, 50)
	for i := range indices {
		indices[i] = i * 2
	}

	sentinel := fmt.Errorf("process error")
	err := r.ReadScattered(context.Background(), indices, 1,
		func(inputPos int, data []byte) error {
			if inputPos == 3 {
				return sentinel
			}
			return nil
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want sentinel error", err)
	}
}

func TestReadScatteredOutOfRange(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	err := r.ReadScattered(context.Background(), []int{10}, 1,
		func(inputPos int, data []byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("got %v, want ErrIndexRange", err)
	}

	err = r.ReadScattered(context.Background(), []int{-1}, 1,
		func(inputPos int, data []byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("got %v, want ErrIndexRange for -1", err)
	}
}

func TestReadScatteredUnsortedPanic(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsorted indices")
		}
	}()

	r.ReadScattered(context.Background(), []int{5, 3}, 1,
		func(inputPos int, data []byte) error { return nil })
}

func TestReadScatteredDuplicatePanic(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate indices")
		}
	}()

	r.ReadScattered(context.Background(), []int{3, 3}, 1,
		func(inputPos int, data []byte) error { return nil })
}

func TestReadScatteredZeroConcurrency(t *testing.T) {
	records := makeRecords(10, 100)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	indices := []int{0, 3, 7}
	results := make([][]byte, len(indices))
	// concurrency=0 should be clamped to 1, not panic.
	err := r.ReadScattered(context.Background(), indices, 0,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d: data mismatch", idx)
		}
	}
}

func TestReadScatteredCrossFORGroupBoundary(t *testing.T) {
	// 300 records spans multiple FOR groups (each 128 records).
	// Consecutive range 120..179 crosses the group boundary at 128.
	records := makeRecords(300, 512)
	path := writeTestPackfile(t, records, WriterOptions{})

	r := Open(path)
	defer r.Close()

	indices := make([]int, 60)
	for i := range indices {
		indices[i] = 120 + i // 120..179, crosses boundary at 128
	}

	results := make([][]byte, len(indices))
	err := r.ReadScattered(context.Background(), indices, 4,
		func(inputPos int, data []byte) error {
			results[inputPos] = append([]byte(nil), data...)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	for i, idx := range indices {
		if !bytes.Equal(results[i], records[idx]) {
			t.Fatalf("index %d (pos %d): data mismatch", idx, i)
		}
	}
}
