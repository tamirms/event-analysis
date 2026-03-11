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

func mustTotalItems(t *testing.T, ir ItemReader) int {
	t.Helper()
	n, err := ir.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLiveRoundTrip(t *testing.T) {
	items := makeItems(300, 200)
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 128, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	if n := mustTotalItems(t, lw); n != len(items) {
		t.Fatalf("TotalItems = %d, want %d", n, len(items))
	}

	for i, want := range items {
		if err := lw.ReadItem(i, func(got []byte) error {
			if !bytes.Equal(got, want) {
				return errors.New("data mismatch")
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadItem(%d): %v", i, err)
		}
	}

	// Verify ReadRange.
	j := 0
	for item, err := range lw.ReadRange(0, len(items)) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[j]) {
			t.Fatalf("ReadRange[%d]: data mismatch", j)
		}
		j++
	}
	if j != len(items) {
		t.Fatalf("ReadRange yielded %d, want %d", j, len(items))
	}
}

func TestLivePendingOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 128})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(50, 100)
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	if n := mustTotalItems(t, lw); n != 50 {
		t.Fatalf("TotalItems = %d, want 50", n)
	}

	for i, want := range items {
		if err := lw.ReadItem(i, func(got []byte) error {
			if !bytes.Equal(got, want) {
				return errors.New("data mismatch")
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadItem(%d): %v", i, err)
		}
	}

	// ReadRange over pending-only.
	j := 0
	for item, err := range lw.ReadRange(0, 50) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[j]) {
			t.Fatalf("ReadRange[%d]: data mismatch", j)
		}
		j++
	}
}

func TestLiveMixedFlushedAndPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(25, 100) // 2 full records (20 items) + 5 pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	if n := mustTotalItems(t, lw); n != 25 {
		t.Fatalf("TotalItems = %d, want 25", n)
	}

	// ReadRange spanning flushed and pending boundary.
	j := 0
	for item, err := range lw.ReadRange(15, 10) { // 15..24: 5 flushed + 5 pending
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[15+j]) {
			t.Fatalf("ReadRange[%d]: data mismatch", j)
		}
		j++
	}
	if j != 10 {
		t.Fatalf("ReadRange yielded %d, want 10", j)
	}
}

func TestLiveFreezeAndVerify(t *testing.T) {
	for _, format := range []RecordFormat{Compressed, Uncompressed, Raw} {
		t.Run(format.String(), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "live.pack")

			lw, err := CreateLive(path, WriterOptions{
				RecordSize:  128,
				Format:      format,
				ContentHash: true,
			})
			if err != nil {
				t.Fatal(err)
			}

			items := makeItems(300, 200)
			for _, item := range items {
				if err := lw.Append(item); err != nil {
					t.Fatal(err)
				}
			}

			if err := lw.Freeze(nil); err != nil {
				t.Fatal(err)
			}

			// Open as standard packfile and verify.
			r := Open(path)
			defer r.Close()

			tc, err := r.TotalItems()
			if err != nil {
				t.Fatal(err)
			}
			if tc != len(items) {
				t.Fatalf("TotalItems = %d, want %d", tc, len(items))
			}

			if err := r.Verify(context.Background()); err != nil {
				t.Fatalf("Verify: %v", err)
			}

			for i, want := range items {
				got := readItemCopy(t, r, i)
				if !bytes.Equal(got, want) {
					t.Fatalf("ReadItem(%d): data mismatch", i)
				}
			}
		})
	}
}

func TestLiveRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(25, 100) // 2 full records + 5 pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Only flushed records survive: 20 items.
	if cp.TotalItems() != 20 {
		t.Fatalf("checkpoint TotalItems = %d, want 20", cp.TotalItems())
	}

	// Simulate crash: close without freeze.
	lw.w.file.Close()

	// Recover.
	lw2, err := OpenLive(path, cp, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	// Verify recovered items are readable.
	if n := mustTotalItems(t, lw2); n != 20 {
		t.Fatalf("recovered TotalItems = %d, want 20", n)
	}
	for i, want := range items[:20] {
		if err := lw2.ReadItem(i, func(got []byte) error {
			if !bytes.Equal(got, want) {
				return errors.New("data mismatch")
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadItem(%d) after recovery: %v", i, err)
		}
	}

	// Re-append the lost items.
	for _, item := range items[20:] {
		if err := lw2.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	// Append more items.
	extra := makeItems(5, 100)
	for _, item := range extra {
		if err := lw2.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	allItems := append(items, extra...)

	if err := lw2.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	// Verify via Reader.
	r := Open(path)
	defer r.Close()

	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != len(allItems) {
		t.Fatalf("TotalItems = %d, want %d", tc, len(allItems))
	}

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	for i, want := range allItems {
		got := readItemCopy(t, r, i)
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestLiveRecoveryTornData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(20, 100) // 2 full records, no pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate torn write: append garbage after the checkpoint.
	garbage := make([]byte, 500)
	rand.Read(garbage)
	lw.w.file.Write(garbage)
	lw.w.file.Close()

	// Recover — truncation should remove the garbage.
	lw2, err := OpenLive(path, cp, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	if err := lw2.Freeze(nil); err != nil {
		t.Fatal(err)
	}

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
		got := readItemCopy(t, r, i)
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadItem(%d): data mismatch", i)
		}
	}
}

func TestLiveRecoveryMismatchedOpts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(10, 100)
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}
	lw.w.file.Close()

	// Wrong RecordSize.
	_, err = OpenLive(path, cp, WriterOptions{RecordSize: 20})
	if err == nil {
		t.Fatal("expected error for mismatched RecordSize")
	}

	// Wrong Format.
	_, err = OpenLive(path, cp, WriterOptions{RecordSize: 10, Format: Uncompressed})
	if err == nil {
		t.Fatal("expected error for mismatched Format")
	}
}

func TestLiveConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(50, 100)
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n, _ := lw.TotalItems(); n != 50 {
				errs <- errors.New("wrong TotalItems")
				return
			}
			for i := range items {
				if err := lw.ReadItem(i, func(got []byte) error {
					if !bytes.Equal(got, items[i]) {
						return errors.New("data mismatch")
					}
					return nil
				}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestLiveEmptyReads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 128})
	if err != nil {
		t.Fatal(err)
	}

	if n := mustTotalItems(t, lw); n != 0 {
		t.Fatalf("TotalItems = %d, want 0", n)
	}

	err = lw.ReadItem(0, func([]byte) error { return nil })
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadItem(0) on empty: got %v, want ErrIndexRange", err)
	}

	// Freeze empty.
	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()
	tc, err := r.TotalItems()
	if err != nil {
		t.Fatal(err)
	}
	if tc != 0 {
		t.Fatalf("TotalItems after freeze = %d, want 0", tc)
	}
}


func TestLiveRecordSize1(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 1, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(10, 100)
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	// With RecordSize=1, every Append flushes. All items are flushed.
	if n := mustTotalItems(t, lw); n != 10 {
		t.Fatalf("TotalItems = %d, want 10", n)
	}

	for i, want := range items {
		if err := lw.ReadItem(i, func(got []byte) error {
			if !bytes.Equal(got, want) {
				return errors.New("data mismatch")
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadItem(%d): %v", i, err)
		}
	}

	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestLiveContentHashSurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(20, 100) // 2 full records
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}
	lw.w.file.Close()

	// Recover and append more.
	lw2, err := OpenLive(path, cp, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	extra := makeItems(10, 100)
	for _, item := range extra {
		if err := lw2.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	if err := lw2.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	// Write the same items via standard Writer and compare content hashes.
	allItems := append(items, extra...)
	refPath := filepath.Join(dir, "ref.pack")
	w, err := Create(refPath, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range allItems {
		if err := w.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}

	r1 := Open(path)
	defer r1.Close()
	r2 := Open(refPath)
	defer r2.Close()

	h1, ok1, err := r1.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	h2, ok2, err := r2.ContentHash()
	if err != nil {
		t.Fatal(err)
	}

	if !ok1 || !ok2 {
		t.Fatal("expected content hash present")
	}
	if h1 != h2 {
		t.Fatalf("content hash mismatch after recovery: %x != %x", h1, h2)
	}

	if err := r1.Verify(context.Background()); err != nil {
		t.Fatalf("Verify recovered: %v", err)
	}
}

func TestLiveAllFormats(t *testing.T) {
	for _, format := range []RecordFormat{Compressed, Uncompressed, Raw} {
		for _, rs := range []int{1, 10, 128} {
			t.Run(fmt.Sprintf("%s/rs=%d", format, rs), func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "live.pack")

				lw, err := CreateLive(path, WriterOptions{
					RecordSize:  rs,
					Format:      format,
					ContentHash: true,
				})
				if err != nil {
					t.Fatal(err)
				}

				items := makeItems(50, 100)
				for _, item := range items {
					if err := lw.Append(item); err != nil {
						t.Fatal(err)
					}
				}

				for i, want := range items {
					if err := lw.ReadItem(i, func(got []byte) error {
						if !bytes.Equal(got, want) {
							return errors.New("data mismatch")
						}
						return nil
					}); err != nil {
						t.Fatalf("ReadItem(%d): %v", i, err)
					}
				}

				if err := lw.Freeze(nil); err != nil {
					t.Fatal(err)
				}

				r := Open(path)
				defer r.Close()
				if err := r.Verify(context.Background()); err != nil {
					t.Fatalf("Verify: %v", err)
				}
			})
		}
	}
}

func TestLiveReadItems(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(25, 100) // 2 full records + 5 pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	// Indices spanning flushed and pending.
	indices := []int{0, 5, 19, 20, 24}
	got := make([][]byte, len(indices))
	err = lw.ReadItems(context.Background(), indices, func(pos int, entry []byte) error {
		got[pos] = bytes.Clone(entry)
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

func TestLiveReadAfterFreeze(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(25, 100) // 2 full records + 5 pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	// All items (including previously pending) should be readable after Freeze.
	if n := mustTotalItems(t, lw); n != 25 {
		t.Fatalf("TotalItems after Freeze = %d, want 25", n)
	}

	for i, want := range items {
		if err := lw.ReadItem(i, func(got []byte) error {
			if !bytes.Equal(got, want) {
				return errors.New("data mismatch")
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadItem(%d) after Freeze: %v", i, err)
		}
	}

	// ReadRange should also work.
	j := 0
	for item, err := range lw.ReadRange(0, 25) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item, items[j]) {
			t.Fatalf("ReadRange[%d] after Freeze: data mismatch", j)
		}
		j++
	}

	// Close, then reads should fail.
	if err := lw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lw.ReadItem(0, func([]byte) error { return nil }); err == nil {
		t.Fatal("expected error reading after Close")
	}
}

func TestLiveClose(t *testing.T) {
	dir := t.TempDir()

	// Close without Freeze removes the file.
	t.Run("without_freeze", func(t *testing.T) {
		path := filepath.Join(dir, "no_freeze.pack")
		lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := lw.Append([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		if err := lw.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("file should be removed after Close without Freeze")
		}
	})

	// Close after Freeze keeps the file.
	t.Run("after_freeze", func(t *testing.T) {
		path := filepath.Join(dir, "frozen.pack")
		lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		items := makeItems(15, 100)
		for _, item := range items {
			if err := lw.Append(item); err != nil {
				t.Fatal(err)
			}
		}
		if err := lw.Freeze(nil); err != nil {
			t.Fatal(err)
		}
		if err := lw.Close(); err != nil {
			t.Fatal(err)
		}

		// File should still be readable via Open.
		r := Open(path)
		defer r.Close()
		tc, err := r.TotalItems()
		if err != nil {
			t.Fatal(err)
		}
		if tc != 15 {
			t.Fatalf("TotalItems via Reader = %d, want 15", tc)
		}
	})

	// Double Close is safe.
	t.Run("double_close", func(t *testing.T) {
		path := filepath.Join(dir, "double.pack")
		lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := lw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := lw.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLiveConcurrencyValidation(t *testing.T) {
	dir := t.TempDir()

	_, err := CreateLive(filepath.Join(dir, "a.pack"), WriterOptions{Concurrency: 4})
	if err == nil {
		t.Fatal("expected error for CreateLive with Concurrency > 0")
	}

	// OpenLive also rejects Concurrency > 0.
	path := filepath.Join(dir, "b.pack")
	lw, err := CreateLive(path, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range makeItems(10, 100) {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}

	_, err = OpenLive(path, cp, WriterOptions{RecordSize: 10, Concurrency: 4})
	if err == nil {
		t.Fatal("expected error for OpenLive with Concurrency > 0")
	}
}

func TestLiveContentHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	// No content hash when disabled.
	noHashPath := filepath.Join(dir, "nohash.pack")
	lwNoHash, err := CreateLive(noHashPath, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := lwNoHash.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected no content hash when disabled")
	}
	lwNoHash.Close()

	items := makeItems(25, 100) // 2 full records + 5 pending
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	// Mid-stream snapshot should be non-destructive (can call repeatedly).
	h1, ok1, err := lw.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !ok1 {
		t.Fatal("expected content hash present")
	}
	h2, _, err := lw.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatal("successive ContentHash calls returned different hashes")
	}

	// Append more and hash should change.
	if err := lw.Append([]byte("extra")); err != nil {
		t.Fatal(err)
	}
	h3, _, err := lw.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if h1 == h3 {
		t.Fatal("ContentHash should change after Append")
	}

	// Freeze and verify hash matches Reader.
	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}
	h4, ok4, err := lw.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !ok4 {
		t.Fatal("expected content hash after Freeze")
	}

	r := Open(path)
	defer r.Close()
	rHash, rOK, err := r.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	if !rOK {
		t.Fatal("Reader should have content hash")
	}
	if h4 != rHash {
		t.Fatalf("LiveWriter hash %x != Reader hash %x", h4, rHash)
	}
}

func TestLiveVerify(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	items := makeItems(25, 100)
	for _, item := range items {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}

	// Verify before Freeze should fail.
	if err := lw.Verify(context.Background()); err == nil {
		t.Fatal("expected error for Verify before Freeze")
	}

	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}

	// Verify after Freeze should succeed.
	if err := lw.Verify(context.Background()); err != nil {
		t.Fatalf("Verify after Freeze: %v", err)
	}

	// Verify with no content hash should be a no-op.
	noHashPath := filepath.Join(dir, "nohash.pack")
	lw2, err := CreateLive(noHashPath, WriterOptions{RecordSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := lw2.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := lw2.Freeze(nil); err != nil {
		t.Fatal(err)
	}
	if err := lw2.Verify(context.Background()); err != nil {
		t.Fatalf("Verify with no hash: %v", err)
	}
}

func TestLiveContentHashAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.pack")

	lw, err := CreateLive(path, WriterOptions{RecordSize: 10, ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range makeItems(10, 100) {
		if err := lw.Append(item); err != nil {
			t.Fatal(err)
		}
	}
	if err := lw.Freeze(nil); err != nil {
		t.Fatal(err)
	}
	if err := lw.Close(); err != nil {
		t.Fatal(err)
	}

	// All methods should return errors after Close.
	if _, err := lw.TotalItems(); err == nil {
		t.Fatal("expected error for TotalItems after Close")
	}
	if _, _, err := lw.ContentHash(); err == nil {
		t.Fatal("expected error for ContentHash after Close")
	}
	if err := lw.Verify(context.Background()); err == nil {
		t.Fatal("expected error for Verify after Close")
	}
}
