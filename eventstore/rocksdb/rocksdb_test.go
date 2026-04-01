package rocksdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/tamir/events-analysis/eventstore"
)

func writeTestStore(t *testing.T, dir string, events [][]byte, opts WriterOptions) string {
	t.Helper()
	dbPath := filepath.Join(dir, "test.db")
	w, err := Create(dbPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := w.AppendEvent(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return dbPath
}

func makeEvents(n int) [][]byte {
	events := make([][]byte, n)
	for i := range n {
		events[i] = []byte(fmt.Sprintf("event-%06d-payload", i))
	}
	return events
}

func TestRoundTrip(t *testing.T) {
	events := makeEvents(500)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	n, err := r.EventCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 500 {
		t.Fatalf("EventCount: got %d, want 500", n)
	}

	// Verify all events via ReadEvent (point reads).
	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}

	// Verify all events via ReadEvents (sequential).
	i := 0
	for ev, err := range r.ReadEvents(0, n) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[i]) {
			t.Fatalf("event %d: got %q, want %q", i, ev, events[i])
		}
		i++
	}
	if i != 500 {
		t.Fatalf("iterated %d events, want 500", i)
	}
}

func TestReadEventsRange(t *testing.T) {
	events := makeEvents(100)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	// Read middle slice.
	i := 0
	for ev, err := range r.ReadEvents(25, 50) {
		if err != nil {
			t.Fatal(err)
		}
		want := events[25+i]
		if !bytes.Equal(ev, want) {
			t.Fatalf("event %d: got %q, want %q", 25+i, ev, want)
		}
		i++
	}
	if i != 50 {
		t.Fatalf("iterated %d events, want 50", i)
	}
}

func TestReadPositionsScattered(t *testing.T) {
	events := makeEvents(1000)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	indices := []int{0, 50, 100, 500, 999}
	i := 0
	for ev, err := range r.ReadPositions(context.Background(), indices) {
		if err != nil {
			t.Fatal(err)
		}
		want := events[indices[i]]
		if !bytes.Equal(ev, want) {
			t.Fatalf("index %d: got %q, want %q", indices[i], ev, want)
		}
		i++
	}
	if i != len(indices) {
		t.Fatalf("iterated %d events, want %d", i, len(indices))
	}
}

func TestEmptyStore(t *testing.T) {
	dbPath := writeTestStore(t, t.TempDir(), nil, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	n, err := r.EventCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("EventCount: got %d, want 0", n)
	}

	_, err = r.ReadEvent(0)
	if err != eventstore.ErrPositionOutOfRange {
		t.Fatalf("ReadEvent(0) on empty: got %v, want ErrPositionOutOfRange", err)
	}
}

func TestReadEventOutOfRange(t *testing.T) {
	events := makeEvents(10)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	_, err := r.ReadEvent(-1)
	if err != eventstore.ErrPositionOutOfRange {
		t.Fatalf("ReadEvent(-1): got %v, want ErrPositionOutOfRange", err)
	}

	_, err = r.ReadEvent(10)
	if err != eventstore.ErrPositionOutOfRange {
		t.Fatalf("ReadEvent(10): got %v, want ErrPositionOutOfRange", err)
	}

	_, err = r.ReadEvent(100)
	if err != eventstore.ErrPositionOutOfRange {
		t.Fatalf("ReadEvent(100): got %v, want ErrPositionOutOfRange", err)
	}
}

func TestReadEventsPanic(t *testing.T) {
	events := makeEvents(10)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	// Force open so panics aren't masked.
	if _, err := r.EventCount(); err != nil {
		t.Fatal(err)
	}

	assertPanics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected panic", name)
			}
		}()
		fn()
	}

	assertPanics("negative start", func() {
		for range r.ReadEvents(-1, 5) {
		}
	})
	assertPanics("negative count", func() {
		for range r.ReadEvents(0, -1) {
		}
	})
	assertPanics("count exceeds", func() {
		for range r.ReadEvents(8, 5) {
		}
	})
}

func TestNoCompression(t *testing.T) {
	events := makeEvents(50)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{NoCompression: true})

	r := Open(dbPath)
	defer r.Close()

	n, err := r.EventCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("EventCount: got %d, want 50", n)
	}

	ev, err := r.ReadEvent(25)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ev, events[25]) {
		t.Fatalf("ReadEvent(25): got %q, want %q", ev, events[25])
	}
}

func TestCloseWithoutFinish(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	w, err := Create(dbPath, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendEvent([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Double Close is safe.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadEventsEarlyBreak(t *testing.T) {
	events := makeEvents(100)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	j := 0
	for ev, err := range r.ReadEvents(0, 100) {
		if err != nil {
			t.Fatal(err)
		}
		_ = ev
		j++
		if j == 5 {
			break
		}
	}
	if j != 5 {
		t.Fatalf("early break: got %d, want 5", j)
	}
}

func TestReadEventsEmpty(t *testing.T) {
	events := makeEvents(10)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	j := 0
	for _, err := range r.ReadEvents(0, 0) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("empty ReadEvents yielded %d, want 0", j)
	}
}

func TestReadPositionsEmpty(t *testing.T) {
	events := makeEvents(10)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	j := 0
	for _, err := range r.ReadPositions(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("empty ReadPositions yielded %d, want 0", j)
	}
}

func TestReadPositionsDuplicatesPanic(t *testing.T) {
	events := makeEvents(100)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate positions")
		}
	}()
	for range r.ReadPositions(context.Background(), []int{5, 5, 10}) {
	}
}

func TestReadPositionsUnsortedPanic(t *testing.T) {
	events := makeEvents(100)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsorted positions")
		}
	}()
	for range r.ReadPositions(context.Background(), []int{10, 5, 20}) {
	}
}

func TestReadPositionsConcurrent(t *testing.T) {
	events := makeEvents(500)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := range goroutines {
		wg.Go(func() {
			localRng := rand.New(rand.NewSource(int64(g)))
			m := map[int]struct{}{}
			for len(m) < 20 {
				m[localRng.Intn(500)] = struct{}{}
			}
			indices := make([]int, 0, len(m))
			for idx := range m {
				indices = append(indices, idx)
			}
			slices.Sort(indices)

			j := 0
			for ev, err := range r.ReadPositions(context.Background(), indices) {
				if err != nil {
					errs <- fmt.Errorf("goroutine %d: %w", g, err)
					return
				}
				if !bytes.Equal(ev, events[indices[j]]) {
					errs <- fmt.Errorf("goroutine %d, index %d: data mismatch", g, indices[j])
					return
				}
				j++
			}
			if j != len(indices) {
				errs <- fmt.Errorf("goroutine %d: got %d events, want %d", g, j, len(indices))
			}
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestReadPositionsPreCanceled(t *testing.T) {
	events := makeEvents(100)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	indices := []int{0, 10, 50, 99}
	var gotErr error
	for _, err := range r.ReadPositions(ctx, indices) {
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected error from pre-canceled context")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", gotErr)
	}
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/test.db")
	defer r.Close()

	_, err := r.ReadEvent(0)
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestDoubleClose(t *testing.T) {
	events := makeEvents(10)
	dbPath := writeTestStore(t, t.TempDir(), events, WriterOptions{})

	r := Open(dbPath)

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close must not panic (double-free).
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}
