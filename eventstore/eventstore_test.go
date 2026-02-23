package eventstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
)

func makeEvents(n int, rng *rand.Rand) [][]byte {
	events := make([][]byte, n)
	for i := range events {
		size := 100 + rng.Intn(400) // 100-499 bytes
		ev := make([]byte, size)
		rng.Read(ev)
		events[i] = ev
	}
	return events
}

func writeTestStore(t *testing.T, events [][]byte, blockSize int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.events")
	w, err := Create(path, WriterOptions{BlockSize: blockSize})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := w.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	events := makeEvents(500, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 500 {
		t.Fatalf("EventCount = %d, want 500", r.EventCount())
	}

	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch (got %d bytes, want %d)", i, len(got), len(want))
		}
	}
}

func TestPartialLastBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(43))
	// 300 events = 2 full batches of 128 + 44 in partial
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 300 {
		t.Fatalf("EventCount = %d, want 300", r.EventCount())
	}

	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}
}

func TestReadEventsFullRange(t *testing.T) {
	rng := rand.New(rand.NewSource(44))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	j := 0
	for ev, err := range r.ReadEvents(0, 300) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[j]) {
			t.Fatalf("ReadEvents[%d]: data mismatch", j)
		}
		j++
	}
	if j != 300 {
		t.Fatalf("ReadEvents yielded %d, want 300", j)
	}
}

func TestReadEventsPartialRange(t *testing.T) {
	rng := rand.New(rand.NewSource(45))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Read events 120-200 (crosses block boundary at 128)
	j := 0
	for ev, err := range r.ReadEvents(120, 80) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[120+j]) {
			t.Fatalf("ReadEvents partial [%d]: data mismatch", j)
		}
		j++
	}
	if j != 80 {
		t.Fatalf("ReadEvents partial yielded %d, want 80", j)
	}
}

func TestReadEventsEarlyBreak(t *testing.T) {
	rng := rand.New(rand.NewSource(46))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	j := 0
	for ev, err := range r.ReadEvents(0, 300) {
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
	rng := rand.New(rand.NewSource(47))
	events := makeEvents(10, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
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

func TestReadIndicesScattered(t *testing.T) {
	rng := rand.New(rand.NewSource(48))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Indices spanning multiple blocks
	indices := []int{0, 1, 127, 128, 200, 299}
	j := 0
	for ev, err := range r.ReadIndices(context.Background(), indices) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[indices[j]]) {
			t.Fatalf("ReadIndices[%d] (event %d): data mismatch", j, indices[j])
		}
		j++
	}
	if j != len(indices) {
		t.Fatalf("ReadIndices yielded %d, want %d", j, len(indices))
	}
}

func TestReadIndicesDuplicatesPanic(t *testing.T) {
	rng := rand.New(rand.NewSource(49))
	events := makeEvents(100, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for duplicate indices")
		}
	}()
	r.ReadIndices(context.Background(), []int{5, 5, 10})
}

func TestReadIndicesUnsortedPanic(t *testing.T) {
	rng := rand.New(rand.NewSource(50))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsorted indices")
		}
	}()
	r.ReadIndices(context.Background(), []int{10, 5, 20})
}

func TestReadIndicesEarlyBreak(t *testing.T) {
	rng := rand.New(rand.NewSource(51))
	events := makeEvents(300, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	before := runtime.NumGoroutine()

	indices := []int{0, 50, 100, 150, 200, 250}
	j := 0
	for _, err := range r.ReadIndices(context.Background(), indices) {
		if err != nil {
			t.Fatal(err)
		}
		j++
		if j == 2 {
			break
		}
	}
	if j != 2 {
		t.Fatalf("early break: got %d, want 2", j)
	}

	// Give goroutines time to clean up
	runtime.Gosched()
	for range 100 {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		runtime.Gosched()
	}
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d, after=%d", before, after)
	}
}

func TestEmptyStore(t *testing.T) {
	path := writeTestStore(t, nil, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 0 {
		t.Fatalf("EventCount = %d, want 0", r.EventCount())
	}

	_, err = r.ReadEvent(0)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadEvent(0) on empty: got %v, want ErrIndexRange", err)
	}
}

func TestOutOfRange(t *testing.T) {
	rng := rand.New(rand.NewSource(53))
	events := makeEvents(10, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.ReadEvent(-1)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadEvent(-1): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadEvent(10)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadEvent(10): got %v, want ErrIndexRange", err)
	}

	_, err = r.ReadEvent(100)
	if !errors.Is(err, ErrIndexRange) {
		t.Fatalf("ReadEvent(100): got %v, want ErrIndexRange", err)
	}
}

func TestReadEventsPanic(t *testing.T) {
	rng := rand.New(rand.NewSource(54))
	events := makeEvents(10, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
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

	assertPanics("negative start", func() { r.ReadEvents(-1, 1) })
	assertPanics("negative count", func() { r.ReadEvents(0, -1) })
	assertPanics("out of range", func() { r.ReadEvents(5, 10) })
}

func TestSingleEvent(t *testing.T) {
	rng := rand.New(rand.NewSource(55))
	events := makeEvents(1, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.ReadEvent(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, events[0]) {
		t.Fatal("data mismatch")
	}
}

func TestSmallBlockSize(t *testing.T) {
	rng := rand.New(rand.NewSource(56))
	events := makeEvents(20, rng)
	path := writeTestStore(t, events, 3) // 3 events per block

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 20 {
		t.Fatalf("EventCount = %d, want 20", r.EventCount())
	}

	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}

	// Test ReadEvents across block boundary
	j := 0
	for ev, err := range r.ReadEvents(2, 5) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[2+j]) {
			t.Fatalf("ReadEvents[%d]: data mismatch", j)
		}
		j++
	}
	if j != 5 {
		t.Fatalf("ReadEvents yielded %d, want 5", j)
	}
}

func TestCreateInvalidBlockSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.events")
	_, err := Create(path, WriterOptions{BlockSize: -1})
	if err == nil {
		t.Fatal("expected error for negative blockSize")
	}
}

func TestParallelCompressionRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(70))
	events := makeEvents(500, rng)

	// Write with serial compression.
	serialPath := filepath.Join(t.TempDir(), "serial.events")
	sw, err := Create(serialPath, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := sw.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Finish(); err != nil {
		t.Fatal(err)
	}

	// Write with parallel compression.
	parallelPath := filepath.Join(t.TempDir(), "parallel.events")
	pw, err := Create(parallelPath, WriterOptions{Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := pw.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Finish(); err != nil {
		t.Fatal(err)
	}

	// Verify parallel output reads back correctly.
	r, err := Open(parallelPath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 500 {
		t.Fatalf("EventCount = %d, want 500", r.EventCount())
	}
	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}
}

func TestNoCompressionRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	events := makeEvents(500, rng)

	// Test with Concurrency set — verifies it's correctly forced to serial.
	path := filepath.Join(t.TempDir(), "nocomp.events")
	w, err := Create(path, WriterOptions{NoCompression: true, Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if err := w.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 500 {
		t.Fatalf("EventCount = %d, want 500", r.EventCount())
	}

	// Verify via ReadEvent (point reads).
	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}

	// Verify via ReadEvents (sequential range).
	i := 0
	for got, err := range r.ReadEvents(0, r.EventCount()) {
		if err != nil {
			t.Fatalf("ReadEvents at %d: %v", i, err)
		}
		if !bytes.Equal(got, events[i]) {
			t.Fatalf("ReadEvents at %d: data mismatch", i)
		}
		i++
	}

	// Verify via ReadIndices (scattered).
	indices := []int{0, 50, 127, 128, 250, 499}
	j := 0
	for got, err := range r.ReadIndices(context.Background(), indices) {
		if err != nil {
			t.Fatalf("ReadIndices at %d: %v", j, err)
		}
		if !bytes.Equal(got, events[indices[j]]) {
			t.Fatalf("ReadIndices at %d (idx=%d): data mismatch", j, indices[j])
		}
		j++
	}
}

func TestReadIndicesAllFromSingleBlock(t *testing.T) {
	rng := rand.New(rand.NewSource(57))
	events := makeEvents(200, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// All indices from the first block
	indices := make([]int, 128)
	for i := range indices {
		indices[i] = i
	}
	j := 0
	for ev, err := range r.ReadIndices(context.Background(), indices) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[j]) {
			t.Fatalf("ReadIndices[%d]: data mismatch", j)
		}
		j++
	}
	if j != 128 {
		t.Fatalf("ReadIndices yielded %d, want 128", j)
	}
}

func TestReadIndicesEmpty(t *testing.T) {
	rng := rand.New(rand.NewSource(58))
	events := makeEvents(10, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	j := 0
	for _, err := range r.ReadIndices(context.Background(), nil) {
		if err != nil {
			t.Fatal(err)
		}
		j++
	}
	if j != 0 {
		t.Fatalf("empty ReadIndices yielded %d, want 0", j)
	}
}

func TestExactBlockMultiple(t *testing.T) {
	// 256 events with blockSize=128 = exactly 2 full blocks, no partial
	rng := rand.New(rand.NewSource(59))
	events := makeEvents(256, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.EventCount() != 256 {
		t.Fatalf("EventCount = %d, want 256", r.EventCount())
	}

	// Verify last event
	got, err := r.ReadEvent(255)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, events[255]) {
		t.Fatal("last event data mismatch")
	}

	// Verify via ReadEvents
	j := 0
	for ev, err := range r.ReadEvents(0, 256) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[j]) {
			t.Fatalf("ReadEvents[%d]: data mismatch", j)
		}
		j++
	}
	if j != 256 {
		t.Fatalf("ReadEvents yielded %d, want 256", j)
	}
}

func TestReadIndicesConcurrent(t *testing.T) {
	rng := rand.New(rand.NewSource(60))
	events := makeEvents(500, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for g := range 10 {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(int64(seed)))
			// Each goroutine picks 20 random sorted-unique indices
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
			for ev, err := range r.ReadIndices(context.Background(), indices) {
				if err != nil {
					errs <- fmt.Errorf("goroutine %d: %w", seed, err)
					return
				}
				if !bytes.Equal(ev, events[indices[j]]) {
					errs <- fmt.Errorf("goroutine %d, index %d: data mismatch", seed, indices[j])
					return
				}
				j++
			}
			if j != len(indices) {
				errs <- fmt.Errorf("goroutine %d: got %d events, want %d", seed, j, len(indices))
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestReadIndicesPreCanceled(t *testing.T) {
	rng := rand.New(rand.NewSource(61))
	events := makeEvents(100, rng)
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	indices := []int{0, 10, 50, 99}
	var gotErr error
	for _, err := range r.ReadIndices(ctx, indices) {
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

func TestUniformSizeEvents(t *testing.T) {
	// All events same size — exercises W=1 path (clamped from 0)
	events := make([][]byte, 200)
	for i := range events {
		events[i] = []byte(fmt.Sprintf("event-%04d-padding-data-here!", i))
	}
	path := writeTestStore(t, events, DefaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i, want := range events {
		got, err := r.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}
}
