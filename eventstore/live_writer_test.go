package eventstore

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/tamir/events-analysis/packfile"
)

func TestLiveEventstoreRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(200))
	events := makeEvents(300, rng)
	path := filepath.Join(t.TempDir(), "live.events")

	lw, err := CreateLive(path, WriterOptions{ContentHash: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range events {
		if err := lw.Append(ev); err != nil {
			t.Fatal(err)
		}
	}

	ec, err := lw.EventCount()
	if err != nil {
		t.Fatal(err)
	}
	if ec != 300 {
		t.Fatalf("EventCount = %d, want 300", ec)
	}

	// ReadEvent returns owned copy.
	got, err := lw.ReadEvent(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, events[0]) {
		t.Fatal("ReadEvent(0): data mismatch")
	}

	// ReadEvents (range).
	j := 0
	for ev, err := range lw.ReadEvents(120, 10) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[120+j]) {
			t.Fatalf("ReadEvents[%d]: data mismatch", j)
		}
		j++
	}
	if j != 10 {
		t.Fatalf("ReadEvents yielded %d, want 10", j)
	}

	// ReadIndices (scattered).
	indices := []int{0, 127, 128, 255, 299}
	j = 0
	for ev, err := range lw.ReadIndices(context.Background(), indices) {
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev, events[indices[j]]) {
			t.Fatalf("ReadIndices[%d]: data mismatch", j)
		}
		j++
	}
	if j != len(indices) {
		t.Fatalf("ReadIndices yielded %d, want %d", j, len(indices))
	}

	// Freeze and verify via standard reader.
	if err := lw.Freeze(); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()

	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify after freeze: %v", err)
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

func TestLiveEventstoreRecovery(t *testing.T) {
	rng := rand.New(rand.NewSource(201))
	events := makeEvents(300, rng)
	path := filepath.Join(t.TempDir(), "recover.events")

	opts := WriterOptions{ContentHash: true}
	lw, err := CreateLive(path, opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range events {
		if err := lw.Append(ev); err != nil {
			t.Fatal(err)
		}
	}

	cp, err := lw.Sync()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash: just drop the LiveWriter (fd leaks, OS cleans up).
	// Don't call Close — it removes the file (Freeze wasn't called).

	// Recover.
	lw2, err := OpenLive(path, cp, opts)
	if err != nil {
		t.Fatal(err)
	}

	ec, err := lw2.EventCount()
	if err != nil {
		t.Fatal(err)
	}
	flushed := cp.TotalItems()
	if ec != flushed {
		t.Fatalf("EventCount = %d, want %d", ec, flushed)
	}

	// Verify flushed events.
	for i := range flushed {
		got, err := lw2.ReadEvent(i)
		if err != nil {
			t.Fatalf("ReadEvent(%d): %v", i, err)
		}
		if !bytes.Equal(got, events[i]) {
			t.Fatalf("ReadEvent(%d): data mismatch", i)
		}
	}

	// Re-append lost events and freeze.
	for i := flushed; i < len(events); i++ {
		if err := lw2.Append(events[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := lw2.Freeze(); err != nil {
		t.Fatal(err)
	}

	r := Open(path)
	defer r.Close()
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify after recovery+freeze: %v", err)
	}
}

func TestLiveEventstoreOutOfRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oor.events")
	lw, err := CreateLive(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer lw.Close()

	_, err = lw.ReadEvent(0)
	if err != ErrIndexRange {
		t.Fatalf("expected ErrIndexRange, got %v", err)
	}
}

func TestLiveEventstoreAllFormats(t *testing.T) {
	formats := []struct {
		name   string
		format packfile.RecordFormat
	}{
		{"Compressed", packfile.Compressed},
		{"Uncompressed", packfile.Uncompressed},
		{"Raw", packfile.Raw},
	}

	for _, f := range formats {
		t.Run(f.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(202))
			events := makeEvents(200, rng)
			path := filepath.Join(t.TempDir(), "fmt.events")

			lw, err := CreateLive(path, WriterOptions{Format: f.format})
			if err != nil {
				t.Fatal(err)
			}

			for _, ev := range events {
				if err := lw.Append(ev); err != nil {
					t.Fatal(err)
				}
			}

			ec, _ := lw.EventCount()
			if ec != 200 {
				t.Fatalf("EventCount = %d, want 200", ec)
			}

			got, err := lw.ReadEvent(199)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, events[199]) {
				t.Fatal("last event mismatch")
			}

			if err := lw.Freeze(); err != nil {
				t.Fatal(err)
			}

			r := Open(path)
			defer r.Close()
			rEC, _ := r.EventCount()
			if rEC != 200 {
				t.Fatalf("after freeze EventCount = %d, want 200", rEC)
			}
		})
	}
}
