package rocksdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/event"
)

func makeTestEvent(contractID []byte, topics ...[]byte) *event.Event {
	return &event.Event{
		ContractID: contractID,
		Topics:     topics,
		EventType:  event.TypeContract,
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	contractA := bytes.Repeat([]byte{0x01}, 32)
	contractB := bytes.Repeat([]byte{0x02}, 32)
	topicTransfer := []byte("transfer")
	topicMint := []byte("mint")

	w := NewWriter(dbPath, WriterOptions{})
	w.Add(makeTestEvent(contractA, topicTransfer), 0)
	w.Add(makeTestEvent(contractA, topicTransfer), 1)
	w.Add(makeTestEvent(contractB, topicMint), 2)
	w.Add(makeTestEvent(contractA), 3)

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	// contractA -> {0, 1, 3}
	bm, err := r.Lookup(bitmapindex.FieldContractID, contractA)
	if err != nil {
		t.Fatal(err)
	}
	got := bm.ToArray()
	want := []uint32{0, 1, 3}
	if len(got) != len(want) {
		t.Fatalf("contractA: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("contractA[%d]: got %d, want %d", i, got[i], want[i])
		}
	}

	// contractB -> {2}
	bm, err = r.Lookup(bitmapindex.FieldContractID, contractB)
	if err != nil {
		t.Fatal(err)
	}
	got = bm.ToArray()
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("contractB: got %v, want [2]", got)
	}

	// topicTransfer (Topic0) -> {0, 1}
	bm, err = r.Lookup(bitmapindex.FieldTopic0, topicTransfer)
	if err != nil {
		t.Fatal(err)
	}
	got = bm.ToArray()
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("topicTransfer: got %v, want [0 1]", got)
	}
}

func TestNonMemberLookup(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	w.Add(makeTestEvent(bytes.Repeat([]byte{0x01}, 32)), 0)

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	_, err := r.Lookup(bitmapindex.FieldContractID, bytes.Repeat([]byte{0xFF}, 32))
	if err != bitmapindex.ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestLargeBitmapRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	contractA := bytes.Repeat([]byte{0x01}, 32)
	for i := range uint32(5000) {
		w.Add(makeTestEvent(contractA), i)
	}

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	bm, err := r.Lookup(bitmapindex.FieldContractID, contractA)
	if err != nil {
		t.Fatal(err)
	}
	if bm.GetCardinality() != 5000 {
		t.Fatalf("cardinality: got %d, want 5000", bm.GetCardinality())
	}
	if !bm.Contains(0) || !bm.Contains(4999) {
		t.Fatal("bitmap missing expected values")
	}
}

func TestLookupKeys(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	const numKeys = 50
	contracts := make([][]byte, numKeys)

	w := NewWriter(dbPath, WriterOptions{})
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
		w.Add(makeTestEvent(raw), uint32(i))
	}

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	query := []bitmapindex.FieldKey{
		{Field: bitmapindex.FieldContractID, Key: contracts[0]},
		{Field: bitmapindex.FieldContractID, Key: contracts[10]},
		{Field: bitmapindex.FieldContractID, Key: contracts[49]},
	}
	results, err := r.LookupKeys(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}

	wantOrds := []uint32{0, 10, 49}
	for i, bm := range results {
		if bm == nil {
			t.Fatalf("result[%d] is nil", i)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != wantOrds[i] {
			t.Fatalf("result[%d]: got %v, want [%d]", i, arr, wantOrds[i])
		}
	}
}

func TestLookupKeysMixed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	contractA := bytes.Repeat([]byte{0x01}, 32)
	w.Add(makeTestEvent(contractA), 0)

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	nonExistent := bytes.Repeat([]byte{0xFF}, 32)
	query := []bitmapindex.FieldKey{
		{Field: bitmapindex.FieldContractID, Key: contractA},
		{Field: bitmapindex.FieldContractID, Key: nonExistent},
	}
	results, err := r.LookupKeys(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}

	if results[0] == nil || !results[0].Contains(0) {
		t.Error("result[0]: expected bitmap containing 0")
	}
	if results[1] != nil {
		t.Error("result[1]: expected nil for not-found key")
	}
}

func TestConcurrentLookups(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	contracts := make([][]byte, 20)
	for i := range contracts {
		raw := make([]byte, 32)
		raw[0] = byte(i)
		raw[31] = byte(i)
		contracts[i] = raw
		w.Add(makeTestEvent(raw), uint32(i))
	}

	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	const goroutines = 8
	const iterations = 100
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := range goroutines {
		wg.Go(func() {
			for i := range iterations {
				cid := contracts[(g*iterations+i)%len(contracts)]
				bm, err := r.Lookup(bitmapindex.FieldContractID, cid)
				if err != nil {
					errs <- err
					return
				}
				if bm.GetCardinality() != 1 {
					errs <- fmt.Errorf("key %d: cardinality got %d, want 1",
						(g*iterations+i)%len(contracts), bm.GetCardinality())
					return
				}
			}
			errs <- nil
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenBadPath(t *testing.T) {
	r := Open("/nonexistent/path/to/test.db")
	defer r.Close()

	_, err := r.Lookup(bitmapindex.FieldContractID, bytes.Repeat([]byte{0x01}, 32))
	if err == nil {
		t.Fatal("expected error for bad path")
	}
}

func TestDoubleClose(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	w.Add(makeTestEvent(bytes.Repeat([]byte{0x01}, 32)), 0)
	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)

	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close must not panic (double-free).
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupKeysEmpty(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w := NewWriter(dbPath, WriterOptions{})
	w.Add(makeTestEvent(bytes.Repeat([]byte{0x01}, 32)), 0)
	if err := w.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := Open(dbPath)
	defer r.Close()

	results, err := r.LookupKeys(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil for empty query, got %v", results)
	}
}
