package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamir/events-analysis/packfile"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Create test keys using ComposeKey with different discriminators.
	keyA := ComposeKey(bytes.Repeat([]byte{0x01}, 32), 0x00) // "contractA, field 0"
	keyB := ComposeKey(bytes.Repeat([]byte{0x02}, 32), 0x00) // "contractB, field 0"
	topicKey := ComposeKey([]byte("transfer"), 0x01)          // "transfer, field 1"
	approveKey := ComposeKey([]byte("approve"), 0x01)         // "approve, field 1"
	mintKey := ComposeKey([]byte("mint"), 0x01)               // "mint, field 1"

	// Build index.
	w := NewWriter()
	w.Add(0, keyA)
	w.Add(1, keyA)
	w.Add(3, keyA)
	w.Add(2, keyB)
	w.Add(0, topicKey)
	w.Add(1, topicKey)
	w.Add(2, mintKey)
	w.Add(3, approveKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Open and query.
	r, err := Open(mphfPath, dataPath, WithExpectedLookups(10))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// keyA should have ordinals {0, 1, 3}.
	bm, err := r.Lookup(keyA)
	if err != nil {
		t.Fatalf("Lookup keyA: %v", err)
	}
	got := bm.ToArray()
	want := []uint32{0, 1, 3}
	if len(got) != len(want) {
		t.Fatalf("keyA: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keyA[%d]: got %d, want %d", i, got[i], want[i])
		}
	}

	// keyB should have ordinals {2}.
	bm, err = r.Lookup(keyB)
	if err != nil {
		t.Fatalf("Lookup keyB: %v", err)
	}
	got = bm.ToArray()
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("keyB: got %v, want [2]", got)
	}

	// topicKey "transfer" should have ordinals {0, 1}.
	bm, err = r.Lookup(topicKey)
	if err != nil {
		t.Fatalf("Lookup topicKey: %v", err)
	}
	got = bm.ToArray()
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("topicKey 'transfer': got %v, want [0 1]", got)
	}
}

func TestNonMemberLookup(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build a small index with one key.
	w := NewWriter()
	w.Add(0, ComposeKey(bytes.Repeat([]byte{0x01}, 32), 0x00))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Look up a key that doesn't exist.
	_, err = r.Lookup(ComposeKey(bytes.Repeat([]byte{0xFF}, 32), 0x00))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestLargeBitmapRoundTrip exercises the zstd compression path by creating
// a bitmap with enough ordinals that its serialized size exceeds 256 bytes.
func TestLargeBitmapRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter()
	// Add many ordinals to a single key to trigger compression (>= 256 bytes serialized).
	key := ComposeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
	for i := uint32(0); i < 5000; i++ {
		w.Add(i, key)
	}
	// Add a second key for diversity.
	key2 := ComposeKey(bytes.Repeat([]byte{0x02}, 32), 0x00)
	w.Add(0, key2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath, WithExpectedLookups(10))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	bm, err := r.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if bm.GetCardinality() != 5000 {
		t.Errorf("cardinality: got %d, want 5000", bm.GetCardinality())
	}
	// Verify first and last ordinals.
	if !bm.Contains(0) {
		t.Error("bitmap should contain 0")
	}
	if !bm.Contains(4999) {
		t.Error("bitmap should contain 4999")
	}

	// Second key.
	bm2, err := r.Lookup(key2)
	if err != nil {
		t.Fatalf("Lookup key2: %v", err)
	}
	if bm2.GetCardinality() != 1 || !bm2.Contains(0) {
		t.Errorf("key2: got cardinality %d, want 1", bm2.GetCardinality())
	}
}

// TestConcurrentLookups verifies that Reader.Lookup is safe for concurrent use.
func TestConcurrentLookups(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build an index with several keys.
	w := NewWriter()
	keys := make([][]byte, 20)
	for i := range keys {
		raw := make([]byte, 32)
		raw[0] = byte(i)
		raw[31] = byte(i)
		keys[i] = ComposeKey(raw, 0x00)
		w.Add(uint32(i), keys[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath, WithExpectedLookups(100))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Launch concurrent lookups.
	const goroutines = 8
	const iterations = 100
	errs := make(chan error, goroutines*iterations)

	for g := range goroutines {
		go func() {
			for i := range iterations {
				key := keys[(g*iterations+i)%len(keys)]
				bm, err := r.Lookup(key)
				if err != nil {
					errs <- err
					return
				}
				if bm.GetCardinality() != 1 {
					errs <- bytes.ErrTooLarge // placeholder error
					return
				}
			}
			errs <- nil
		}()
	}

	for range goroutines {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent lookup error: %v", err)
		}
	}
}

// TestManyKeys verifies correctness with a larger number of distinct keys.
func TestManyKeys(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	const numKeys = 1000
	w := NewWriter()
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		binary.BigEndian.PutUint32(raw[28:], uint32(i*7919))
		keys[i] = ComposeKey(raw, 0x00)
		w.Add(uint32(i), keys[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath, WithExpectedLookups(numKeys))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Verify all keys round-trip correctly.
	for i, key := range keys {
		bm, err := r.Lookup(key)
		if err != nil {
			t.Fatalf("Lookup key %d: %v", i, err)
		}
		got := bm.ToArray()
		if len(got) != 1 || got[0] != uint32(i) {
			t.Errorf("key %d: got %v, want [%d]", i, got, i)
		}
	}

	// Verify non-member returns ErrKeyNotFound.
	nonMember := make([]byte, 32)
	nonMember[0] = 0xFF
	nonMember[1] = 0xFF
	_, err = r.Lookup(ComposeKey(nonMember, 0x00))
	if err != ErrKeyNotFound {
		t.Errorf("non-member: got %v, want ErrKeyNotFound", err)
	}
}

// TestMultipleDiscriminatorsSameKey verifies that the same raw key with
// different discriminators produces independent bitmaps.
func TestMultipleDiscriminatorsSameKey(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter()
	raw := bytes.Repeat([]byte{0x42}, 32)
	// Same raw key, different discriminators, different ordinals.
	w.Add(10, ComposeKey(raw, 0x00))
	w.Add(20, ComposeKey(raw, 0x01))
	w.Add(30, ComposeKey(raw, 0x02))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath, WithExpectedLookups(10))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, tt := range []struct {
		disc    byte
		wantOrd uint32
	}{
		{0x00, 10},
		{0x01, 20},
		{0x02, 30},
	} {
		bm, err := r.Lookup(ComposeKey(raw, tt.disc))
		if err != nil {
			t.Fatalf("Lookup disc %d: %v", tt.disc, err)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != tt.wantOrd {
			t.Errorf("disc %d: got %v, want [%d]", tt.disc, arr, tt.wantOrd)
		}
	}
}

// TestCRC32CCorruptionDetected verifies that corrupting a byte in a packfile
// record causes Lookup to return a CRC mismatch error.
func TestCRC32CCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build a small index.
	w := NewWriter()
	key := ComposeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
	w.Add(0, key)
	w.Add(1, key)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Read the raw record to find its location in the file.
	pack, err := packfile.Open(dataPath)
	if err != nil {
		t.Fatalf("Open packfile: %v", err)
	}
	rec, err := pack.ReadRecordInto(0, nil)
	if err != nil {
		pack.Close()
		t.Fatalf("ReadRecordInto: %v", err)
	}
	pack.Close()

	// Verify it works before corruption.
	r, err := Open(mphfPath, dataPath, WithExpectedLookups(10))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	bm, err := r.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup before corruption: %v", err)
	}
	if bm.GetCardinality() != 2 {
		t.Fatalf("cardinality before corruption: got %d, want 2", bm.GetCardinality())
	}
	r.Close()

	// Corrupt a byte in the record's data (after fingerprint, before CRC).
	// The record layout is: [4B fingerprint][1B flags][N data][4B CRC32C].
	// Corrupt the flags byte so CRC won't match.
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Find the first occurrence of the record in the file by searching for the
	// first fingerprintSize bytes followed by the exact record content.
	recIdx := bytes.Index(data, rec[:fingerprintSize+1])
	if recIdx < 0 {
		t.Fatal("could not find record in packfile")
	}
	// Flip the flags byte.
	data[recIdx+fingerprintSize] ^= 0xFF

	corruptedPath := filepath.Join(dir, "corrupted.bitmaps")
	if err := os.WriteFile(corruptedPath, data, 0644); err != nil {
		t.Fatalf("write corrupted file: %v", err)
	}

	r, err = Open(mphfPath, corruptedPath, WithExpectedLookups(10))
	if err != nil {
		t.Fatalf("Open corrupted: %v", err)
	}
	defer r.Close()

	_, err = r.Lookup(key)
	if err == nil {
		t.Fatal("expected error from corrupted record, got nil")
	}
	// Should be a CRC mismatch or deserialization error.
	t.Logf("corruption detected: %v", err)
}

// TestComposeKey verifies the ComposeKey helper.
func TestComposeKey(t *testing.T) {
	key := []byte("hello")
	composed := ComposeKey(key, 0x42)
	if len(composed) != 6 {
		t.Fatalf("len: got %d, want 6", len(composed))
	}
	if !bytes.Equal(composed[:5], key) {
		t.Errorf("prefix: got %x, want %x", composed[:5], key)
	}
	if composed[5] != 0x42 {
		t.Errorf("discriminator: got %x, want 0x42", composed[5])
	}

	// Different discriminators produce different composed keys.
	c1 := ComposeKey(key, 0x00)
	c2 := ComposeKey(key, 0x01)
	if bytes.Equal(c1, c2) {
		t.Error("different discriminators should produce different composed keys")
	}
}

// TestCRC32CPackfileIntegrity verifies CRC32C is correctly written and verified
// by manually checking a record's CRC32C.
func TestCRC32CPackfileIntegrity(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter()
	key := ComposeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
	w.Add(0, key)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Read the raw record from the packfile and verify CRC32C manually.
	pack, err := packfile.Open(dataPath)
	if err != nil {
		t.Fatalf("Open packfile: %v", err)
	}
	defer pack.Close()

	rec, err := pack.ReadRecordInto(0, nil)
	if err != nil {
		t.Fatalf("ReadRecordInto: %v", err)
	}

	if len(rec) < fingerprintSize+1+checksumSize {
		t.Fatalf("record too short: %d bytes", len(rec))
	}

	storedCRC := binary.LittleEndian.Uint32(rec[len(rec)-checksumSize:])
	computedCRC := crc32.Checksum(rec[:len(rec)-checksumSize], crc32cTable)
	if storedCRC != computedCRC {
		t.Errorf("CRC32C mismatch: stored=%08x computed=%08x", storedCRC, computedCRC)
	}
}
