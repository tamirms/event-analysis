package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tamir/events-analysis/packfile"
)

// composeKey appends disc as a trailing byte, producing a lookup key
// that disambiguates the same raw key across different logical fields.
func composeKey(key []byte, disc byte) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	out[len(key)] = disc
	return out
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Create test keys with different discriminators.
	keyA := composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00) // "contractA, field 0"
	keyB := composeKey(bytes.Repeat([]byte{0x02}, 32), 0x00) // "contractB, field 0"
	topicKey := composeKey([]byte("transfer"), 0x01)          // "transfer, field 1"
	approveKey := composeKey([]byte("approve"), 0x01)         // "approve, field 1"
	mintKey := composeKey([]byte("mint"), 0x01)               // "mint, field 1"

	// Build index.
	w := NewWriter(WriterOptions{})
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
	r, err := Open(mphfPath, dataPath)
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
	w := NewWriter(WriterOptions{})
	w.Add(0, composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00))

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
	_, err = r.Lookup(composeKey(bytes.Repeat([]byte{0xFF}, 32), 0x00))
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

	w := NewWriter(WriterOptions{})
	// Add many ordinals to a single key to trigger compression (>= 256 bytes serialized).
	key := composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
	for i := uint32(0); i < 5000; i++ {
		w.Add(i, key)
	}
	// Add a second key for diversity.
	key2 := composeKey(bytes.Repeat([]byte{0x02}, 32), 0x00)
	w.Add(0, key2)

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
	w := NewWriter(WriterOptions{})
	keys := make([][]byte, 20)
	for i := range keys {
		raw := make([]byte, 32)
		raw[0] = byte(i)
		raw[31] = byte(i)
		keys[i] = composeKey(raw, 0x00)
		w.Add(uint32(i), keys[i])
	}

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

	// Launch concurrent lookups.
	const goroutines = 8
	const iterations = 100
	errs := make(chan error, goroutines)

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
					errs <- fmt.Errorf("key %d: cardinality got %d, want 1",
						(g*iterations+i)%len(keys), bm.GetCardinality())
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
	w := NewWriter(WriterOptions{})
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		binary.BigEndian.PutUint32(raw[28:], uint32(i*7919))
		keys[i] = composeKey(raw, 0x00)
		w.Add(uint32(i), keys[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r, err := Open(mphfPath, dataPath)
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
	_, err = r.Lookup(composeKey(nonMember, 0x00))
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

	w := NewWriter(WriterOptions{})
	raw := bytes.Repeat([]byte{0x42}, 32)
	// Same raw key, different discriminators, different ordinals.
	w.Add(10, composeKey(raw, 0x00))
	w.Add(20, composeKey(raw, 0x01))
	w.Add(30, composeKey(raw, 0x02))

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

	for _, tt := range []struct {
		disc    byte
		wantOrd uint32
	}{
		{0x00, 10},
		{0x01, 20},
		{0x02, 30},
	} {
		bm, err := r.Lookup(composeKey(raw, tt.disc))
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
	w := NewWriter(WriterOptions{})
	key := composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
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
	r, err := Open(mphfPath, dataPath)
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
	// The batch record layout is: [2B count][fingerprints][flags][sizes][data][4B CRC32C].
	// Corrupt the flags byte so CRC won't match.
	fileData, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Find the first occurrence of the record in the file by searching for the
	// first fingerprintSize bytes followed by the exact record content.
	recIdx := bytes.Index(fileData, rec[:fingerprintSize+1])
	if recIdx < 0 {
		t.Fatal("could not find record in packfile")
	}
	// Flip the flags byte.
	fileData[recIdx+fingerprintSize] ^= 0xFF

	corruptedDataPath := filepath.Join(dir, "corrupted.bitmaps")
	if err := os.WriteFile(corruptedDataPath, fileData, 0644); err != nil {
		t.Fatalf("write corrupted file: %v", err)
	}

	// Use original MPHF with corrupted data file.
	r, err = Open(mphfPath, corruptedDataPath)
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

// TestCRC32CPackfileIntegrity verifies CRC32C is correctly written and verified
// by manually checking a record's CRC32C.
func TestCRC32CPackfileIntegrity(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter(WriterOptions{})
	key := composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)
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

// --- LookupKeys tests ---

// buildTestIndex builds a small index in dir with the given keys, each mapped
// to ordinal = its index position. Returns mphf and data paths.
func buildTestIndex(t *testing.T, dir string, keys [][]byte, opts WriterOptions) (string, string) {
	t.Helper()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter(opts)
	for i, key := range keys {
		w.Add(uint32(i), key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx, mphfPath, dataPath); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return mphfPath, dataPath
}

func TestLookupKeys(t *testing.T) {
	dir := t.TempDir()

	keys := make([][]byte, 50)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		keys[i] = composeKey(raw, 0x00)
	}

	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Look up a subset of keys.
	query := [][]byte{keys[0], keys[10], keys[49]}
	ctx := context.Background()
	results, err := r.LookupKeys(ctx, query)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}

	if len(results) != len(query) {
		t.Fatalf("results length: got %d, want %d", len(results), len(query))
	}

	wantOrds := []uint32{0, 10, 49}
	for i, bm := range results {
		if bm == nil {
			t.Errorf("result[%d] is nil", i)
			continue
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != wantOrds[i] {
			t.Errorf("result[%d]: got %v, want [%d]", i, arr, wantOrds[i])
		}
	}
}

func TestLookupKeysAllNotFound(t *testing.T) {
	dir := t.TempDir()

	keys := [][]byte{composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)}
	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Query with keys that don't exist.
	query := [][]byte{
		composeKey(bytes.Repeat([]byte{0xAA}, 32), 0x00),
		composeKey(bytes.Repeat([]byte{0xBB}, 32), 0x00),
		composeKey(bytes.Repeat([]byte{0xCC}, 32), 0x00),
	}
	results, err := r.LookupKeys(context.Background(), query)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}

	for i, bm := range results {
		if bm != nil {
			t.Errorf("result[%d] should be nil for not-found key", i)
		}
	}
}

func TestLookupKeysMixedBatches(t *testing.T) {
	dir := t.TempDir()

	// Use small batch size so keys span multiple batches.
	const numKeys = 200
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		binary.BigEndian.PutUint32(raw[28:], uint32(i*7919))
		keys[i] = composeKey(raw, 0x00)
	}

	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{BatchSize: 16})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Mix found and not-found keys from different batches.
	nonExistent := composeKey(bytes.Repeat([]byte{0xFF}, 32), 0x00)
	query := [][]byte{keys[0], nonExistent, keys[50], keys[150], nonExistent}

	results, err := r.LookupKeys(context.Background(), query)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("results length: got %d, want 5", len(results))
	}

	// keys[0] → ordinal 0
	if results[0] == nil || !results[0].Contains(0) {
		t.Errorf("result[0]: expected bitmap containing 0")
	}
	// not-found
	if results[1] != nil {
		t.Errorf("result[1]: expected nil for not-found key")
	}
	// keys[50] → ordinal 50
	if results[2] == nil || !results[2].Contains(50) {
		t.Errorf("result[2]: expected bitmap containing 50")
	}
	// keys[150] → ordinal 150
	if results[3] == nil || !results[3].Contains(150) {
		t.Errorf("result[3]: expected bitmap containing 150")
	}
	// not-found
	if results[4] != nil {
		t.Errorf("result[4]: expected nil for not-found key")
	}
}

func TestLookupKeysBatchCoalescing(t *testing.T) {
	dir := t.TempDir()

	// Use batch size 128 (default) — 10 keys will all land in the same batch.
	const numKeys = 10
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		keys[i] = composeKey(raw, 0x00)
	}

	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// All keys should be in one batch — LookupKeys reads it once.
	results, err := r.LookupKeys(context.Background(), keys)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}

	for i, bm := range results {
		if bm == nil {
			t.Errorf("result[%d] is nil", i)
			continue
		}
		if !bm.Contains(uint32(i)) {
			t.Errorf("result[%d]: expected to contain %d", i, i)
		}
	}
}

func TestLookupKeysConcurrent(t *testing.T) {
	dir := t.TempDir()

	const numKeys = 100
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		keys[i] = composeKey(raw, 0x00)
	}

	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine queries a different subset.
			start := g * (numKeys / goroutines)
			end := start + numKeys/goroutines
			query := keys[start:end]

			results, err := r.LookupKeys(context.Background(), query)
			if err != nil {
				errs <- err
				return
			}
			for i, bm := range results {
				if bm == nil {
					errs <- fmt.Errorf("result[%d] is nil", start+i)
					return
				}
				if !bm.Contains(uint32(start + i)) {
					errs <- fmt.Errorf("result[%d]: missing ordinal %d", start+i, start+i)
					return
				}
			}
			errs <- nil
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent LookupKeys error: %v", err)
		}
	}
}

func TestLookupKeysEmpty(t *testing.T) {
	dir := t.TempDir()

	keys := [][]byte{composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)}
	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// nil input
	results, err := r.LookupKeys(context.Background(), nil)
	if err != nil {
		t.Fatalf("LookupKeys(nil): %v", err)
	}
	if results != nil {
		t.Errorf("LookupKeys(nil): got %v, want nil", results)
	}

	// empty input
	results, err = r.LookupKeys(context.Background(), [][]byte{})
	if err != nil {
		t.Fatalf("LookupKeys(empty): %v", err)
	}
	if results != nil {
		t.Errorf("LookupKeys(empty): got %v, want nil", results)
	}
}

func TestCustomBatchSize(t *testing.T) {
	dir := t.TempDir()

	const numKeys = 100
	keys := make([][]byte, numKeys)
	for i := range keys {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		keys[i] = composeKey(raw, 0x00)
	}

	// Use non-default batch size.
	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{BatchSize: 16})

	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Verify batch size was stored and read back.
	if r.batchSize != 16 {
		t.Errorf("batchSize: got %d, want 16", r.batchSize)
	}

	// Verify all keys round-trip correctly.
	for i, key := range keys {
		bm, err := r.Lookup(key)
		if err != nil {
			t.Fatalf("Lookup key %d: %v", i, err)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != uint32(i) {
			t.Errorf("key %d: got %v, want [%d]", i, arr, i)
		}
	}

	// Also verify via LookupKeys.
	results, err := r.LookupKeys(context.Background(), keys)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}
	for i, bm := range results {
		if bm == nil {
			t.Errorf("LookupKeys result[%d] is nil", i)
			continue
		}
		if !bm.Contains(uint32(i)) {
			t.Errorf("LookupKeys result[%d]: missing ordinal %d", i, i)
		}
	}
}

func TestMetadataValidation(t *testing.T) {
	dir := t.TempDir()

	// Build a valid index first.
	keys := [][]byte{composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)}
	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	// Verify it opens cleanly.
	r, err := Open(mphfPath, dataPath)
	if err != nil {
		t.Fatalf("Open valid: %v", err)
	}
	r.Close()

	// Create a packfile with missing metadata.
	noMetaPath := filepath.Join(dir, "nometa.bitmaps")
	pw, err := packfile.Create(noMetaPath, packfile.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// No SetMetadata call — metadata will be nil/empty.
	pw.Append([]byte{0x01, 0x00}) // dummy record
	pw.Finish()

	_, err = Open(mphfPath, noMetaPath)
	if err == nil {
		t.Fatal("expected error for missing metadata")
	}
	t.Logf("missing metadata: %v", err)
}

func TestMetadataFlags(t *testing.T) {
	dir := t.TempDir()

	// Build a valid index first to get a valid MPHF.
	keys := [][]byte{composeKey(bytes.Repeat([]byte{0x01}, 32), 0x00)}
	mphfPath, dataPath := buildTestIndex(t, dir, keys, WriterOptions{})

	// Read the valid packfile and tamper with metadata flags.
	pack, err := packfile.Open(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	meta := pack.Metadata()
	rec, err := pack.ReadRecordInto(0, nil)
	pack.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Set unknown flags.
	binary.LittleEndian.PutUint16(meta[6:8], 0xFFFF)

	// Write a new packfile with the tampered metadata.
	tamperedPath := filepath.Join(dir, "tampered.bitmaps")
	pw, err := packfile.Create(tamperedPath, packfile.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pw.SetMetadata(meta)
	pw.Append(rec)
	pw.Finish()

	_, err = Open(mphfPath, tamperedPath)
	if err == nil {
		t.Fatal("expected error for unknown flags")
	}
	t.Logf("unknown flags: %v", err)
}
