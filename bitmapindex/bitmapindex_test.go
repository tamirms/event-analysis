package bitmapindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/packfile"
)

var testCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

// makeTestEvent builds a minimal Event with the given contractID and topics.
func makeTestEvent(contractID []byte, topics ...[]byte) *event.Event {
	return &event.Event{
		ContractID: contractID,
		Topics:     topics,
		EventType:  event.TypeContract,
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	contractA := bytes.Repeat([]byte{0x01}, 32)
	contractB := bytes.Repeat([]byte{0x02}, 32)
	topicTransfer := []byte("transfer")
	topicMint := []byte("mint")
	topicApprove := []byte("approve")

	// Build index.
	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	w.Add(makeTestEvent(contractA, topicTransfer), 0)
	w.Add(makeTestEvent(contractA, topicTransfer), 1)
	w.Add(makeTestEvent(contractB, topicMint), 2)
	w.Add(makeTestEvent(contractA, topicApprove), 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Open and query.
	r := Open(mphfPath, dataPath)
	defer r.Close()

	// contractA should have ordinals {0, 1, 3}.
	bm, err := r.Lookup(FieldContractID, contractA)
	if err != nil {
		t.Fatalf("Lookup contractA: %v", err)
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

	// contractB should have ordinals {2}.
	bm, err = r.Lookup(FieldContractID, contractB)
	if err != nil {
		t.Fatalf("Lookup contractB: %v", err)
	}
	got = bm.ToArray()
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("contractB: got %v, want [2]", got)
	}

	// topicTransfer (Topic0) should have ordinals {0, 1}.
	bm, err = r.Lookup(FieldTopic0, topicTransfer)
	if err != nil {
		t.Fatalf("Lookup topicTransfer: %v", err)
	}
	got = bm.ToArray()
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("topicTransfer: got %v, want [0 1]", got)
	}
}

// TestPrefetchPath exercises the ReadAll→OpenBytes MPHF loading path
// by passing a large WithExpectedLookups value on a small file.
func TestPrefetchPath(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	contractA := bytes.Repeat([]byte{0x01}, 32)
	contractB := bytes.Repeat([]byte{0x02}, 32)

	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	w.Add(makeTestEvent(contractA), 0)
	w.Add(makeTestEvent(contractA), 1)
	w.Add(makeTestEvent(contractB), 2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Large expectedLookups on a small file → prefetch path.
	r := Open(mphfPath, dataPath, WithExpectedLookups(1000))
	defer r.Close()

	bm, err := r.Lookup(FieldContractID, contractA)
	if err != nil {
		t.Fatalf("Lookup contractA: %v", err)
	}
	got := bm.ToArray()
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("contractA: got %v, want [0 1]", got)
	}

	bm, err = r.Lookup(FieldContractID, contractB)
	if err != nil {
		t.Fatalf("Lookup contractB: %v", err)
	}
	got = bm.ToArray()
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("contractB: got %v, want [2]", got)
	}
}

func TestNonMemberLookup(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build a small index with one key.
	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	w.Add(makeTestEvent(bytes.Repeat([]byte{0x01}, 32)), 0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r := Open(mphfPath, dataPath)
	defer r.Close()

	// Look up a key that doesn't exist.
	_, err := r.Lookup(FieldContractID, bytes.Repeat([]byte{0xFF}, 32))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

// TestLargeBitmapRoundTrip exercises round-trip with a large bitmap whose
// serialized size exceeds 256 bytes.
func TestLargeBitmapRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	contractA := bytes.Repeat([]byte{0x01}, 32)
	contractB := bytes.Repeat([]byte{0x02}, 32)
	// Add many ordinals so the bitmap serializes to a substantial size.
	for i := range uint32(5000) {
		w.Add(makeTestEvent(contractA), i)
	}
	// Add a second key for diversity.
	w.Add(makeTestEvent(contractB), 0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r := Open(mphfPath, dataPath)
	defer r.Close()

	bm, err := r.Lookup(FieldContractID, contractA)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if bm.GetCardinality() != 5000 {
		t.Errorf("cardinality: got %d, want 5000", bm.GetCardinality())
	}
	if !bm.Contains(0) {
		t.Error("bitmap should contain 0")
	}
	if !bm.Contains(4999) {
		t.Error("bitmap should contain 4999")
	}

	bm2, err := r.Lookup(FieldContractID, contractB)
	if err != nil {
		t.Fatalf("Lookup contractB: %v", err)
	}
	if bm2.GetCardinality() != 1 || !bm2.Contains(0) {
		t.Errorf("contractB: got cardinality %d, want 1", bm2.GetCardinality())
	}
}

// TestConcurrentLookups verifies that Reader.Lookup is safe for concurrent use.
func TestConcurrentLookups(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build an index with several keys.
	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	contracts := make([][]byte, 20)
	for i := range contracts {
		raw := make([]byte, 32)
		raw[0] = byte(i)
		raw[31] = byte(i)
		contracts[i] = raw
		w.Add(makeTestEvent(raw), uint32(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r := Open(mphfPath, dataPath)
	defer r.Close()

	const goroutines = 8
	const iterations = 100
	errs := make(chan error, goroutines)

	for g := range goroutines {
		go func() {
			for i := range iterations {
				cid := contracts[(g*iterations+i)%len(contracts)]
				bm, err := r.Lookup(FieldContractID, cid)
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
	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		binary.BigEndian.PutUint32(raw[28:], uint32(i*7919))
		contracts[i] = raw
		w.Add(makeTestEvent(raw), uint32(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r := Open(mphfPath, dataPath)
	defer r.Close()

	for i, cid := range contracts {
		bm, err := r.Lookup(FieldContractID, cid)
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
	_, err := r.Lookup(FieldContractID, nonMember)
	if err != ErrKeyNotFound {
		t.Errorf("non-member: got %v, want ErrKeyNotFound", err)
	}
}

// TestMultipleDiscriminatorsSameKey verifies that the same raw key with
// different fields produces independent bitmaps.
func TestMultipleDiscriminatorsSameKey(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	raw := bytes.Repeat([]byte{0x42}, 32)
	// Same 32-byte key as ContractID vs Topic0 vs Topic1.
	w.Add(makeTestEvent(raw), 10)                   // ContractID
	w.Add(makeTestEvent(nil, raw), 20)              // Topic0
	w.Add(makeTestEvent(nil, []byte("x"), raw), 30) // Topic1

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	r := Open(mphfPath, dataPath)
	defer r.Close()

	for _, tt := range []struct {
		field   Field
		wantOrd uint32
	}{
		{FieldContractID, 10},
		{FieldTopic0, 20},
		{FieldTopic1, 30},
	} {
		bm, err := r.Lookup(tt.field, raw)
		if err != nil {
			t.Fatalf("Lookup field %d: %v", tt.field, err)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != tt.wantOrd {
			t.Errorf("field %d: got %v, want [%d]", tt.field, arr, tt.wantOrd)
		}
	}
}

// TestCorruptionDetected verifies that corrupting a byte in a packfile
// record causes Lookup to return an integrity error (CRC32C or zstd checksum).
func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	// Build a small index.
	w := NewWriter(mphfPath, dataPath, WriterOptions{})
	cid := bytes.Repeat([]byte{0x01}, 32)
	w.Add(makeTestEvent(cid), 0)
	w.Add(makeTestEvent(cid), 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	// Verify it works before corruption.
	r := Open(mphfPath, dataPath)
	bm, err := r.Lookup(FieldContractID, cid)
	if err != nil {
		t.Fatalf("Lookup before corruption: %v", err)
	}
	if bm.GetCardinality() != 2 {
		t.Fatalf("cardinality before corruption: got %d, want 2", bm.GetCardinality())
	}
	r.Close()

	// Corrupt a byte in the first record data (byte 0 of the file).
	fileData, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	fileData[0] ^= 0xFF

	corruptedDataPath := filepath.Join(dir, "corrupted.bitmaps")
	if err := os.WriteFile(corruptedDataPath, fileData, 0644); err != nil {
		t.Fatalf("write corrupted file: %v", err)
	}

	r = Open(mphfPath, corruptedDataPath)
	defer r.Close()

	_, err = r.Lookup(FieldContractID, cid)
	if err == nil {
		t.Fatal("expected error from corrupted record, got nil")
	}
	t.Logf("corruption detected: %v", err)
}

// --- LookupKeys tests ---

// buildTestIndex builds a small index in dir where each contract key gets
// ordinal = its index position. Returns mphf and data paths.
func buildTestIndex(t *testing.T, dir string, contracts [][]byte, opts WriterOptions) (string, string) {
	t.Helper()
	mphfPath := filepath.Join(dir, "index.mphf")
	dataPath := filepath.Join(dir, "index.bitmaps")

	w := NewWriter(mphfPath, dataPath, opts)
	for i, cid := range contracts {
		w.Add(makeTestEvent(cid), uint32(i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := w.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return mphfPath, dataPath
}

func TestLookupKeys(t *testing.T) {
	dir := t.TempDir()

	contracts := make([][]byte, 50)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	query := []FieldKey{
		{FieldContractID, contracts[0]},
		{FieldContractID, contracts[10]},
		{FieldContractID, contracts[49]},
	}
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

	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	query := []FieldKey{
		{FieldContractID, bytes.Repeat([]byte{0xAA}, 32)},
		{FieldContractID, bytes.Repeat([]byte{0xBB}, 32)},
		{FieldContractID, bytes.Repeat([]byte{0xCC}, 32)},
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

	const numKeys = 200
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		binary.BigEndian.PutUint32(raw[28:], uint32(i*7919))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{BatchSize: 16})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	nonExistent := bytes.Repeat([]byte{0xFF}, 32)
	query := []FieldKey{
		{FieldContractID, contracts[0]},
		{FieldContractID, nonExistent},
		{FieldContractID, contracts[50]},
		{FieldContractID, contracts[150]},
		{FieldContractID, nonExistent},
	}

	results, err := r.LookupKeys(context.Background(), query)
	if err != nil {
		t.Fatalf("LookupKeys: %v", err)
	}

	if len(results) != 5 {
		t.Fatalf("results length: got %d, want 5", len(results))
	}

	if results[0] == nil || !results[0].Contains(0) {
		t.Errorf("result[0]: expected bitmap containing 0")
	}
	if results[1] != nil {
		t.Errorf("result[1]: expected nil for not-found key")
	}
	if results[2] == nil || !results[2].Contains(50) {
		t.Errorf("result[2]: expected bitmap containing 50")
	}
	if results[3] == nil || !results[3].Contains(150) {
		t.Errorf("result[3]: expected bitmap containing 150")
	}
	if results[4] != nil {
		t.Errorf("result[4]: expected nil for not-found key")
	}
}

func TestLookupKeysBatchCoalescing(t *testing.T) {
	dir := t.TempDir()

	const numKeys = 10
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	query := make([]FieldKey, numKeys)
	for i, cid := range contracts {
		query[i] = FieldKey{FieldContractID, cid}
	}
	results, err := r.LookupKeys(context.Background(), query)
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
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := range goroutines {
		wg.Go(func() {
			start := g * (numKeys / goroutines)
			end := start + numKeys/goroutines
			query := make([]FieldKey, end-start)
			for i := range query {
				query[i] = FieldKey{FieldContractID, contracts[start+i]}
			}

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
		})
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

	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	results, err := r.LookupKeys(context.Background(), nil)
	if err != nil {
		t.Fatalf("LookupKeys(nil): %v", err)
	}
	if results != nil {
		t.Errorf("LookupKeys(nil): got %v, want nil", results)
	}

	results, err = r.LookupKeys(context.Background(), []FieldKey{})
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
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{BatchSize: 16})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	// Verify all keys look up correctly — if batchSize were wrong, lookups would fail.
	for i, cid := range contracts {
		bm, err := r.Lookup(FieldContractID, cid)
		if err != nil {
			t.Fatalf("Lookup key %d: %v", i, err)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != uint32(i) {
			t.Errorf("key %d: got %v, want [%d]", i, arr, i)
		}
	}

	query := make([]FieldKey, numKeys)
	for i, cid := range contracts {
		query[i] = FieldKey{FieldContractID, cid}
	}
	results, err := r.LookupKeys(context.Background(), query)
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

func TestCompressedRoundTrip(t *testing.T) {
	dir := t.TempDir()

	const numKeys = 200
	contracts := make([][]byte, numKeys)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}

	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{Compress: true})

	r := Open(mphfPath, dataPath)
	defer r.Close()

	for i, cid := range contracts {
		bm, err := r.Lookup(FieldContractID, cid)
		if err != nil {
			t.Fatalf("Lookup key %d: %v", i, err)
		}
		arr := bm.ToArray()
		if len(arr) != 1 || arr[0] != uint32(i) {
			t.Errorf("key %d: got %v, want [%d]", i, arr, i)
		}
	}
}

func TestMetadataValidation(t *testing.T) {
	dir := t.TempDir()

	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, _ := buildTestIndex(t, dir, contracts, WriterOptions{})

	// Create a plain packfile with non-bitmapindex content (no fingerprint prefix).
	// When the bitmapindex reader does Lookup, the fingerprint check should fail
	// and return ErrKeyNotFound.
	plainPath := filepath.Join(dir, "plain.bitmaps")
	pw, err := packfile.Create(plainPath, packfile.WriterOptions{RecordSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = pw.Append([]byte{0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	if err = pw.Finish(nil); err != nil {
		t.Fatal(err)
	}

	r := Open(mphfPath, plainPath)
	defer r.Close()
	_, err = r.Lookup(FieldContractID, bytes.Repeat([]byte{0x01}, 32))
	if err == nil {
		t.Fatal("expected error for non-bitmapindex packfile")
	}
	t.Logf("non-bitmapindex packfile: %v", err)
}

func TestMetadataFlags(t *testing.T) {
	dir := t.TempDir()

	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	// Read the raw file bytes and locate the trailer.
	fileData, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}

	// Parse 64-byte trailer. Flags are at offset 5.
	trailerStart := len(fileData) - 64
	flags := fileData[trailerStart+5]
	if flags&0x1 == 0 {
		t.Fatalf("expected uncompressed flag (bit 0) in trailer, got %02x", flags)
	}

	// Set an unknown flag (bit 2) — must be rejected.
	// Also recompute the CRC so this tests flag rejection, not CRC failure.
	fileData[trailerStart+5] = 0x04

	// Recompute trailer CRC over trailer[0:60] only.
	crc := crc32.New(testCRC32CTable)
	crc.Write(fileData[trailerStart : trailerStart+60])
	binary.LittleEndian.PutUint32(fileData[trailerStart+60:], crc.Sum32())

	tamperedPath := filepath.Join(dir, "tampered.bitmaps")
	if err := os.WriteFile(tamperedPath, fileData, 0644); err != nil {
		t.Fatal(err)
	}

	r := Open(mphfPath, tamperedPath)
	defer r.Close()
	_, err = r.Lookup(FieldContractID, bytes.Repeat([]byte{0x01}, 32))
	if err == nil {
		t.Fatal("expected error for unknown flags")
	}
	t.Logf("unknown flags: %v", err)
}

func TestMPHFErrorAtQueryTime(t *testing.T) {
	dir := t.TempDir()
	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	_, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open("/nonexistent/bad.mphf", dataPath)
	defer r.Close()

	_, err := r.Lookup(FieldContractID, bytes.Repeat([]byte{0x01}, 32))
	if err == nil {
		t.Fatal("expected MPHF error")
	}
}

func TestPackfileErrorAtQueryTime(t *testing.T) {
	dir := t.TempDir()
	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, _ := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, "/nonexistent/bad.bitmaps")
	defer r.Close()

	_, err := r.Lookup(FieldContractID, bytes.Repeat([]byte{0x01}, 32))
	if err == nil {
		t.Fatal("expected packfile error")
	}
}

func TestCloseWithoutQuery(t *testing.T) {
	dir := t.TempDir()
	contracts := [][]byte{bytes.Repeat([]byte{0x01}, 32)}
	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{})

	r := Open(mphfPath, dataPath)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseWithFailedOpen(t *testing.T) {
	r := Open("/nonexistent/bad.mphf", "/nonexistent/bad.bitmaps")
	// Close drains goroutines and releases resources. Open errors are
	// surfaced at query time, not at Close time, so this should not panic.
	r.Close()
}

// --- Content Hash Tests ---

func TestContentHashRoundTrip(t *testing.T) {
	dir := t.TempDir()
	mphfPath, dataPath := buildTestIndex(t, dir, makeContracts(50), WriterOptions{ContentHash: true})

	r := Open(mphfPath, dataPath)
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
	contracts := makeContracts(50)

	dir1 := t.TempDir()
	mphfPath1, dataPath1 := buildTestIndex(t, dir1, contracts, WriterOptions{ContentHash: true})

	dir2 := t.TempDir()
	mphfPath2, dataPath2 := buildTestIndex(t, dir2, contracts, WriterOptions{ContentHash: true})

	r1 := Open(mphfPath1, dataPath1)
	defer r1.Close()
	r2 := Open(mphfPath2, dataPath2)
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
	dir := t.TempDir()
	mphfPath, dataPath := buildTestIndex(t, dir, makeContracts(10), WriterOptions{})

	r := Open(mphfPath, dataPath)
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

func TestContentHashCompressed(t *testing.T) {
	dir := t.TempDir()
	mphfPath, dataPath := buildTestIndex(t, dir, makeContracts(50), WriterOptions{ContentHash: true, Compress: true})

	r := Open(mphfPath, dataPath)
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

func TestContentHashCorruption(t *testing.T) {
	dir := t.TempDir()
	contracts := makeContracts(50)
	mphfPath, dataPath := buildTestIndex(t, dir, contracts, WriterOptions{ContentHash: true})

	// Verify it works before corruption.
	r := Open(mphfPath, dataPath)
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify before corruption: %v", err)
	}
	r.Close()

	// Corrupt the content hash in the trailer (bytes [26:58] from trailer start).
	// Recompute CRC so the test validates hash mismatch, not CRC failure.
	fileData, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}

	trailerStart := len(fileData) - 64
	fileData[trailerStart+26] ^= 0xFF

	// Recompute trailer CRC.
	// Recompute trailer CRC over trailer[0:60] only.
	crc := crc32.New(testCRC32CTable)
	crc.Write(fileData[trailerStart : trailerStart+60])
	binary.LittleEndian.PutUint32(fileData[trailerStart+60:], crc.Sum32())

	corruptedPath := filepath.Join(dir, "corrupted.bitmaps")
	if err := os.WriteFile(corruptedPath, fileData, 0644); err != nil {
		t.Fatal(err)
	}

	r = Open(mphfPath, corruptedPath)
	defer r.Close()

	err = r.Verify(context.Background())
	if err == nil {
		t.Fatal("expected error from corrupted hash")
	}
	if !errors.Is(err, packfile.ErrContentHashMismatch) {
		t.Fatalf("expected ErrContentHashMismatch, got: %v", err)
	}
}

func makeContracts(n int) [][]byte {
	contracts := make([][]byte, n)
	for i := range contracts {
		raw := make([]byte, 32)
		binary.BigEndian.PutUint32(raw, uint32(i))
		contracts[i] = raw
	}
	return contracts
}
