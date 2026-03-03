package packfile

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/zstd"
)

// buildPayload assembles entries into a contiguous payload and returns item sizes.
func buildPayload(entries [][]byte) (payload []byte, sizes []uint32) {
	sizes = make([]uint32, len(entries))
	for i, e := range entries {
		payload = append(payload, e...)
		sizes[i] = uint32(len(e))
	}
	return
}

// buildForIndex encodes sizes as a FOR index: [packed][1B W][4B min][4B CRC32C].
func buildForIndex(sizes []uint32) []byte {
	encoded := intpack.EncodeGroup(sizes)
	return binary.LittleEndian.AppendUint32(encoded, CRC32C(encoded))
}

// configTestDecoder sets up a decoder to decode a single record containing n items.
func configTestDecoder(rd *decoder, n int, format RecordFormat) {
	rd.format = format
	rd.totalItems = n
	rd.recordSize = n
}

func TestDecoderCompressed(t *testing.T) {
	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("!"),
	}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	compressed, err := zstd.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := append(compressed, forIndex...)

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, len(entries), Compressed)

	if err := rd.Decode(data, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		got := rd.Entry(i)
		if string(got) != string(want) {
			t.Errorf("Entry(%d) = %q, want %q", i, got, want)
		}
	}

	// Verify EntryCopy returns an independent copy.
	cp := rd.EntryCopy(0)
	cp[0] = 'X'
	if rd.Entry(0)[0] == 'X' {
		t.Error("EntryCopy should return an independent copy")
	}
}

func TestDecoderUncompressed(t *testing.T) {
	entries := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	// CRC_items covers payload only; FOR index has its own CRC.
	raw := binary.LittleEndian.AppendUint32(append([]byte{}, payload...), CRC32C(payload))
	raw = append(raw, forIndex...)

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, len(entries), Uncompressed)

	if err := rd.Decode(raw, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		got := rd.Entry(i)
		if string(got) != string(want) {
			t.Errorf("Entry(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestDecoderCRC32CCorruption(t *testing.T) {
	// recordSize=1: no FOR index, just [payload][4B CRC_items].
	payload := []byte("data")
	raw := binary.LittleEndian.AppendUint32(append([]byte{}, payload...), CRC32C(payload))

	// Corrupt a byte in the payload.
	raw[0] ^= 0xFF

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, 1, Uncompressed)

	err := rd.Decode(raw, 0)
	if err == nil {
		t.Fatal("expected CRC32C error")
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got: %v", err)
	}
}

func TestDecoderCRC32CShortRecord(t *testing.T) {
	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, 1, Uncompressed)

	// 4 bytes = too short (need at least 5: 1 payload + 4 CRC).
	err := rd.Decode([]byte{1, 2, 3, 4}, 0)
	if err == nil {
		t.Fatal("expected error for short record")
	}

	// 0 bytes.
	err = rd.Decode(nil, 0)
	if err == nil {
		t.Fatal("expected error for nil record")
	}
}

func TestDecoderPool(t *testing.T) {
	entries := [][]byte{[]byte("pooled"), []byte("data")}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	compressed, err := zstd.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := append(compressed, forIndex...)

	rd := getDecoder()
	configTestDecoder(rd, len(entries), Compressed)
	if err := rd.Decode(data, 0); err != nil {
		putDecoder(rd)
		t.Fatal(err)
	}
	got := rd.EntryCopy(0)
	putDecoder(rd)

	if string(got) != "pooled" {
		t.Fatalf("got %q, want %q", got, "pooled")
	}
}

func TestEntryBoundsCheck(t *testing.T) {
	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	compressed, err := zstd.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	data := append(compressed, forIndex...)

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, 3, Compressed)
	if err := rd.Decode(data, 0); err != nil {
		t.Fatal(err)
	}

	// Negative index.
	assertPanics(t, "Entry(-1)", func() { rd.Entry(-1) })

	// Index == n (one past end).
	assertPanics(t, "Entry(3)", func() { rd.Entry(3) })

	// EntryCopy delegates to Entry, so same bounds check.
	assertPanics(t, "EntryCopy(-1)", func() { rd.EntryCopy(-1) })
	assertPanics(t, "EntryCopy(3)", func() { rd.EntryCopy(3) })
}

func assertPanics(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	f()
}

func TestDecoderNoForIndex(t *testing.T) {
	// When recordSize=1, the entire decompressed payload is one item (no FOR index).
	payload := []byte("single-item-payload")
	compressed, err := zstd.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, 1, Compressed)

	if err := rd.Decode(compressed, 0); err != nil {
		t.Fatal(err)
	}
	got := rd.Entry(0)
	if string(got) != string(payload) {
		t.Errorf("Entry(0) = %q, want %q", got, payload)
	}
}

func TestDecoderRaw(t *testing.T) {
	// When format=Raw: [payload][FOR index], no CRC on payload.
	entries := [][]byte{[]byte("raw"), []byte("data")}
	payload, sizes := buildPayload(entries)
	forIndex := buildForIndex(sizes)
	raw := append(append([]byte{}, payload...), forIndex...)

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, len(entries), Raw)

	if err := rd.Decode(raw, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries {
		got := rd.Entry(i)
		if string(got) != string(want) {
			t.Errorf("Entry(%d) = %q, want %q", i, got, want)
		}
	}
}

// --- Metadata tests ---

func TestItemsInRecord(t *testing.T) {
	tests := []struct {
		name       string
		total      int
		recordSize int
		recordIdx  int
		want       int
	}{
		{"full block", 300, 128, 0, 128},
		{"full block second", 300, 128, 1, 128},
		{"partial last block", 300, 128, 2, 44},
		{"exact multiple", 256, 128, 1, 128},
		{"single record", 50, 128, 0, 50},
		{"totalItems zero", 0, 128, 0, 0},
		{"small recordSize", 10, 3, 0, 3},
		{"small recordSize last", 10, 3, 3, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := itemsInRecord(tt.total, tt.recordSize, tt.recordIdx)
			if got != tt.want {
				t.Errorf("itemsInRecord(%d, %d, %d) = %d, want %d",
					tt.total, tt.recordSize, tt.recordIdx, got, tt.want)
			}
		})
	}
}

func TestDecoderReuse(t *testing.T) {
	// Decode a 5-entry record, then decode a 2-entry record on the same
	// decoder. Verifies that stale state from the first decode (larger
	// sizes/offsets slices) doesn't leak into the second.
	entries1 := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc"), []byte("dd"), []byte("e")}
	payload1, sizes1 := buildPayload(entries1)
	forIndex1 := buildForIndex(sizes1)
	comp1, err := zstd.Encode(payload1)
	if err != nil {
		t.Fatal(err)
	}
	data1 := append(comp1, forIndex1...)

	rd := newDecoder()
	defer rd.Close()
	configTestDecoder(rd, 5, Compressed)

	if err := rd.Decode(data1, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries1 {
		if got := string(rd.Entry(i)); got != string(want) {
			t.Errorf("first decode Entry(%d) = %q, want %q", i, got, want)
		}
	}

	// Second decode: fewer entries.
	entries2 := [][]byte{[]byte("xx"), []byte("yy")}
	payload2, sizes2 := buildPayload(entries2)
	forIndex2 := buildForIndex(sizes2)
	comp2, err := zstd.Encode(payload2)
	if err != nil {
		t.Fatal(err)
	}
	data2 := append(comp2, forIndex2...)

	rd.totalItems = 2
	rd.recordSize = 2
	if err := rd.Decode(data2, 0); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries2 {
		if got := string(rd.Entry(i)); got != string(want) {
			t.Errorf("second decode Entry(%d) = %q, want %q", i, got, want)
		}
	}

	// Bounds check: Entry(2) should panic after the second decode.
	assertPanics(t, "Entry(2) after shrink", func() { rd.Entry(2) })
}

func TestItemsInRecordPanics(t *testing.T) {
	// recordSize <= 0 should panic.
	assertPanics(t, "recordSize=0", func() { itemsInRecord(10, 0, 0) })
	assertPanics(t, "recordSize=-1", func() { itemsInRecord(10, -1, 0) })

	// recordIdx out of range should panic.
	assertPanics(t, "recordIdx=5", func() { itemsInRecord(300, 128, 5) })
	assertPanics(t, "recordIdx=-1", func() { itemsInRecord(300, 128, -1) })
}
