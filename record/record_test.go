package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/zstd"
)

// Use the package-level crc32cTable directly (same package test).

// buildRecord constructs a raw record from entries: [entry0][entry1]...[trailing FOR sizes].
func buildRecord(entries [][]byte) []byte {
	sizes := make([]uint32, len(entries))
	var buf []byte
	for i, e := range entries {
		buf = append(buf, e...)
		sizes[i] = uint32(len(e))
	}
	buf = append(buf, intpack.EncodeTrailingGroup(sizes)...)
	return buf
}

func TestDecoderCompressed(t *testing.T) {
	entries := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("!"),
	}
	raw := buildRecord(entries)

	compressed, err := zstd.Encode(raw)
	if err != nil {
		t.Fatal(err)
	}

	rd := New()
	defer rd.Close()

	if err := rd.Decode(compressed, len(entries), true); err != nil {
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
	raw := buildRecord(entries)

	// Append CRC32C.
	crc := crc32.Checksum(raw, crc32cTable)
	raw = binary.LittleEndian.AppendUint32(raw, crc)

	rd := New()
	defer rd.Close()

	if err := rd.Decode(raw, len(entries), false); err != nil {
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
	entries := [][]byte{[]byte("data")}
	raw := buildRecord(entries)
	crc := crc32.Checksum(raw, crc32cTable)
	raw = binary.LittleEndian.AppendUint32(raw, crc)

	// Corrupt a byte in the payload.
	raw[0] ^= 0xFF

	rd := New()
	defer rd.Close()

	err := rd.Decode(raw, 1, false)
	if err == nil {
		t.Fatal("expected CRC32C error")
	}
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got: %v", err)
	}
}

func TestDecoderCRC32CShortRecord(t *testing.T) {
	rd := New()
	defer rd.Close()

	// 4 bytes = too short (need at least 5: 1 payload + 4 CRC).
	err := rd.Decode([]byte{1, 2, 3, 4}, 1, false)
	if err == nil {
		t.Fatal("expected error for short record")
	}

	// 0 bytes.
	err = rd.Decode(nil, 1, false)
	if err == nil {
		t.Fatal("expected error for nil record")
	}
}

func TestDecoderPool(t *testing.T) {
	entries := [][]byte{[]byte("pooled")}
	raw := buildRecord(entries)
	compressed, err := zstd.Encode(raw)
	if err != nil {
		t.Fatal(err)
	}

	rd := Get()
	if err := rd.Decode(compressed, 1, true); err != nil {
		Put(rd)
		t.Fatal(err)
	}
	got := rd.EntryCopy(0)
	Put(rd)

	if string(got) != "pooled" {
		t.Fatalf("got %q, want %q", got, "pooled")
	}
}

func TestEntryBoundsCheck(t *testing.T) {
	entries := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	raw := buildRecord(entries)
	compressed, err := zstd.Encode(raw)
	if err != nil {
		t.Fatal(err)
	}

	rd := New()
	defer rd.Close()
	if err := rd.Decode(compressed, 3, true); err != nil {
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
			got := ItemsInRecord(tt.total, tt.recordSize, tt.recordIdx)
			if got != tt.want {
				t.Errorf("ItemsInRecord(%d, %d, %d) = %d, want %d",
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
	raw1 := buildRecord(entries1)
	comp1, err := zstd.Encode(raw1)
	if err != nil {
		t.Fatal(err)
	}

	rd := New()
	defer rd.Close()

	if err := rd.Decode(comp1, 5, true); err != nil {
		t.Fatal(err)
	}
	for i, want := range entries1 {
		if got := string(rd.Entry(i)); got != string(want) {
			t.Errorf("first decode Entry(%d) = %q, want %q", i, got, want)
		}
	}

	// Second decode: fewer entries.
	entries2 := [][]byte{[]byte("xx"), []byte("yy")}
	raw2 := buildRecord(entries2)
	comp2, err := zstd.Encode(raw2)
	if err != nil {
		t.Fatal(err)
	}

	if err := rd.Decode(comp2, 2, true); err != nil {
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
	assertPanics(t, "recordSize=0", func() { ItemsInRecord(10, 0, 0) })
	assertPanics(t, "recordSize=-1", func() { ItemsInRecord(10, -1, 0) })

	// recordIdx out of range should panic.
	assertPanics(t, "recordIdx=5", func() { ItemsInRecord(300, 128, 5) })
	assertPanics(t, "recordIdx=-1", func() { ItemsInRecord(300, 128, -1) })
}

func TestEncodeDecodeMetadata(t *testing.T) {
	tests := []struct {
		totalItems int
		recordSize int
		flags      uint32
	}{
		{1000, 128, 0},
		{0, 64, FlagNoCompression},
		{8700000, 256, 0},
		{1, 1, 0xFFFFFFFF},
	}
	for _, tt := range tests {
		total, recSize, flags, err := DecodeMetadata(EncodeMetadata(tt.totalItems, tt.recordSize, tt.flags))
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}
		if total != tt.totalItems || recSize != tt.recordSize || flags != tt.flags {
			t.Errorf("round-trip mismatch: got (%d, %d, %d), want (%d, %d, %d)",
				total, recSize, flags, tt.totalItems, tt.recordSize, tt.flags)
		}
	}

	// Too-short metadata.
	_, _, _, err := DecodeMetadata(make([]byte, 11))
	if err == nil {
		t.Error("expected error for short metadata")
	}
}
