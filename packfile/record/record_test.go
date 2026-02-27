package record

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

// buildRecord constructs a raw record from entries: [entry0][entry1]...[trailing FOR sizes].
func buildRecord(entries [][]byte) []byte {
	sizes := make([]uint32, len(entries))
	var buf []byte
	for i, e := range entries {
		buf = append(buf, e...)
		sizes[i] = uint32(len(e))
	}
	buf = append(buf, packfile.EncodeTrailingGroup(sizes)...)
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
	crc := packfile.CRC32C(raw)
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
	crc := packfile.CRC32C(raw)
	raw = binary.LittleEndian.AppendUint32(raw, crc)

	// Corrupt a byte in the payload.
	raw[0] ^= 0xFF

	rd := New()
	defer rd.Close()

	err := rd.Decode(raw, 1, false)
	if err == nil {
		t.Fatal("expected CRC32C error")
	}
	if !errors.Is(err, packfile.ErrChecksum) {
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
