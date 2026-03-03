package intpack_test

import (
	"math"
	"testing"

	"github.com/tamir/events-analysis/intpack"
)

func TestGroupRoundTrip(t *testing.T) {
	values := []uint32{10, 20, 30, 40, 50}
	encoded := intpack.EncodeGroup(values)

	decoded, consumed, err := intpack.DecodeGroup(encoded, len(values), nil)
	if err != nil {
		t.Fatalf("DecodeGroup: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
	}
	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("value[%d] = %d, want %d", i, v, values[i])
		}
	}
}

func TestWidth32(t *testing.T) {
	values := []uint32{0, math.MaxUint32}
	encoded := intpack.EncodeGroup(values)

	decoded, consumed, err := intpack.DecodeGroup(encoded, len(values), nil)
	if err != nil {
		t.Fatalf("DecodeGroup: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
	}
	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("value[%d] = %d, want %d", i, v, values[i])
		}
	}
}

func TestDecodeGroupErrors(t *testing.T) {
	// Invalid width (> 32): set the 5th-from-last byte to 33.
	data := make([]byte, 12)
	data[len(data)-5] = 33 // width = 33
	_, _, err := intpack.DecodeGroup(data, 1, nil)
	if err == nil {
		t.Fatal("expected error for width > 32")
	}

	// Data too short (< 5 bytes).
	_, _, err = intpack.DecodeGroup([]byte{1, 2, 3, 4}, 1, nil)
	if err == nil {
		t.Fatal("expected error for short data")
	}

	// n <= 0.
	_, _, err = intpack.DecodeGroup(data, 0, nil)
	if err == nil {
		t.Fatal("expected error for n=0")
	}
	_, _, err = intpack.DecodeGroup(data, -1, nil)
	if err == nil {
		t.Fatal("expected error for n=-1")
	}

	// Truncated payload: valid footer but packed residuals cut short.
	// Encode 128 values (width=7, packSize=112), then keep only last 6 bytes
	// (1 packed byte + 1B width + 4B min). DecodeGroup needs 112+5=117 bytes.
	vals := make([]uint32, 128)
	for i := range vals {
		vals[i] = uint32(i)
	}
	encoded := intpack.EncodeGroup(vals)
	truncated := encoded[len(encoded)-6:] // [1 packed byte][width][min]
	_, _, err = intpack.DecodeGroup(truncated, 128, nil)
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

func TestFORRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []uint32
	}{
		{"uniform", []uint32{100, 100, 100, 100}},
		{"ascending", []uint32{10, 20, 30, 40, 50}},
		{"single", []uint32{42}},
		{"wide_range", []uint32{0, 1, 1000000}},
		{"max_group", func() []uint32 {
			v := make([]uint32, 128)
			for i := range v {
				v[i] = uint32(i * 7)
			}
			return v
		}()},
	}

	var dst []uint32
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := intpack.EncodeGroup(tt.values)

			var consumed int
			var err error
			dst, consumed, err = intpack.DecodeGroup(encoded, len(tt.values), dst)
			if err != nil {
				t.Fatalf("DecodeGroup: %v", err)
			}
			if consumed != len(encoded) {
				t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
			}
			if len(dst) != len(tt.values) {
				t.Fatalf("decoded %d values, want %d", len(dst), len(tt.values))
			}
			for i, v := range dst {
				if v != tt.values[i] {
					t.Fatalf("value[%d] = %d, want %d", i, v, tt.values[i])
				}
			}
		})
	}
}

func TestDecodeGroupBufferReuse(t *testing.T) {
	// Decode a 3-value group, then a 5-value group, reusing dst.
	sizes1 := []uint32{10, 20, 30}
	encoded1 := intpack.EncodeGroup(sizes1)

	var dst []uint32
	var err error
	dst, _, err = intpack.DecodeGroup(encoded1, len(sizes1), dst)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}

	sizes2 := []uint32{100, 200, 300, 400, 500}
	encoded2 := intpack.EncodeGroup(sizes2)
	dst, _, err = intpack.DecodeGroup(encoded2, len(sizes2), dst)
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	for i := range sizes2 {
		if dst[i] != sizes2[i] {
			t.Errorf("dst[%d] = %d, want %d", i, dst[i], sizes2[i])
		}
	}
}

func TestEncodeGroupLayout(t *testing.T) {
	// Verify the layout is [packed][1B W][4B min LE].
	sizes := []uint32{100, 200, 300}
	encoded := intpack.EncodeGroup(sizes)

	// The last 5 bytes are [W][min(4)].
	// W = bits.Len32(300-100) = bits.Len32(200) = 8.
	wantW := uint8(8)
	wantMin := uint32(100)

	gotW := encoded[len(encoded)-5]
	if gotW != wantW {
		t.Errorf("width byte = %d, want %d", gotW, wantW)
	}

	gotMin := uint32(encoded[len(encoded)-4]) |
		uint32(encoded[len(encoded)-3])<<8 |
		uint32(encoded[len(encoded)-2])<<16 |
		uint32(encoded[len(encoded)-1])<<24
	if gotMin != wantMin {
		t.Errorf("min = %d, want %d", gotMin, wantMin)
	}
}

func TestDecodeGroupWithPrefix(t *testing.T) {
	// DecodeGroup reads from the tail — a prefix of arbitrary bytes is ignored.
	values := []uint32{5, 10, 15}
	encoded := intpack.EncodeGroup(values)

	// Prepend 20 bytes of arbitrary data.
	prefix := make([]byte, 20)
	for i := range prefix {
		prefix[i] = 0xAB
	}
	buf := append(prefix, encoded...)

	// Pass buf[:20+len(encoded)] = the full buffer; DecodeGroup reads from tail.
	decoded, consumed, err := intpack.DecodeGroup(buf, len(values), nil)
	if err != nil {
		t.Fatalf("DecodeGroup with prefix: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("consumed %d, want %d (only the encoded bytes)", consumed, len(encoded))
	}
	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("value[%d] = %d, want %d", i, v, values[i])
		}
	}
}
