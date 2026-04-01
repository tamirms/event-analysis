package packfile

import (
	"math"
	"testing"
)

func TestFORGroupRoundTrip(t *testing.T) {
	values := []uint32{10, 20, 30, 40, 50}
	encoded := encodeGroup(values)

	decoded, consumed, err := decodeGroup(encoded, len(values), nil)
	if err != nil {
		t.Fatalf("decodeGroup: %v", err)
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

func TestFORWidth32(t *testing.T) {
	values := []uint32{0, math.MaxUint32}
	encoded := encodeGroup(values)

	decoded, consumed, err := decodeGroup(encoded, len(values), nil)
	if err != nil {
		t.Fatalf("decodeGroup: %v", err)
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

func TestFORdecodeGroupErrors(t *testing.T) {
	// Invalid width (> 32): set the 5th-from-last byte to 33.
	data := make([]byte, 12)
	data[len(data)-5] = 33 // width = 33
	_, _, err := decodeGroup(data, 1, nil)
	if err == nil {
		t.Fatal("expected error for width > 32")
	}

	// Data too short (< 5 bytes).
	_, _, err = decodeGroup([]byte{1, 2, 3, 4}, 1, nil)
	if err == nil {
		t.Fatal("expected error for short data")
	}

	// n <= 0.
	_, _, err = decodeGroup(data, 0, nil)
	if err == nil {
		t.Fatal("expected error for n=0")
	}
	_, _, err = decodeGroup(data, -1, nil)
	if err == nil {
		t.Fatal("expected error for n=-1")
	}

	// Truncated payload: valid footer but packed residuals cut short.
	// Encode 128 values (width=7, packSize=112), then keep only last 6 bytes
	// (1 packed byte + 1B width + 4B min). decodeGroup needs 112+5=117 bytes.
	vals := make([]uint32, 128)
	for i := range vals {
		vals[i] = uint32(i)
	}
	encoded := encodeGroup(vals)
	truncated := encoded[len(encoded)-6:] // [1 packed byte][width][min]
	_, _, err = decodeGroup(truncated, 128, nil)
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
			encoded := encodeGroup(tt.values)

			var consumed int
			var err error
			dst, consumed, err = decodeGroup(encoded, len(tt.values), dst)
			if err != nil {
				t.Fatalf("decodeGroup: %v", err)
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

func TestFORdecodeGroupBufferReuse(t *testing.T) {
	sizes1 := []uint32{10, 20, 30}
	encoded1 := encodeGroup(sizes1)

	var dst []uint32
	var err error
	dst, _, err = decodeGroup(encoded1, len(sizes1), dst)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}

	sizes2 := []uint32{100, 200, 300, 400, 500}
	encoded2 := encodeGroup(sizes2)
	dst, _, err = decodeGroup(encoded2, len(sizes2), dst)
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	for i := range sizes2 {
		if dst[i] != sizes2[i] {
			t.Errorf("dst[%d] = %d, want %d", i, dst[i], sizes2[i])
		}
	}
}

func TestFORencodeGroupLayout(t *testing.T) {
	sizes := []uint32{100, 200, 300}
	encoded := encodeGroup(sizes)

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

func TestFORdecodeGroupWithPrefix(t *testing.T) {
	values := []uint32{5, 10, 15}
	encoded := encodeGroup(values)

	prefix := make([]byte, 20)
	for i := range prefix {
		prefix[i] = 0xAB
	}
	buf := append(prefix, encoded...)

	decoded, consumed, err := decodeGroup(buf, len(values), nil)
	if err != nil {
		t.Fatalf("decodeGroup with prefix: %v", err)
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
