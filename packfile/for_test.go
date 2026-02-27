package packfile

import (
	"math"
	"testing"
)

func TestTrailingGroupRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		sizes  []uint32
		data   []byte // prepended before trailing group
	}{
		{
			name:  "uniform sizes",
			sizes: []uint32{100, 100, 100, 100},
			data:  make([]byte, 400),
		},
		{
			name:  "varying sizes",
			sizes: []uint32{10, 200, 50, 1000, 3},
			data:  make([]byte, 10+200+50+1000+3),
		},
		{
			name:  "single entry",
			sizes: []uint32{42},
			data:  make([]byte, 42),
		},
		{
			name:  "large values",
			sizes: []uint32{100000, 200000, 150000},
			data:  make([]byte, 100000+200000+150000),
		},
		{
			name:  "zeros not allowed but ones",
			sizes: []uint32{1, 1, 1},
			data:  make([]byte, 3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trailing := EncodeTrailingGroup(tt.sizes)

			// Build full buffer: data + trailing
			buf := make([]byte, len(tt.data)+len(trailing))
			copy(buf, tt.data)
			copy(buf[len(tt.data):], trailing)

			sizes, err := DecodeTrailingGroup(buf, len(tt.sizes), nil)
			if err != nil {
				t.Fatalf("DecodeTrailingGroup: %v", err)
			}
			if len(sizes) != len(tt.sizes) {
				t.Fatalf("len(sizes) = %d, want %d", len(sizes), len(tt.sizes))
			}
			for i := range tt.sizes {
				if sizes[i] != tt.sizes[i] {
					t.Errorf("sizes[%d] = %d, want %d", i, sizes[i], tt.sizes[i])
				}
			}
		})
	}
}

func TestTrailingGroupBufferReuse(t *testing.T) {
	sizes1 := []uint32{10, 20, 30}
	data1 := make([]byte, 60)
	trailing1 := EncodeTrailingGroup(sizes1)
	buf1 := append(data1, trailing1...)

	var reuseSizes []uint32

	reuseSizes, err := DecodeTrailingGroup(buf1, 3, reuseSizes)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}

	// Decode again with larger sizes, reusing the sizes buffer.
	sizes2 := []uint32{100, 200, 300, 400, 500}
	data2 := make([]byte, 1500)
	trailing2 := EncodeTrailingGroup(sizes2)
	buf2 := append(data2, trailing2...)

	reuseSizes, err = DecodeTrailingGroup(buf2, 5, reuseSizes)
	if err != nil {
		t.Fatalf("second decode: %v", err)
	}
	for i := range sizes2 {
		if reuseSizes[i] != sizes2[i] {
			t.Errorf("reuse sizes[%d] = %d, want %d", i, reuseSizes[i], sizes2[i])
		}
	}
}

func TestTrailingGroupTooSmall(t *testing.T) {
	_, err := DecodeTrailingGroup([]byte{1, 2, 3, 4, 5}, 1, nil)
	if err == nil {
		t.Fatal("expected error for data too small")
	}
}

func TestTrailingGroupSumMismatch(t *testing.T) {
	// Build valid trailing group for sizes [10, 20] but prepend wrong amount of data.
	trailing := EncodeTrailingGroup([]uint32{10, 20})
	buf := make([]byte, 99+len(trailing)) // 99 != 10+20=30
	copy(buf[99:], trailing)

	_, err := DecodeTrailingGroup(buf, 2, nil)
	if err == nil {
		t.Fatal("expected error for size sum mismatch")
	}
}

func TestEncodeTrailingGroupLayout(t *testing.T) {
	// Verify the trailing format is [min][packed][W], not [W][min][packed].
	sizes := []uint32{100, 100, 100}
	standard := EncodeGroup(sizes)
	trailing := EncodeTrailingGroup(sizes)

	if len(trailing) != len(standard) {
		t.Fatalf("length mismatch: trailing=%d standard=%d", len(trailing), len(standard))
	}

	// Last byte of trailing should be W (first byte of standard).
	if trailing[len(trailing)-1] != standard[0] {
		t.Errorf("trailing last byte = %d, want W = %d", trailing[len(trailing)-1], standard[0])
	}

	// First 4 bytes of trailing should be min (bytes 1-4 of standard).
	for i := range 4 {
		if trailing[i] != standard[i+1] {
			t.Errorf("trailing[%d] = %d, want min byte %d", i, trailing[i], standard[i+1])
		}
	}
}

func TestGroupRoundTrip(t *testing.T) {
	values := []uint32{10, 20, 30, 40, 50}
	encoded := EncodeGroup(values)

	// Add 7-byte padding for safe 8-byte reads.
	padded := make([]byte, len(encoded)+7)
	copy(padded, encoded)

	decoded, consumed := decodeGroup(padded, len(values), nil)
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
	encoded := EncodeGroup(values)

	padded := make([]byte, len(encoded)+7)
	copy(padded, encoded)

	decoded, consumed := decodeGroup(padded, len(values), nil)
	if consumed != len(encoded) {
		t.Fatalf("consumed %d bytes, want %d", consumed, len(encoded))
	}
	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("value[%d] = %d, want %d", i, v, values[i])
		}
	}
}
