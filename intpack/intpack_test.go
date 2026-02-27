package intpack_test

import (
	"math"
	"testing"

	"github.com/tamir/events-analysis/intpack"
)

func TestTrailingGroupRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		sizes []uint32
		data  []byte // prepended before trailing group
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
			trailing := intpack.EncodeTrailingGroup(tt.sizes)

			// Build full buffer: data + trailing
			buf := make([]byte, len(tt.data)+len(trailing))
			copy(buf, tt.data)
			copy(buf[len(tt.data):], trailing)

			sizes, err := intpack.DecodeTrailingGroup(buf, len(tt.sizes), nil)
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
	trailing1 := intpack.EncodeTrailingGroup(sizes1)
	buf1 := append(data1, trailing1...)

	var reuseSizes []uint32

	reuseSizes, err := intpack.DecodeTrailingGroup(buf1, 3, reuseSizes)
	if err != nil {
		t.Fatalf("first decode: %v", err)
	}

	// Decode again with larger sizes, reusing the sizes buffer.
	sizes2 := []uint32{100, 200, 300, 400, 500}
	data2 := make([]byte, 1500)
	trailing2 := intpack.EncodeTrailingGroup(sizes2)
	buf2 := append(data2, trailing2...)

	reuseSizes, err = intpack.DecodeTrailingGroup(buf2, 5, reuseSizes)
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
	_, err := intpack.DecodeTrailingGroup([]byte{1, 2, 3, 4, 5}, 1, nil)
	if err == nil {
		t.Fatal("expected error for data too small")
	}
}

func TestTrailingGroupSumMismatch(t *testing.T) {
	// Build valid trailing group for sizes [10, 20] but prepend wrong amount of data.
	trailing := intpack.EncodeTrailingGroup([]uint32{10, 20})
	buf := make([]byte, 99+len(trailing)) // 99 != 10+20=30
	copy(buf[99:], trailing)

	_, err := intpack.DecodeTrailingGroup(buf, 2, nil)
	if err == nil {
		t.Fatal("expected error for size sum mismatch")
	}
}

func TestEncodeTrailingGroupLayout(t *testing.T) {
	// Verify the trailing format is [min][packed][W], not [W][min][packed].
	sizes := []uint32{100, 100, 100}
	standard := intpack.EncodeGroup(sizes)
	trailing := intpack.EncodeTrailingGroup(sizes)

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
	encoded := intpack.EncodeGroup(values)

	// Add 7-byte padding for safe 8-byte reads.
	padded := make([]byte, len(encoded)+7)
	copy(padded, encoded)

	decoded, consumed, err := intpack.DecodeGroup(padded, len(values), nil)
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

	padded := make([]byte, len(encoded)+7)
	copy(padded, encoded)

	decoded, consumed, err := intpack.DecodeGroup(padded, len(values), nil)
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
	// Invalid width (> 32).
	data := make([]byte, 12)
	data[0] = 33 // width = 33
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

	// Truncated payload: valid header but packed residuals cut short.
	// Encode 128 values (width > 0) then truncate to just the header.
	vals := make([]uint32, 128)
	for i := range vals {
		vals[i] = uint32(i)
	}
	encoded := intpack.EncodeGroup(vals)
	truncated := make([]byte, 6+7) // 5-byte header + 1 payload byte + 7 overshoot
	copy(truncated, encoded[:6])
	_, _, err = intpack.DecodeGroup(truncated, 128, nil)
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}

	// DecodeTrailingGroup with n <= 0.
	_, err = intpack.DecodeTrailingGroup(make([]byte, 10), 0, nil)
	if err == nil {
		t.Fatal("expected error for DecodeTrailingGroup n=0")
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
			// Pad for safe 8-byte overshoot reads.
			padded := make([]byte, len(encoded)+7)
			copy(padded, encoded)

			var consumed int
			var err error
			dst, consumed, err = intpack.DecodeGroup(padded, len(tt.values), dst)
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
