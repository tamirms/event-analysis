// Package intpack implements FOR (Frame-of-Reference) integer encoding
// for groups of uint32 values. Values are encoded as bit-packed residuals
// relative to the group minimum, requiring only ceil(log2(max-min)) bits
// per value.
//
// Layout: [packed residuals][1B width][4B min LE]
// Width and min are always the final 5 bytes, so callers can locate metadata
// from the tail of any buffer without knowing the packed size upfront.
package intpack

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// EncodeGroup FOR-encodes values into one group: [packed residuals][1B W][4B min LE].
// W = bits.Len32(max - min), clamped to min 1. Pure codec — no CRC, no trailer.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte {
	minVal, width := rangeWidth(values)
	packSize := (int(width)*len(values) + 7) / 8
	buf := make([]byte, packSize+5+7) // packed + footer(1B W, 4B min) + 7 overshoot for safe writes
	packResiduals(buf, 0, values, minVal, width)
	buf[packSize] = width
	binary.LittleEndian.PutUint32(buf[packSize+1:], minVal)
	return buf[:packSize+5]
}

// DecodeGroup FOR-decodes one group of n values from the tail of buf.
// buf must end at the last byte of [min] (the byte before any trailing CRC or other data).
// Returns decoded values (written into dst[0:n], reallocating if cap(dst) < n),
// bytes consumed from the tail, and any error.
func DecodeGroup(buf []byte, n int, dst []uint32) (values []uint32, consumed int, err error) {
	if n <= 0 {
		return dst, 0, fmt.Errorf("intpack: DecodeGroup n must be > 0, got %d", n)
	}
	if len(buf) < 5 {
		return dst, 0, fmt.Errorf("intpack: DecodeGroup buf too short (%d bytes, need >= 5)", len(buf))
	}
	width := uint64(buf[len(buf)-5])
	if width > 32 {
		return dst, 0, fmt.Errorf("intpack: invalid FOR width %d (max 32)", width)
	}
	groupMin := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	packSize := (int(width)*n + 7) / 8
	consumed = packSize + 5
	if len(buf) < consumed {
		return dst, 0, fmt.Errorf("intpack: DecodeGroup buf too short for payload (%d bytes, need >= %d)", len(buf), consumed)
	}
	dst = ensureCap(dst, n)
	unpackResiduals(buf[len(buf)-5-packSize:len(buf)-5], n, width, groupMin, dst)
	return dst, consumed, nil
}

// rangeWidth computes the minimum value and the bit width needed to
// FOR-encode values. Width is clamped to at least 1.
func rangeWidth(values []uint32) (minVal uint32, width uint8) {
	minVal = values[0]
	maxVal := values[0]
	for _, v := range values[1:] {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	width = uint8(bits.Len32(maxVal - minVal))
	if width == 0 {
		width = 1
	}
	return
}

// packResiduals bit-packs (values[i] - minVal) into buf starting at the given
// byte offset. buf must have 7 bytes of overshoot past the packed payload for
// safe 8-byte writes.
func packResiduals(buf []byte, offset int, values []uint32, minVal uint32, width uint8) {
	for j, v := range values {
		residual := uint64(v - minVal)
		bitPos := uint64(j) * uint64(width)
		bytePos := uint64(offset) + bitPos/8
		shift := bitPos % 8
		existing := binary.LittleEndian.Uint64(buf[bytePos:])
		binary.LittleEndian.PutUint64(buf[bytePos:], existing|(residual<<shift))
	}
}

// unpackResiduals unpacks n bit-packed residuals from packed and adds groupMin.
// Safe to call with any packed length — elements near the boundary where an
// 8-byte read would exceed len(packed) are decoded byte-by-byte.
func unpackResiduals(packed []byte, n int, w uint64, groupMin uint32, values []uint32) {
	if w == 0 {
		for i := range values {
			values[i] = groupMin
		}
		return
	}
	mask := uint64((1 << w) - 1)
	// When len(packed) < 7, safeLimit is negative — all reads use the
	// byte-by-byte fallback path since int(bytePos) is always >= 0.
	safeLimit := len(packed) - 7
	for j := range n {
		bitPos := uint64(j) * w
		bytePos := bitPos / 8
		shift := bitPos % 8
		var raw uint64
		if int(bytePos) < safeLimit {
			raw = binary.LittleEndian.Uint64(packed[bytePos:])
		} else {
			for k := 0; k < 8 && int(bytePos)+k < len(packed); k++ {
				raw |= uint64(packed[int(bytePos)+k]) << (k * 8)
			}
		}
		values[j] = groupMin + uint32((raw>>shift)&mask)
	}
}

func ensureCap(s []uint32, n int) []uint32 {
	if cap(s) < n {
		return make([]uint32, n)
	}
	return s[:n]
}
