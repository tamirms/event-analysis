package packfile

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// EncodeGroup FOR-encodes values into one group: [1B W][4B min LE][packed residuals].
// W = bits.Len32(max - min), clamped to min 1. Pure codec — no CRC, no trailer.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte {
	minVal, width := rangeWidth(values)
	packSize := (int(width)*len(values) + 7) / 8
	buf := make([]byte, 5+packSize+7) // header + packed + 7 overshoot for safe writes

	buf[0] = width
	binary.LittleEndian.PutUint32(buf[1:], minVal)
	packResiduals(buf, 5, values, minVal, width)

	return buf[:5+packSize]
}

// decodeGroup FOR-decodes one group of n values from data into dst.
// Returns values (possibly reallocated if dst is too small) and bytes consumed.
// Panics if n <= 0. data must have 7 bytes of overshoot past the encoded
// payload for safe 8-byte reads.
func decodeGroup(data []byte, n int, dst []uint32) (values []uint32, size int) {
	dst = ensureCap(dst, n)
	w := uint64(data[0])
	if w > 32 {
		panic(fmt.Sprintf("packfile: invalid FOR width %d (max 32)", w))
	}
	groupMin := binary.LittleEndian.Uint32(data[1:])
	packSize := (int(w)*n + 7) / 8
	unpackResiduals(data[5:], n, w, groupMin, dst)
	return dst, 5 + packSize
}

// EncodeTrailingGroup FOR-encodes values into trailing format:
// [4B min LE][packed residuals][1B W]
// W is the last byte, suitable for appending after variable-length data.
// Panics if len(values) == 0.
func EncodeTrailingGroup(values []uint32) []byte {
	minVal, width := rangeWidth(values)
	packSize := (int(width)*len(values) + 7) / 8
	buf := make([]byte, 4+packSize+1+7) // min + packed + W + 7 overshoot for safe writes

	binary.LittleEndian.PutUint32(buf[0:], minVal)
	packResiduals(buf, 4, values, minVal, width)
	buf[4+packSize] = width

	return buf[:4+packSize+1]
}

// DecodeTrailingGroup decodes a trailing FOR group from the end of data.
// The trailing format is [4B min LE][packed residuals][1B W] appended after
// variable-length data entries. n is the number of values.
// Returns decoded sizes (possibly reallocated). Validates that sum(sizes)
// equals the data length preceding the trailing group.
func DecodeTrailingGroup(data []byte, n int, sizes []uint32) ([]uint32, error) {
	if len(data) < 6 { // minimum: 1 data byte + 4B min + 1B W
		return sizes, fmt.Errorf("packfile: data too small for trailing FOR group (%d bytes)", len(data))
	}

	w := uint64(data[len(data)-1]) // W is the last byte
	if w > 32 {
		return sizes, fmt.Errorf("packfile: invalid FOR width %d in trailing group (max 32)", w)
	}
	packSize := (int(w)*n + 7) / 8
	indexSize := 4 + packSize + 1 // min(4) + packed + W(1)

	if indexSize > len(data) {
		return sizes, fmt.Errorf("packfile: trailing FOR index size %d exceeds data size %d", indexSize, len(data))
	}

	dataEnd := len(data) - indexSize
	groupMin := binary.LittleEndian.Uint32(data[dataEnd:])
	packed := data[dataEnd+4 : len(data)-1]

	sizes = ensureCap(sizes, n)
	unpackResiduals(packed, n, w, groupMin, sizes)

	// Validate sum(sizes) == dataEnd.
	sum := 0
	for _, s := range sizes {
		sum += int(s)
	}
	if sum != dataEnd {
		return sizes, fmt.Errorf("packfile: trailing FOR size sum %d != data end %d", sum, dataEnd)
	}

	return sizes, nil
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
	safeLimit := len(packed) - 7 // bytePos < safeLimit → 8-byte read is in bounds
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
