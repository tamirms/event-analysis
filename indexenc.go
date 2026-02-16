package main

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"

	"github.com/klauspost/compress/zstd"
)

// --- Helpers ---

// deltaEncode computes deltas[i] = uint32(offsets[i+1] - offsets[i]).
func deltaEncode(offsets []int64) []uint32 {
	if len(offsets) < 2 {
		return nil
	}
	deltas := make([]uint32, len(offsets)-1)
	for i := range deltas {
		deltas[i] = uint32(offsets[i+1] - offsets[i])
	}
	return deltas
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// zstd encoder/decoder for data generation (kept for test data generation).
var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic("failed to create zstd encoder: " + err.Error())
	}
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("failed to create zstd decoder: " + err.Error())
	}
}

// --- Approach A: Global W with anchors ---

// GlobalWEncode encodes deltas with a single global bit width and 8-byte anchors per group.
// Per group: 8B anchor + 1B width + packed bits.
func GlobalWEncode(deltas []uint32, baseOffsets []int64, grpSize int) []byte {
	nGroups := (len(deltas) + grpSize - 1) / grpSize

	// Compute global W
	var globalMax uint32
	for _, d := range deltas {
		if d > globalMax {
			globalMax = d
		}
	}
	globalW := bits.Len32(globalMax)

	buf := make([]byte, 0, nGroups*(9+grpSize*4))

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]

		// Anchor (8 bytes)
		var anchor [8]byte
		binary.LittleEndian.PutUint64(anchor[:], uint64(baseOffsets[g]))
		buf = append(buf, anchor[:]...)

		// Width (1 byte) — always globalW
		buf = append(buf, byte(globalW))

		if globalW == 0 {
			continue
		}

		// Pack bits
		count := len(groupDeltas)
		packedBytes := (globalW*count + 7) / 8
		packed := make([]byte, packedBytes+8)

		bitPos := 0
		for _, d := range groupDeltas {
			byteIdx := bitPos >> 3
			shift := uint(bitPos & 7)
			cur := binary.LittleEndian.Uint64(packed[byteIdx:])
			cur |= uint64(d) << shift
			binary.LittleEndian.PutUint64(packed[byteIdx:], cur)
			bitPos += globalW
		}
		buf = append(buf, packed[:packedBytes]...)
	}

	return buf
}

// GlobalWDecode decodes global-W encoded data back into absolute offsets.
// Returns []int64 of length n+1.
func GlobalWDecode(data []byte, n int, grpSize int) []int64 {
	offsets := make([]int64, n+1)
	pos := 0
	outIdx := 0
	remaining := n

	var padBuf [512*4 + 8]byte // max group size 512 × 32 bits + padding

	var lastOffset int64
	for remaining > 0 {
		anchor := int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		width := int(data[pos])
		pos++

		count := grpSize
		if remaining < count {
			count = remaining
		}

		offset := anchor
		if width == 0 {
			for i := 0; i < count; i++ {
				offsets[outIdx] = offset
				outIdx++
			}
		} else {
			packedBytes := (width*count + 7) / 8
			copy(padBuf[:packedBytes], data[pos:pos+packedBytes])
			for i := packedBytes; i < packedBytes+8; i++ {
				padBuf[i] = 0
			}
			pos += packedBytes

			mask := uint64((1 << width) - 1)
			bitPos := 0
			for i := 0; i < count; i++ {
				byteIdx := bitPos >> 3
				shift := uint(bitPos & 7)
				word := binary.LittleEndian.Uint64(padBuf[byteIdx:])
				d := uint32((word >> shift) & mask)
				offsets[outIdx] = offset
				outIdx++
				offset += int64(d)
				bitPos += width
			}
		}
		lastOffset = offset
		remaining -= count
	}

	offsets[n] = lastOffset
	return offsets
}

// GlobalWDecodeWithAnchors decodes and cross-validates anchors against accumulated offsets.
// Returns offsets and whether validation passed.
func GlobalWDecodeWithAnchors(data []byte, n int, grpSize int) ([]int64, bool) {
	offsets := make([]int64, n+1)
	pos := 0
	outIdx := 0
	remaining := n

	var padBuf [512*4 + 8]byte

	var lastOffset int64
	valid := true
	groupIdx := 0
	for remaining > 0 {
		anchor := int64(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		width := int(data[pos])
		pos++

		count := grpSize
		if remaining < count {
			count = remaining
		}

		// Validate: anchor should match accumulated offset (except first group)
		if groupIdx > 0 && anchor != lastOffset {
			valid = false
		}

		offset := anchor
		if width == 0 {
			for i := 0; i < count; i++ {
				offsets[outIdx] = offset
				outIdx++
			}
		} else {
			packedBytes := (width*count + 7) / 8
			copy(padBuf[:packedBytes], data[pos:pos+packedBytes])
			for i := packedBytes; i < packedBytes+8; i++ {
				padBuf[i] = 0
			}
			pos += packedBytes

			mask := uint64((1 << width) - 1)
			bitPos := 0
			for i := 0; i < count; i++ {
				byteIdx := bitPos >> 3
				shift := uint(bitPos & 7)
				word := binary.LittleEndian.Uint64(padBuf[byteIdx:])
				d := uint32((word >> shift) & mask)
				offsets[outIdx] = offset
				outIdx++
				offset += int64(d)
				bitPos += width
			}
		}
		lastOffset = offset
		remaining -= count
		groupIdx++
	}

	offsets[n] = lastOffset
	return offsets, valid
}

// --- Approach B: Per-group W with CRC32C checksum ---

// PerGroupWEncode encodes deltas with per-group bit widths and no anchors.
// Layout: [W₀][packed₀][W₁][packed₁]...[CRC32C][trailer]
// CRC32C covers the packed blob bytes (before CRC+trailer).
// Trailer: totalRecords(4) + groupSize(2) + indexLength(4)
func PerGroupWEncode(deltas []uint32, grpSize int) []byte {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	buf := make([]byte, 0, nGroups*(1+grpSize*4))

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]

		var maxDelta uint32
		for _, d := range groupDeltas {
			if d > maxDelta {
				maxDelta = d
			}
		}
		w := bits.Len32(maxDelta)

		buf = append(buf, byte(w))

		if w == 0 {
			continue
		}

		count := len(groupDeltas)
		packedBytes := (w*count + 7) / 8
		packed := make([]byte, packedBytes+8)

		bitPos := 0
		for _, d := range groupDeltas {
			byteIdx := bitPos >> 3
			shift := uint(bitPos & 7)
			cur := binary.LittleEndian.Uint64(packed[byteIdx:])
			cur |= uint64(d) << shift
			binary.LittleEndian.PutUint64(packed[byteIdx:], cur)
			bitPos += w
		}
		buf = append(buf, packed[:packedBytes]...)
	}

	// CRC32C over the packed blob bytes
	crc := crc32.Checksum(buf, crc32cTable)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	buf = append(buf, crcBuf[:]...)

	// Trailer: totalRecords(4) + groupSize(2) + indexLength(4)
	var trailer [10]byte
	binary.LittleEndian.PutUint32(trailer[0:], uint32(len(deltas)))
	binary.LittleEndian.PutUint16(trailer[4:], uint16(grpSize))
	binary.LittleEndian.PutUint32(trailer[6:], uint32(len(buf)+10)) // total length including trailer
	buf = append(buf, trailer[:]...)

	return buf
}

// PerGroupWDecode decodes per-group-W encoded data back into absolute offsets.
// Returns []int64 of length n+1.
func PerGroupWDecode(data []byte, n int, grpSize int) []int64 {
	offsets := make([]int64, n+1)
	pos := 0
	outIdx := 0
	remaining := n

	var padBuf [512*4 + 8]byte

	var offset int64
	for remaining > 0 {
		w := int(data[pos])
		pos++

		count := grpSize
		if remaining < count {
			count = remaining
		}

		if w == 0 {
			for i := 0; i < count; i++ {
				offsets[outIdx] = offset
				outIdx++
			}
		} else {
			packedBytes := (w*count + 7) / 8
			copy(padBuf[:packedBytes], data[pos:pos+packedBytes])
			for i := packedBytes; i < packedBytes+8; i++ {
				padBuf[i] = 0
			}
			pos += packedBytes

			mask := uint64((1 << w) - 1)
			bitPos := 0
			for i := 0; i < count; i++ {
				byteIdx := bitPos >> 3
				shift := uint(bitPos & 7)
				word := binary.LittleEndian.Uint64(padBuf[byteIdx:])
				d := uint32((word >> shift) & mask)
				offsets[outIdx] = offset
				outIdx++
				offset += int64(d)
				bitPos += w
			}
		}
		remaining -= count
	}

	offsets[n] = offset
	return offsets
}

// PerGroupWDecodeWithCRC verifies CRC32C over the packed blob, then decodes.
// Returns offsets and whether the CRC matched.
func PerGroupWDecodeWithCRC(data []byte, n int, grpSize int) ([]int64, bool) {
	// Strip trailer (10 bytes) and CRC (4 bytes) from end
	trailerStart := len(data) - 10
	crcStart := trailerStart - 4

	storedCRC := binary.LittleEndian.Uint32(data[crcStart:trailerStart])
	computedCRC := crc32.Checksum(data[:crcStart], crc32cTable)

	offsets := PerGroupWDecode(data[:crcStart], n, grpSize)
	return offsets, computedCRC == storedCRC
}

// PerGroupWEncodeSize computes the encoded size without actually encoding.
func PerGroupWEncodeSize(deltas []uint32, grpSize int) (headerBytes, dataBytes, totalBytes int) {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	headerBytes = nGroups // 1 byte W per group

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}

		var maxDelta uint32
		for _, d := range deltas[start:end] {
			if d > maxDelta {
				maxDelta = d
			}
		}
		w := bits.Len32(maxDelta)
		count := end - start
		dataBytes += (w*count + 7) / 8
	}

	totalBytes = headerBytes + dataBytes + 4 + 10 // +CRC +trailer
	return
}

// --- Approach C: Per-group Frame-of-Reference (FOR) ---

// FOREncode encodes deltas using per-group frame-of-reference.
// Per group: [1B width][4B groupMin LE][packed residuals at W' bits each]
// Residual[i] = delta[i] - groupMin; W' = bits.Len32(groupMax - groupMin).
// Appends CRC32C + trailer.
func FOREncode(deltas []uint32, grpSize int) []byte {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	buf := make([]byte, 0, nGroups*(5+grpSize*4))

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]

		var minDelta, maxDelta uint32
		minDelta = groupDeltas[0]
		maxDelta = groupDeltas[0]
		for _, d := range groupDeltas[1:] {
			if d < minDelta {
				minDelta = d
			}
			if d > maxDelta {
				maxDelta = d
			}
		}
		w := bits.Len32(maxDelta - minDelta)

		buf = append(buf, byte(w))
		var minBuf [4]byte
		binary.LittleEndian.PutUint32(minBuf[:], minDelta)
		buf = append(buf, minBuf[:]...)

		if w == 0 {
			continue
		}

		count := len(groupDeltas)
		packedBytes := (w*count + 7) / 8
		packed := make([]byte, packedBytes+8)

		bitPos := 0
		for _, d := range groupDeltas {
			residual := d - minDelta
			byteIdx := bitPos >> 3
			shift := uint(bitPos & 7)
			cur := binary.LittleEndian.Uint64(packed[byteIdx:])
			cur |= uint64(residual) << shift
			binary.LittleEndian.PutUint64(packed[byteIdx:], cur)
			bitPos += w
		}
		buf = append(buf, packed[:packedBytes]...)
	}

	// CRC32C over the packed blob bytes
	crc := crc32.Checksum(buf, crc32cTable)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	buf = append(buf, crcBuf[:]...)

	// Trailer: totalRecords(4) + groupSize(2) + indexLength(4)
	var trailer [10]byte
	binary.LittleEndian.PutUint32(trailer[0:], uint32(len(deltas)))
	binary.LittleEndian.PutUint16(trailer[4:], uint16(grpSize))
	binary.LittleEndian.PutUint32(trailer[6:], uint32(len(buf)+10))
	buf = append(buf, trailer[:]...)

	return buf
}

// FORDecode decodes FOR-encoded data back into absolute offsets.
// Returns []int64 of length n+1.
func FORDecode(data []byte, n int, grpSize int) []int64 {
	offsets := make([]int64, n+1)
	pos := 0
	outIdx := 0
	remaining := n

	var padBuf [512*4 + 8]byte

	var offset int64
	for remaining > 0 {
		w := int(data[pos])
		pos++
		groupMin := binary.LittleEndian.Uint32(data[pos:])
		pos += 4

		count := grpSize
		if remaining < count {
			count = remaining
		}

		if w == 0 {
			for i := 0; i < count; i++ {
				offsets[outIdx] = offset
				outIdx++
				offset += int64(groupMin)
			}
		} else {
			packedBytes := (w*count + 7) / 8
			copy(padBuf[:packedBytes], data[pos:pos+packedBytes])
			for i := packedBytes; i < packedBytes+8; i++ {
				padBuf[i] = 0
			}
			pos += packedBytes

			mask := uint64((1 << w) - 1)
			bitPos := 0
			for i := 0; i < count; i++ {
				byteIdx := bitPos >> 3
				shift := uint(bitPos & 7)
				word := binary.LittleEndian.Uint64(padBuf[byteIdx:])
				residual := uint32((word >> shift) & mask)
				offsets[outIdx] = offset
				outIdx++
				offset += int64(groupMin + residual)
				bitPos += w
			}
		}
		remaining -= count
	}

	offsets[n] = offset
	return offsets
}

// FORDecodeWithCRC verifies CRC32C over the packed blob, then decodes.
// Returns offsets and whether the CRC matched.
func FORDecodeWithCRC(data []byte, n int, grpSize int) ([]int64, bool) {
	trailerStart := len(data) - 10
	crcStart := trailerStart - 4

	storedCRC := binary.LittleEndian.Uint32(data[crcStart:trailerStart])
	computedCRC := crc32.Checksum(data[:crcStart], crc32cTable)

	offsets := FORDecode(data[:crcStart], n, grpSize)
	return offsets, computedCRC == storedCRC
}

// FOREncodeSize computes the encoded size without actually encoding.
func FOREncodeSize(deltas []uint32, grpSize int) (headerBytes, dataBytes, totalBytes int) {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	headerBytes = nGroups * 5 // 1 byte W + 4 bytes min per group

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		groupDeltas := deltas[start:end]

		var minDelta, maxDelta uint32
		minDelta = groupDeltas[0]
		maxDelta = groupDeltas[0]
		for _, d := range groupDeltas[1:] {
			if d < minDelta {
				minDelta = d
			}
			if d > maxDelta {
				maxDelta = d
			}
		}
		w := bits.Len32(maxDelta - minDelta)
		count := end - start
		dataBytes += (w*count + 7) / 8
	}

	totalBytes = headerBytes + dataBytes + 4 + 10 // +CRC +trailer
	return
}

// GlobalWEncodeSize computes the encoded size for global-W encoding.
func GlobalWEncodeSize(deltas []uint32, grpSize int) (headerBytes, dataBytes, totalBytes int) {
	nGroups := (len(deltas) + grpSize - 1) / grpSize
	headerBytes = nGroups * 9 // 8-byte anchor + 1-byte W per group

	var globalMax uint32
	for _, d := range deltas {
		if d > globalMax {
			globalMax = d
		}
	}
	globalW := bits.Len32(globalMax)

	for g := 0; g < nGroups; g++ {
		start := g * grpSize
		end := start + grpSize
		if end > len(deltas) {
			end = len(deltas)
		}
		count := end - start
		dataBytes += (globalW*count + 7) / 8
	}

	totalBytes = headerBytes + dataBytes
	return
}
