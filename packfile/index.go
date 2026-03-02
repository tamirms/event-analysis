package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/tamir/events-analysis/intpack"
)

// decodeIndex decodes a FOR-128 encoded offset index with CRC32C integrity.
// buf must contain at least indexSize+7 bytes (7 for safe 8-byte overshoot in DecodeGroup).
// Returns recordCount+1 offsets: offsets[i] = start offset of record i, offsets[recordCount] = indexBase.
func decodeIndex(buf []byte, recordCount int, indexSize int, indexBase int64) ([]int64, error) {
	if indexSize < 4 {
		return nil, fmt.Errorf("%w: index too small (%d bytes)", ErrCorrupt, indexSize)
	}

	// Sanity-check recordCount against indexSize to prevent OOM from crafted trailers.
	// Each FOR group of up to 128 records requires at least 6 bytes (1B W + 4B min + 1B packed).
	maxGroups := (indexSize - 4) / 6 // subtract CRC, divide by min group size
	maxRecords := maxGroups * groupSize
	if recordCount > maxRecords {
		return nil, fmt.Errorf("%w: recordCount %d implausible for indexSize %d", ErrCorrupt, recordCount, indexSize)
	}

	// Verify CRC32C over raw index bytes (all groups, excluding trailing 4-byte CRC).
	payloadLen := indexSize - 4
	storedCRC := binary.LittleEndian.Uint32(buf[payloadLen:])
	if storedCRC != CRC32C(buf[:payloadLen]) {
		return nil, ErrChecksum
	}

	offsets := make([]int64, recordCount+1)
	idx := 0
	pos := 0
	offset := int64(0)

	groupCount := (recordCount + groupSize - 1) / groupSize

	var values []uint32
	for g := range groupCount {
		limit := groupSize
		if g == groupCount-1 && recordCount%groupSize != 0 {
			limit = recordCount % groupSize
		}

		if pos >= payloadLen {
			return nil, fmt.Errorf("%w: index decode overran payload at group %d (pos %d >= %d)", ErrCorrupt, g, pos, payloadLen)
		}

		var size int
		var err error
		values, size, err = intpack.DecodeGroup(buf[pos:], limit, values)
		if err != nil {
			return nil, fmt.Errorf("%w: index group %d: %w", ErrCorrupt, g, err)
		}
		for _, v := range values {
			offsets[idx] = offset
			idx++
			offset += int64(v)
		}
		pos += size
	}

	// Structural sanity check: running sum must arrive at indexBase.
	if offset != indexBase {
		return nil, fmt.Errorf("%w: final offset %d != indexBase %d", ErrCorrupt, offset, indexBase)
	}
	offsets[recordCount] = indexBase

	return offsets, nil
}

// encodeIndex encodes record offsets into a FOR-128 index section with CRC32C.
// offsets must have recordCount+1 entries (the last being end-of-data).
// Returns the index bytes including the trailing CRC32C.
func encodeIndex(offsets []int64) ([]byte, error) {
	recordCount := len(offsets) - 1
	if recordCount > math.MaxUint32 {
		return nil, fmt.Errorf("packfile: record count %d exceeds uint32 max", recordCount)
	}

	var buf bytes.Buffer
	var deltas []uint32
	for g := 0; g*groupSize < recordCount; g++ {
		base := g * groupSize
		end := min(base+groupSize, recordCount)

		deltas = deltas[:0]
		if cap(deltas) < end-base {
			deltas = make([]uint32, 0, end-base)
		}
		for j := base; j < end; j++ {
			d := offsets[j+1] - offsets[j]
			if d > math.MaxUint32 {
				return nil, fmt.Errorf("packfile: record size delta %d exceeds 4GB", d)
			}
			deltas = append(deltas, uint32(d))
		}

		buf.Write(intpack.EncodeGroup(deltas))
	}

	// CRC32C over raw index bytes.
	binary.Write(&buf, binary.LittleEndian, CRC32C(buf.Bytes()))
	return buf.Bytes(), nil
}
