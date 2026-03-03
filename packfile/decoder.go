package packfile

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/zstd"
)

// decoder decodes packfile records. Handles compressed, uncompressed (CRC32C),
// and raw record formats, with or without a trailing size table.
//
// Configure format, totalItems, and recordSize after obtaining a decoder
// from the pool (or after newDecoder) and before calling Decode.
type decoder struct {
	format     RecordFormat
	totalItems int
	recordSize int

	scratch      []byte
	decompressed []byte
	sizes        []uint32
	offsets      []int // prefix sum: offsets[i] = byte offset of entry i
	dec          *zstd.Decompressor
}

// newDecoder creates a decoder with an initialized decompressor.
func newDecoder() *decoder {
	return &decoder{dec: zstd.NewDecompressor()}
}

// decoderPool holds reusable decoders. Each owns a ZSTD_DCtx (C-allocated). If the
// GC evicts an entry, that context is leaked — acceptable because pool churn
// is low in practice and runtime.SetFinalizer is unsafe with sync.Pool
// (the finalizer can fire while the object is still pooled).
var decoderPool = sync.Pool{
	New: func() any { return newDecoder() },
}

// getDecoder returns a decoder from the pool.
func getDecoder() *decoder { return decoderPool.Get().(*decoder) }

// putDecoder returns a decoder to the pool, resetting all buffers and config.
func putDecoder(rd *decoder) {
	rd.format = Compressed
	rd.totalItems = 0
	rd.recordSize = 0
	rd.scratch = rd.scratch[:0]
	rd.decompressed = rd.decompressed[:0]
	rd.sizes = rd.sizes[:0]
	rd.offsets = rd.offsets[:0]
	decoderPool.Put(rd)
}

// Close releases the decompressor. Must NOT be called on pooled decoders.
func (rd *decoder) Close() {
	if rd.dec != nil {
		rd.dec.Close()
		rd.dec = nil
	}
}

// itemsInRecord returns the number of items in the given record index.
// Handles the last record which may be partial, and totalItems == 0.
// Panics if recordSize <= 0 or recordIdx is out of range.
func itemsInRecord(totalItems, recordSize, recordIdx int) int {
	if totalItems == 0 {
		return 0
	}
	if recordSize <= 0 {
		panic(fmt.Sprintf("packfile: itemsInRecord recordSize must be > 0, got %d", recordSize))
	}
	recordCount := (totalItems + recordSize - 1) / recordSize
	if recordIdx < 0 || recordIdx >= recordCount {
		panic(fmt.Sprintf("packfile: itemsInRecord recordIdx %d out of range [0, %d)", recordIdx, recordCount))
	}
	last := recordCount - 1
	if recordIdx < last {
		return recordSize
	}
	rem := totalItems % recordSize
	if rem == 0 {
		return recordSize
	}
	return rem
}

// Decode decodes the record at recordIdx.
// The number of entries is computed from totalItems and recordSize.
// Behavior depends on format and recordSize:
//   - Compressed: zstd-decompress (zstd provides integrity)
//   - Uncompressed: verify trailing CRC32C, strip it
//   - Raw: use data as-is (no integrity check)
//   - recordSize>1: strip and verify the FOR index from the raw record tail first,
//     then apply format-specific payload processing
//   - recordSize==1: entire payload is one item (no FOR index)
func (rd *decoder) Decode(data []byte, recordIdx int) error {
	n := itemsInRecord(rd.totalItems, rd.recordSize, recordIdx)

	// For multi-item records: the FOR index is always the last bytes of the raw
	// record, uncompressed, with its own CRC32C. Strip and verify it before any
	// format-specific processing. Last 9 bytes are always [1B width][4B min][4B CRC].
	var forIndexBytes []byte
	if rd.recordSize > 1 {
		const forFooterSize = 5 // 1B width + 4B min
		const crcSize = 4
		const metaSize = forFooterSize + crcSize // fixed tail of every multi-item record
		if len(data) < metaSize {
			return fmt.Errorf("packfile: record too short for FOR index: %d bytes", len(data))
		}
		width := data[len(data)-metaSize]
		if width > 32 {
			return fmt.Errorf("%w: invalid FOR width %d", ErrCorrupt, width)
		}
		packSize := (int(width)*n + 7) / 8
		forStart := len(data) - metaSize - packSize
		if forStart < 0 {
			return fmt.Errorf("%w: FOR index size exceeds record size", ErrCorrupt)
		}
		forIndexBytes = data[forStart : len(data)-crcSize]
		storedCRC := binary.LittleEndian.Uint32(data[len(data)-crcSize:])
		if storedCRC != CRC32C(forIndexBytes) {
			return fmt.Errorf("packfile: FOR index CRC32C: %w", ErrChecksum)
		}
		data = data[:forStart]
	}

	// Format-specific payload processing on data (with FOR index already stripped).
	switch rd.format {
	case Compressed:
		decoded, err := rd.dec.Decode(rd.decompressed[:0], data)
		if err != nil {
			return err
		}
		rd.decompressed = decoded
	case Raw:
		rd.decompressed = append(rd.decompressed[:0], data...)
	default: // Uncompressed: CRC_items covers payload only
		if len(data) < 5 {
			return fmt.Errorf("packfile: too short for CRC32C: %d bytes (min 5)", len(data))
		}
		payloadEnd := len(data) - 4
		stored := binary.LittleEndian.Uint32(data[payloadEnd:])
		if stored != CRC32C(data[:payloadEnd]) {
			return fmt.Errorf("packfile: CRC32C: %w", ErrChecksum)
		}
		rd.decompressed = append(rd.decompressed[:0], data[:payloadEnd]...)
	}

	if rd.recordSize > 1 {
		var err error
		rd.sizes, _, err = intpack.DecodeGroup(forIndexBytes, n, rd.sizes)
		if err != nil {
			return err
		}
		// Validate sum(sizes) == actual payload length (format-aware via rd.decompressed).
		sum := 0
		for _, s := range rd.sizes {
			sum += int(s)
		}
		if sum != len(rd.decompressed) {
			return fmt.Errorf("%w: item size sum %d != payload len %d", ErrCorrupt, sum, len(rd.decompressed))
		}
	} else {
		if cap(rd.sizes) >= 1 {
			rd.sizes = rd.sizes[:1]
		} else {
			rd.sizes = make([]uint32, 1)
		}
		rd.sizes[0] = uint32(len(rd.decompressed))
	}

	// Build prefix sum of sizes for offset lookups.
	if cap(rd.offsets) <= n {
		rd.offsets = make([]int, n+1)
	} else {
		rd.offsets = rd.offsets[:n+1]
	}
	rd.offsets[0] = 0
	for i, s := range rd.sizes {
		rd.offsets[i+1] = rd.offsets[i] + int(s)
	}
	return nil
}

// Entry returns the entry at index i within the decoded record.
// The returned slice is valid only until the next Decode call.
// Panics if i is out of [0, n) where n is the number of entries.
func (rd *decoder) Entry(i int) []byte {
	if i < 0 || i >= len(rd.sizes) {
		panic(fmt.Sprintf("packfile: Entry(%d) out of range [0, %d)", i, len(rd.sizes)))
	}
	return rd.decompressed[rd.offsets[i]:rd.offsets[i+1]]
}

// EntryCopy returns an owned copy of the entry at index i.
// Panics if i is out of [0, n) where n is the number of entries.
func (rd *decoder) EntryCopy(i int) []byte {
	e := rd.Entry(i)
	out := make([]byte, len(e))
	copy(out, e)
	return out
}
