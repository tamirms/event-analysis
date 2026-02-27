// Package record provides a compression-aware Decoder for packfile records
// containing multiple entries with a trailing FOR-encoded size index.
package record

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

// Decoder decodes packfile records containing multiple entries with a trailing
// FOR-encoded size index. Handles both zstd-compressed and CRC32C-verified
// uncompressed records.
type Decoder struct {
	scratch      []byte
	decompressed []byte
	sizes        []uint32
	offsets      []int // prefix sum: offsets[i] = byte offset of entry i
	dec          *zstd.Decompressor
}

// New creates a new Decoder with a runtime finalizer for the decompressor.
func New() *Decoder {
	rd := &Decoder{dec: zstd.NewDecompressor()}
	runtime.SetFinalizer(rd, func(d *Decoder) { d.dec.Close() })
	return rd
}

var pool = sync.Pool{
	New: func() any { return New() },
}

// Get returns a Decoder from the pool.
func Get() *Decoder { return pool.Get().(*Decoder) }

// Put returns a Decoder to the pool, resetting scratch buffers.
func Put(rd *Decoder) {
	rd.scratch = rd.scratch[:0]
	rd.decompressed = rd.decompressed[:0]
	pool.Put(rd)
}

// Close releases the decompressor. Must NOT be called on pooled decoders.
func (rd *Decoder) Close() {
	if rd.dec != nil {
		rd.dec.Close()
		rd.dec = nil
	}
}

// Decode decodes a record containing n entries.
// If compressed, the record is zstd-decompressed (content checksum provides integrity).
// If !compressed, the trailing 4 bytes are verified as CRC32C over the preceding payload.
func (rd *Decoder) Decode(data []byte, n int, compressed bool) error {
	if compressed {
		decoded, err := rd.dec.Decode(rd.decompressed[:0], data)
		if err != nil {
			return err
		}
		rd.decompressed = decoded
	} else {
		if len(data) < 5 {
			return fmt.Errorf("record too short for CRC32C: %d bytes (min 5)", len(data))
		}
		payloadEnd := len(data) - 4
		stored := binary.LittleEndian.Uint32(data[payloadEnd:])
		if stored != packfile.CRC32C(data[:payloadEnd]) {
			return fmt.Errorf("record CRC32C: %w", packfile.ErrChecksum)
		}
		rd.decompressed = append(rd.decompressed[:0], data[:payloadEnd]...)
	}

	var err error
	rd.sizes, err = packfile.DecodeTrailingGroup(rd.decompressed, n, rd.sizes)
	if err != nil {
		return err
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

// ReadAndDecode reads a record from the packfile and decodes it.
func (rd *Decoder) ReadAndDecode(pr *packfile.Reader, recordIdx, n int, compressed bool) error {
	var err error
	rd.scratch, err = pr.ReadRecord(recordIdx, rd.scratch)
	if err != nil {
		return err
	}
	return rd.Decode(rd.scratch, n, compressed)
}

// Entry returns the entry at index i within the decoded record.
// The returned slice is valid only until the next Decode call.
func (rd *Decoder) Entry(i int) []byte {
	return rd.decompressed[rd.offsets[i]:rd.offsets[i+1]]
}

// EntryCopy returns an owned copy of the entry at index i.
func (rd *Decoder) EntryCopy(i int) []byte {
	e := rd.Entry(i)
	out := make([]byte, len(e))
	copy(out, e)
	return out
}
