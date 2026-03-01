// Package record provides a compression-aware Decoder for packfile records
// containing multiple entries with a trailing FOR-encoded size index, plus
// shared metadata encoding conventions used by eventstore and bitmapindex.
package record

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/tamir/events-analysis/intpack"
	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

// FlagNoCompression indicates records are stored uncompressed with CRC32C integrity.
const FlagNoCompression uint32 = 1 << 0

// FlagContentHash indicates metadata contains a 32-byte SHA-256 content hash
// after the standard 12-byte header.
const FlagContentHash uint32 = 1 << 1

// ErrChecksum is returned when a record's CRC32C checksum does not match.
var ErrChecksum = errors.New("record: checksum mismatch")

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in metadata. Used by eventstore and bitmapindex Verify().
var ErrContentHashMismatch = errors.New("record: content hash mismatch")

// KnownFlags is the set of all metadata flags recognized by the current version.
const KnownFlags = FlagNoCompression | FlagContentHash

// ContentHasher computes a chunked SHA-256 content hash over a stream of entries.
// Entries are length-prefixed and grouped into fixed-size chunks; chunk digests
// are aggregated into a final hash. chunkSize is typically the record size (blockN).
//
//	chunkDigest_i = SHA-256([4B len][entry_{i*K}] ... [4B len][entry_{i*K+K-1}])
//	finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
type ContentHasher struct {
	digests   []byte // concatenated 32-byte chunk digests
	buf       []byte
	count     int
	chunkSize int
	lenBuf    [4]byte
}

// NewContentHasher creates a ContentHasher with the given chunk size.
// Panics if chunkSize <= 0.
func NewContentHasher(chunkSize int) *ContentHasher {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("record: NewContentHasher chunkSize must be > 0, got %d", chunkSize))
	}
	return &ContentHasher{
		chunkSize: chunkSize,
	}
}

// Add appends one logical entry. Parts are concatenated under a single length prefix.
func (h *ContentHasher) Add(parts ...[]byte) {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	binary.LittleEndian.PutUint32(h.lenBuf[:], uint32(total))
	h.buf = append(h.buf, h.lenBuf[:]...)
	for _, p := range parts {
		h.buf = append(h.buf, p...)
	}
	h.count++
	if h.count == h.chunkSize {
		digest := sha256.Sum256(h.buf)
		h.digests = append(h.digests, digest[:]...)
		h.buf = h.buf[:0]
		h.count = 0
	}
}

// Sum flushes any partial chunk and returns the final hash.
// After calling Sum, the hasher must not be reused (no further Add calls).
func (h *ContentHasher) Sum() [32]byte {
	if h.count > 0 {
		digest := sha256.Sum256(h.buf)
		h.digests = append(h.digests, digest[:]...)
		h.buf = h.buf[:0]
		h.count = 0
	}
	return sha256.Sum256(h.digests)
}

// --- Metadata helpers ---
//
// Shared 12-byte metadata format: [u32 totalItems][u32 recordSize][u32 flags].
// Used by eventstore and bitmapindex to encode format parameters into the
// packfile's opaque metadata field.

// EncodeMetadata encodes fixed-width 12-byte metadata: [u32 totalItems][u32 recordSize][u32 flags].
func EncodeMetadata(totalItems, recordSize int, flags uint32) []byte {
	var meta [12]byte
	binary.LittleEndian.PutUint32(meta[0:], uint32(totalItems))
	binary.LittleEndian.PutUint32(meta[4:], uint32(recordSize))
	binary.LittleEndian.PutUint32(meta[8:], flags)
	return meta[:]
}

// DecodeMetadata parses fixed-width 12-byte metadata.
// Does NOT validate flags — callers validate against their own known-flags set.
func DecodeMetadata(meta []byte) (totalItems, recordSize int, flags uint32, err error) {
	if len(meta) < 12 {
		return 0, 0, 0, fmt.Errorf("record: metadata too short: %d bytes (want >= 12)", len(meta))
	}
	totalItems = int(binary.LittleEndian.Uint32(meta[0:]))
	recordSize = int(binary.LittleEndian.Uint32(meta[4:]))
	flags = binary.LittleEndian.Uint32(meta[8:])
	return totalItems, recordSize, flags, nil
}

// ItemsInRecord returns the number of items in the given record index.
// Handles the last record which may be partial, and totalItems == 0.
// Panics if recordSize <= 0 or recordIdx is out of range.
func ItemsInRecord(totalItems, recordSize, recordIdx int) int {
	if totalItems == 0 {
		return 0
	}
	if recordSize <= 0 {
		panic(fmt.Sprintf("record: ItemsInRecord recordSize must be > 0, got %d", recordSize))
	}
	recordCount := (totalItems + recordSize - 1) / recordSize
	if recordIdx < 0 || recordIdx >= recordCount {
		panic(fmt.Sprintf("record: ItemsInRecord recordIdx %d out of range [0, %d)", recordIdx, recordCount))
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

// ContentHash extracts the 32-byte SHA-256 content hash from metadata.
// Layout: [0:4] totalItems, [4:8] recordSize, [8:12] flags, [12:44] SHA-256 hash.
// Returns (hash, true) if FlagContentHash is set and metadata is long enough.
func ContentHash(meta []byte, flags uint32) ([32]byte, bool) {
	if flags&FlagContentHash == 0 || len(meta) < 44 {
		return [32]byte{}, false
	}
	return [32]byte(meta[12:44]), true
}

// --- Decoder ---

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

// New creates a new Decoder with an initialized decompressor.
// Callers must call Close() when done to release CGO resources.
func New() *Decoder {
	return &Decoder{dec: zstd.NewDecompressor()}
}

// pool holds reusable Decoders. Each owns a ZSTD_DCtx (C-allocated). If the
// GC evicts an entry, that context is leaked — acceptable because pool churn
// is low in practice and runtime.SetFinalizer is unsafe with sync.Pool
// (the finalizer can fire while the object is still pooled).
var pool = sync.Pool{
	New: func() any { return New() },
}

// Get returns a Decoder from the pool.
func Get() *Decoder { return pool.Get().(*Decoder) }

// Put returns a Decoder to the pool, resetting all buffers.
func Put(rd *Decoder) {
	rd.scratch = rd.scratch[:0]
	rd.decompressed = rd.decompressed[:0]
	rd.sizes = rd.sizes[:0]
	rd.offsets = rd.offsets[:0]
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
			return fmt.Errorf("record: too short for CRC32C: %d bytes (min 5)", len(data))
		}
		payloadEnd := len(data) - 4
		stored := binary.LittleEndian.Uint32(data[payloadEnd:])
		if stored != packfile.CRC32C(data[:payloadEnd]) {
			return fmt.Errorf("record: CRC32C: %w", ErrChecksum)
		}
		rd.decompressed = append(rd.decompressed[:0], data[:payloadEnd]...)
	}

	var err error
	rd.sizes, err = intpack.DecodeTrailingGroup(rd.decompressed, n, rd.sizes)
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
// Panics if i is out of [0, n) where n is the number of entries.
func (rd *Decoder) Entry(i int) []byte {
	if i < 0 || i >= len(rd.sizes) {
		panic(fmt.Sprintf("record: Entry(%d) out of range [0, %d)", i, len(rd.sizes)))
	}
	return rd.decompressed[rd.offsets[i]:rd.offsets[i+1]]
}

// EntryCopy returns an owned copy of the entry at index i.
// Panics if i is out of [0, n) where n is the number of entries.
func (rd *Decoder) EntryCopy(i int) []byte {
	e := rd.Entry(i)
	out := make([]byte, len(e))
	copy(out, e)
	return out
}
