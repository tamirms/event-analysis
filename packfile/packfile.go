package packfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// ReadAtCloser is the minimal interface needed by Reader to access packfile data.
// *os.File satisfies this interface.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

const (
	magic     = 0x534C4348 // "SLCH"
	version   = 1
	groupSize = 128
	trailerSize = 32
)

// Trailer holds the parsed trailer fields.
type Trailer struct {
	Version         uint8
	RecordCount     uint32
	IndexSize       uint32
	MetadataSize    uint32
	TrailerChecksum uint32
}

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
	Metadata []byte // opaque, caller-defined, stored in the file

	// BytesPerSync initiates background writeback of dirty pages every N bytes
	// written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
	// is non-blocking — it tells the kernel to start flushing without waiting.
	// This spreads I/O across the write phase so the final fdatasync in Finish()
	// has less data to flush. 0 disables (default).
	BytesPerSync int
}

// Errors
var (
	ErrCorrupt    = errors.New("packfile: corrupt file")
	ErrMagic      = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
	ErrVersion    = fmt.Errorf("%w: unsupported version", ErrCorrupt)
	ErrChecksum   = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	ErrSize       = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)
	ErrIndexRange = errors.New("packfile: record index out of range")
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C computes the CRC32C (Castagnoli) checksum of b.
func CRC32C(b []byte) uint32 { return crc32.Checksum(b, crc32cTable) }

// --- Metadata helpers ---
//
// Shared 12-byte metadata format: [u32 totalItems][u32 recordSize][u32 flags].
// Used by eventstore and bitmapindex to encode format parameters into the
// packfile's opaque metadata field.

// FlagNoCompression indicates records are stored uncompressed with CRC32C integrity.
const FlagNoCompression uint32 = 1 << 0

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
		return 0, 0, 0, fmt.Errorf("packfile: metadata too short: %d bytes (want >= 12)", len(meta))
	}
	totalItems = int(binary.LittleEndian.Uint32(meta[0:]))
	recordSize = int(binary.LittleEndian.Uint32(meta[4:]))
	flags = binary.LittleEndian.Uint32(meta[8:])
	return totalItems, recordSize, flags, nil
}

// ItemsInRecord returns the number of items in the given record index.
// Handles the last record which may be partial, and totalItems == 0.
func ItemsInRecord(totalItems, recordSize, recordIdx int) int {
	if totalItems == 0 {
		return 0
	}
	recordCount := (totalItems + recordSize - 1) / recordSize
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
