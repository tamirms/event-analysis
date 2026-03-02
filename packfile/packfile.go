package packfile

import (
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
	magic       = 0x534C4348 // "SLCH"
	version     = 1
	groupSize   = 128 // values per FOR group in the index section
	trailerSize = 64
)

// RecordFormat describes how records are stored on disk.
type RecordFormat int

const (
	// Compressed: zstd-compressed records (default). Integrity is provided
	// by zstd's built-in content checksum.
	Compressed RecordFormat = iota

	// Uncompressed: records stored as-is with a trailing 4-byte CRC32C.
	Uncompressed

	// Raw: records stored as-is with no integrity wrapper. Use when items
	// are already compressed or checksummed.
	Raw
)

// On-disk flag bits (uint8 at trailer offset 5).
const (
	flagNoCompression uint8 = 1 << 0
	flagContentHash   uint8 = 1 << 1
	flagNoCRC         uint8 = 1 << 2
)

const knownFlags = flagNoCompression | flagContentHash | flagNoCRC

// Trailer holds the parsed trailer fields.
type Trailer struct {
	Version        uint8
	RecordCount    uint32
	TotalItems     uint32
	RecordSize     uint32
	IndexSize      uint32
	AppDataSize    uint32
	ContentHash    [32]byte
	Format         RecordFormat
	HasContentHash bool
	Checksum       uint32
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
