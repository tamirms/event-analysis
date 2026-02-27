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
