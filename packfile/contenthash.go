package packfile

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in the trailer.
var ErrContentHashMismatch = errors.New("packfile: content hash mismatch")

// contentHasher computes a chunked SHA-256 content hash over a stream of entries.
// Entries are length-prefixed and grouped into fixed-size chunks; chunk digests
// are aggregated into a final hash. chunkSize is typically the record size.
//
//	chunkDigest_i = SHA-256([4B len][entry_{i*K}] ... [4B len][entry_{i*K+K-1}])
//	finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
type contentHasher struct {
	digests   []byte // concatenated 32-byte chunk digests
	buf       []byte
	count     int
	chunkSize int
	lenBuf    [4]byte
}

// newContentHasher creates a contentHasher with the given chunk size.
// Panics if chunkSize <= 0.
func newContentHasher(chunkSize int) *contentHasher {
	if chunkSize <= 0 {
		panic(fmt.Sprintf("packfile: newContentHasher chunkSize must be > 0, got %d", chunkSize))
	}
	return &contentHasher{
		chunkSize: chunkSize,
	}
}

// Add appends one logical entry. Parts are concatenated under a single length prefix.
func (h *contentHasher) Add(parts ...[]byte) {
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

// Snapshot returns the content hash as if Sum were called now, without
// mutating the hasher state. Safe for concurrent readers.
func (h *contentHasher) Snapshot() [32]byte {
	if h.count == 0 {
		return sha256.Sum256(h.digests)
	}
	partial := sha256.Sum256(h.buf)
	tmp := make([]byte, 0, len(h.digests)+sha256.Size)
	tmp = append(tmp, h.digests...)
	tmp = append(tmp, partial[:]...)
	return sha256.Sum256(tmp)
}

// Sum flushes any partial chunk and returns the final hash.
// After calling Sum, the hasher must not be reused (no further Add calls).
func (h *contentHasher) Sum() [32]byte {
	if h.count > 0 {
		digest := sha256.Sum256(h.buf)
		h.digests = append(h.digests, digest[:]...)
		h.buf = h.buf[:0]
		h.count = 0
	}
	return sha256.Sum256(h.digests)
}

// verifyContentHash re-reads all items from ir, recomputes the chunked
// SHA-256 content hash, and compares it to expected.
func verifyContentHash(ctx context.Context, ir ItemReader, recordSize int, expected [32]byte) error {
	total, err := ir.TotalItems()
	if err != nil {
		return err
	}
	hasher := newContentHasher(recordSize)
	i := 0
	for item, err := range ir.ReadRange(0, total) {
		if err != nil {
			return err
		}
		hasher.Add(item)
		i++
		if i%recordSize == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	computed := hasher.Sum()
	if computed != expected {
		return fmt.Errorf("packfile: %w: expected %x, got %x",
			ErrContentHashMismatch, expected, computed)
	}
	return nil
}

