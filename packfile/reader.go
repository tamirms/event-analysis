package packfile

import (
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"sync"
)

const (
	readBufSize  = 1 << 20     // 1MB
	speculativeReadSize = 256 * 1024 // 256KB speculative read for Open
)

var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readBufSize)
		return &b
	},
}

// Reader provides random access to records in a packfile.
// Safe for concurrent use by multiple goroutines.
type Reader struct {
	file      ReadAtCloser
	trailer   Trailer
	metadata  []byte
	indexBase int64   // byte position where index section starts
	offsets   []int64 // decoded absolute offsets, len = recordCount + 1
}

// Open reads the trailer and full index, validates file integrity,
// and returns a Reader ready for concurrent use.
//
// Uses a speculative 256KB read from EOF to capture the trailer,
// index, and metadata in a single syscall when they fit.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			f.Close()
		}
	}()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	if fileSize < trailerSize {
		return nil, ErrSize
	}

	// Speculative read: last min(speculativeReadSize, fileSize) bytes.
	speculativeSize := int64(speculativeReadSize)
	if speculativeSize > fileSize {
		speculativeSize = fileSize
	}
	speculativeOff := fileSize - speculativeSize
	speculativeBuf := make([]byte, speculativeSize)
	if _, err := f.ReadAt(speculativeBuf, speculativeOff); err != nil {
		return nil, err
	}

	// Parse trailer from last 32 bytes of speculativeBuf.
	tb := speculativeBuf[len(speculativeBuf)-trailerSize:]

	m := binary.LittleEndian.Uint32(tb[0:])
	if m != magic {
		return nil, ErrMagic
	}
	v := tb[4]
	if v != version {
		return nil, ErrVersion
	}
	recordCount := int(binary.LittleEndian.Uint32(tb[6:]))
	indexSize := int(binary.LittleEndian.Uint32(tb[10:]))
	metadataSize := int(binary.LittleEndian.Uint32(tb[14:]))
	storedTrailerCRC := binary.LittleEndian.Uint32(tb[18:])

	if storedTrailerCRC != crc32c(tb[0:18]) {
		return nil, ErrChecksum
	}

	indexBase := fileSize - int64(trailerSize) - int64(metadataSize) - int64(indexSize)
	if indexBase < 0 {
		return nil, ErrSize
	}

	// Tail = index + metadata + trailer. Check if speculativeBuf captured it all.
	tailSize := int64(indexSize) + int64(metadataSize) + int64(trailerSize)

	var indexBuf []byte
	var metadata []byte

	if tailSize <= speculativeSize {
		// Everything is in speculativeBuf — no additional reads needed.
		tailStart := len(speculativeBuf) - int(tailSize)

		indexBuf = make([]byte, indexSize+7) // +7 for safe 8-byte overshoot
		copy(indexBuf, speculativeBuf[tailStart:tailStart+indexSize])

		if metadataSize > 0 {
			metadata = make([]byte, metadataSize)
			metaStart := tailStart + indexSize
			copy(metadata, speculativeBuf[metaStart:metaStart+metadataSize])
		}
	} else {
		// Index + metadata too large for speculativeBuf — single fallback read.
		readSize := indexSize + metadataSize
		buf := make([]byte, readSize+7) // +7 for safe 8-byte overshoot in DecodeGroup
		if readSize > 0 {
			if _, err := f.ReadAt(buf[:readSize], indexBase); err != nil {
				return nil, err
			}
		}

		indexBuf = buf[:indexSize+7]
		if metadataSize > 0 {
			metadata = make([]byte, metadataSize)
			copy(metadata, buf[indexSize:indexSize+metadataSize])
		}
	}

	offsets, err := decodeIndex(indexBuf, recordCount, indexSize, indexBase)
	if err != nil {
		return nil, err
	}

	cleanup = false
	return &Reader{
		file: f,
		trailer: Trailer{
			Version:         v,
			RecordCount:     uint32(recordCount),
			IndexSize:       uint32(indexSize),
			MetadataSize:    uint32(metadataSize),
			TrailerChecksum: storedTrailerCRC,
		},
		metadata:  metadata,
		indexBase: indexBase,
		offsets:   offsets,
	}, nil
}

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
	if storedCRC != crc32c(buf[:payloadLen]) {
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

		if pos > payloadLen {
			return nil, fmt.Errorf("%w: index decode overran payload at group %d (pos %d > %d)", ErrCorrupt, g, pos, payloadLen)
		}

		var size int
		values, size = DecodeGroup(buf[pos:], limit, values)
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

func (r *Reader) resolveOffsetPair(index int) (start, end int64, err error) {
	if index < 0 || index >= int(r.trailer.RecordCount) {
		return 0, 0, fmt.Errorf("%w: %d not in [0, %d)",
			ErrIndexRange, index, r.trailer.RecordCount)
	}
	return r.offsets[index], r.offsets[index+1], nil
}

// ReadRecordInto reads a single record into a caller-provided buffer.
// Returns a slice of buf (possibly reallocated if buf is too small).
// Caller must reassign: buf, err = r.ReadRecordInto(index, buf)
//
// Note: the packfile format does not include per-record checksums.
// Data integrity for individual records must be provided by an upper
// layer (e.g. zstd content checksums in the eventstore package).
func (r *Reader) ReadRecordInto(index int, buf []byte) ([]byte, error) {
	start, end, err := r.resolveOffsetPair(index)
	if err != nil {
		return buf, err
	}
	size := int(end - start)
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	if _, err := r.file.ReadAt(buf, start); err != nil {
		return buf, err
	}
	return buf, nil
}

// ReadRecords returns an iterator over count consecutive records
// starting at index. Reads in batches using a pooled 1MB buffer.
// Each yielded []byte is valid only until the next iteration —
// copy if you need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRecords(index, count int) iter.Seq2[[]byte, error] {
	if index < 0 || count < 0 || index+count > int(r.trailer.RecordCount) {
		panic(fmt.Sprintf("packfile: ReadRecords(%d, %d) out of range [0, %d)",
			index, count, r.trailer.RecordCount))
	}

	return func(yield func([]byte, error) bool) {
		if count == 0 {
			return
		}

		bp := readBufPool.Get().(*[]byte)
		buf := *bp
		defer readBufPool.Put(bp)

		i := index
		end := index + count

		for i < end {
			// Compute batch: consecutive records fitting in buf.
			// batchEnd is exclusive — batch covers [batchStart, batchEnd).
			batchStart := i
			batchEnd := batchStart + 1
			for batchEnd < end && r.offsets[batchEnd+1]-r.offsets[batchStart] <= int64(len(buf)) {
				batchEnd++
			}

			// If a single record exceeds the buffer, allocate one-off.
			recSize := r.offsets[batchStart+1] - r.offsets[batchStart]
			if recSize > int64(len(buf)) {
				oneOff := make([]byte, recSize)
				if _, err := r.file.ReadAt(oneOff, r.offsets[batchStart]); err != nil {
					yield(nil, err)
					return
				}
				if !yield(oneOff, nil) {
					return
				}
				i = batchStart + 1
				continue
			}

			// Read batch.
			batchBytes := r.offsets[batchEnd] - r.offsets[batchStart]
			readBuf := buf[:batchBytes]
			if _, err := r.file.ReadAt(readBuf, r.offsets[batchStart]); err != nil {
				yield(nil, err)
				return
			}

			// Yield subslices.
			for j := batchStart; j < batchEnd; j++ {
				lo := r.offsets[j] - r.offsets[batchStart]
				hi := r.offsets[j+1] - r.offsets[batchStart]
				if !yield(readBuf[lo:hi], nil) {
					return
				}
			}

			i = batchEnd
		}
	}
}

// RecordCount returns the number of records in the packfile.
func (r *Reader) RecordCount() int {
	return int(r.trailer.RecordCount)
}

// Trailer returns the parsed trailer.
func (r *Reader) Trailer() Trailer {
	return r.trailer
}

// Metadata returns a copy of the opaque metadata stored in the packfile.
func (r *Reader) Metadata() []byte {
	if r.metadata == nil {
		return nil
	}
	out := make([]byte, len(r.metadata))
	copy(out, r.metadata)
	return out
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}
