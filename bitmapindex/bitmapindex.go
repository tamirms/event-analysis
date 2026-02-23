package bitmapindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/tamir/events-analysis/zstd"
)

// ErrKeyNotFound is returned when a lookup key is not in the index
// (fingerprint mismatch with the MPHF candidate).
var ErrKeyNotFound = errors.New("bitmapindex: key not found")

const (
	fingerprintSize = 4
	flagCompressed  = 0x01
	checksumSize    = 4
	metadataSize    = 8

	// DefaultBatchSize is the number of bitmaps per packfile record.
	DefaultBatchSize = 128

	// knownFlags is the bitmask of all flag bits the reader understands.
	// Unknown bits cause Open to reject the file.
	knownFlags uint16 = 0
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ComposeKey appends discriminator to key, producing a unique lookup key.
// Use distinct discriminator values to prevent collisions when the same
// raw key appears in different logical categories (e.g., contract ID vs topic).
//
// Both Writer.Add and Reader.Lookup expect keys in this format.
func ComposeKey(key []byte, discriminator byte) []byte {
	composite := make([]byte, len(key)+1)
	copy(composite, key)
	composite[len(key)] = discriminator
	return composite
}

// batchRecord holds parsed offsets into a CRC-verified batch record.
// Returned by value (~64 bytes) to stay on the stack.
type batchRecord struct {
	count     int
	rec       []byte // CRC-verified, CRC stripped
	fpBase    int    // offset of fingerprints section
	flagsBase int    // offset of flags section
	sizesBase int    // offset of sizes section
	dataBase  int    // offset of concatenated bitmap data
}

// parseBatch verifies the CRC32C of rec and parses the batch header.
func parseBatch(rec []byte, batchIdx int) (batchRecord, error) {
	// Minimum batch: 2 (count) + 4 (fp) + 1 (flags) + 4 (size) + 4 (CRC) = 15 bytes.
	if len(rec) < 2+fingerprintSize+1+4+checksumSize {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch record too short at batch %d: %d bytes", batchIdx, len(rec))
	}

	// Verify CRC32C.
	storedCRC := binary.LittleEndian.Uint32(rec[len(rec)-checksumSize:])
	computedCRC := crc32.Checksum(rec[:len(rec)-checksumSize], crc32cTable)
	if storedCRC != computedCRC {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch CRC32C mismatch at batch %d: stored=%08x computed=%08x", batchIdx, storedCRC, computedCRC)
	}
	rec = rec[:len(rec)-checksumSize]

	count := int(binary.LittleEndian.Uint16(rec[:2]))
	headerSize := 2 + count*fingerprintSize + count + count*4
	if len(rec) < headerSize {
		return batchRecord{}, fmt.Errorf("bitmapindex: batch header truncated at batch %d", batchIdx)
	}

	return batchRecord{
		count:     count,
		rec:       rec,
		fpBase:    2,
		flagsBase: 2 + count*fingerprintSize,
		sizesBase: 2 + count*fingerprintSize + count,
		dataBase:  headerSize,
	}, nil
}

// extractBitmap extracts and deserializes the bitmap at localIdx within batch.
// hk is the 16-byte pre-hash; the first 4 bytes are used for fingerprint verification.
// lb is a caller-owned buffer for decompression.
func extractBitmap(batch batchRecord, localIdx int, hk []byte, lb *lookupBuf) (*roaring.Bitmap, error) {
	if localIdx >= batch.count {
		return nil, fmt.Errorf("bitmapindex: local index %d >= batch count %d", localIdx, batch.count)
	}

	// Verify fingerprint.
	fpOff := batch.fpBase + localIdx*fingerprintSize
	if batch.rec[fpOff] != hk[0] || batch.rec[fpOff+1] != hk[1] ||
		batch.rec[fpOff+2] != hk[2] || batch.rec[fpOff+3] != hk[3] {
		return nil, ErrKeyNotFound
	}

	flags := batch.rec[batch.flagsBase+localIdx]
	dataSize := int(binary.LittleEndian.Uint32(batch.rec[batch.sizesBase+localIdx*4:]))

	// Compute data offset by summing sizes of preceding bitmaps.
	dataOff := batch.dataBase
	for i := range localIdx {
		dataOff += int(binary.LittleEndian.Uint32(batch.rec[batch.sizesBase+i*4:]))
	}

	if dataOff+dataSize > len(batch.rec) {
		return nil, fmt.Errorf("bitmapindex: bitmap data overflow at local %d in batch", localIdx)
	}

	data := batch.rec[dataOff : dataOff+dataSize]

	// Decompress if needed.
	if flags&flagCompressed != 0 {
		decoded, err := lb.dec.Decode(lb.decompBuf[:0], data)
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: decompress bitmap at local %d: %w", localIdx, err)
		}
		lb.decompBuf = decoded
		data = decoded
	}

	// Deserialize roaring bitmap. Must use UnmarshalBinary (copies data)
	// rather than FromBuffer, because data references the pool buffer
	// which is returned to sync.Pool on defer.
	bm := roaring.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap at local %d: %w", localIdx, err)
	}

	return bm, nil
}

// lookupBuf holds reusable buffers for batch reads and decompression.
type lookupBuf struct {
	rec       []byte
	decompBuf []byte
	dec       *zstd.Decompressor
}
