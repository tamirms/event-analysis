package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/klauspost/compress/zstd"
)

const (
	indexHeaderSize = 8
	indexVersion    = 1
)

// ChunkReader reads ledgers from a single chunk's index+data file pair.
// ReadLedger reuses internal buffers — the returned []byte is only valid until the next ReadLedger call.
type ChunkReader struct {
	dataFile   *os.File
	offsets    []uint64
	decoder    *zstd.Decoder
	numLedgers int
	compBuf    []byte // reusable compressed read buffer
	decompBuf  []byte // reusable decompression buffer
}

// NewChunkReader opens the index and data files and parses offsets.
func NewChunkReader(indexPath, dataPath string) (*ChunkReader, error) {
	// Open and parse index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()

	// Read header
	header := make([]byte, indexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("failed to read index header: %w", err)
	}

	version := header[0]
	offsetSize := header[1]

	if version != indexVersion {
		return nil, fmt.Errorf("unsupported index version: %d", version)
	}
	if offsetSize != 4 && offsetSize != 8 {
		return nil, fmt.Errorf("invalid offset size: %d", offsetSize)
	}

	// Determine number of offsets from file size
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}
	numOffsets := int((fileInfo.Size() - indexHeaderSize) / int64(offsetSize))
	numLedgers := numOffsets - 1

	// Read all offsets
	offsetsData := make([]byte, numOffsets*int(offsetSize))
	if _, err := indexFile.ReadAt(offsetsData, indexHeaderSize); err != nil {
		return nil, fmt.Errorf("failed to read offsets: %w", err)
	}

	offsets := make([]uint64, numOffsets)
	for i := 0; i < numOffsets; i++ {
		if offsetSize == 4 {
			offsets[i] = uint64(binary.LittleEndian.Uint32(offsetsData[i*4 : i*4+4]))
		} else {
			offsets[i] = binary.LittleEndian.Uint64(offsetsData[i*8 : i*8+8])
		}
	}

	// Open data file (kept open for the lifetime of the reader)
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Create zstd decoder
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &ChunkReader{
		dataFile:   dataFile,
		offsets:    offsets,
		decoder:    decoder,
		numLedgers: numLedgers,
	}, nil
}

// NumLedgers returns the number of ledgers in this chunk.
func (r *ChunkReader) NumLedgers() int {
	return r.numLedgers
}

// ReadLedger reads and decompresses a single ledger by local index (0-based).
func (r *ChunkReader) ReadLedger(localIndex int) ([]byte, error) {
	if localIndex < 0 || localIndex >= r.numLedgers {
		return nil, fmt.Errorf("local index %d out of range [0, %d)", localIndex, r.numLedgers)
	}

	startOffset := r.offsets[localIndex]
	endOffset := r.offsets[localIndex+1]
	recordSize := int(endOffset - startOffset)

	// Reuse compressed read buffer
	if recordSize > cap(r.compBuf) {
		r.compBuf = make([]byte, recordSize)
	} else {
		r.compBuf = r.compBuf[:recordSize]
	}

	if _, err := r.dataFile.ReadAt(r.compBuf, int64(startOffset)); err != nil {
		return nil, fmt.Errorf("failed to read compressed data at index %d: %w", localIndex, err)
	}

	// Reuse decompression buffer
	decompressed, err := r.decoder.DecodeAll(r.compBuf, r.decompBuf[:0])
	if err != nil {
		return nil, fmt.Errorf("failed to decompress ledger at index %d: %w", localIndex, err)
	}
	r.decompBuf = decompressed

	return decompressed, nil
}

// Close releases all resources.
func (r *ChunkReader) Close() {
	if r.decoder != nil {
		r.decoder.Close()
	}
	if r.dataFile != nil {
		r.dataFile.Close()
	}
}
