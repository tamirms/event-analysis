package bitmapindex

import (
	"errors"
	"hash/crc32"
)

// ErrKeyNotFound is returned when a lookup key is not in the index
// (fingerprint mismatch with the MPHF candidate).
var ErrKeyNotFound = errors.New("bitmapindex: key not found")

const (
	fingerprintSize  = 4
	flagCompressed   = 0x01
	checksumSize     = 4
	metadataSize     = 8
	defaultBatchSize = 128

	// knownFlags is the bitmask of all flag bits the reader understands.
	// Unknown bits cause Open to reject the file.
	knownFlags uint16 = 0
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
