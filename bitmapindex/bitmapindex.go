package bitmapindex

import (
	"errors"
	"hash/crc32"
)

// ErrKeyNotFound is returned when a lookup key is not in the index
// (fingerprint mismatch with the MPHF candidate).
var ErrKeyNotFound = errors.New("bitmapindex: key not found")

const (
	fingerprintSize = 4
	flagCompressed  = 0x01
	checksumSize    = 4
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
