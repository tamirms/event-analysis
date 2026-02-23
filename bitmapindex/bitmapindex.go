package bitmapindex

import "hash/crc32"

// Batch record format constants shared by reader and writer.
const (
	fingerprintSize = 4
	flagCompressed  = 0x01 // per-bitmap flag in batch records
	checksumSize    = 4
	metadataSize    = 8
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
