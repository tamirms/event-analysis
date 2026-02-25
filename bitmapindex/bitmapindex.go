package bitmapindex

import (
	"context"
	"hash/crc32"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamir/events-analysis/event"
)

// Batch record format constants shared by reader and writer.
const (
	fingerprintSize = 4
	flagCompressed  = 0x01 // per-bitmap flag in batch records
	checksumSize    = 4
	metadataSize    = 8
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// IndexWriter accumulates events and builds an index.
type IndexWriter interface {
	Add(ev *event.Event, id uint32)
	Finish(ctx context.Context) error
}

// IndexReader provides point lookups against a bitmap index.
type IndexReader interface {
	Lookup(f Field, key []byte) (*roaring.Bitmap, error)
	LookupKeys(ctx context.Context, keys []FieldKey) ([]*roaring.Bitmap, error)
	Close() error
}

// Compile-time conformance checks.
var (
	_ IndexWriter = (*Writer)(nil)
	_ IndexReader = (*Reader)(nil)
)
