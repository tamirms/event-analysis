package bitmapindex

import (
	"context"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamir/events-analysis/event"
)

// Batch record format constant shared by reader and writer.
const fingerprintSize = 4

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
