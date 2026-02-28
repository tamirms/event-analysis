package rocksdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/event"
	"github.com/tamir/events-analysis/rocksdbutil"
)

var _ bitmapindex.IndexWriter = (*Writer)(nil)

// compositeKey builds a RocksDB key: field_byte || original_key.
func compositeKey(f bitmapindex.Field, key []byte) []byte {
	ck := make([]byte, 1+len(key))
	ck[0] = byte(f)
	copy(ck[1:], key)
	return ck
}

// WriterOptions configures the RocksDB bitmap index writer.
type WriterOptions struct {
	CapacityHint int // pre-sizes internal map; 0 -> no hint
}

// Writer accumulates (field, key) -> bitmap mappings and writes them to a RocksDB
// via SST file ingest.
type Writer struct {
	dbPath  string
	bitmaps map[string]*roaring.Bitmap // composite key -> bitmap
}

// NewWriter creates a new RocksDB bitmap index writer.
func NewWriter(dbPath string, opts WriterOptions) *Writer {
	hint := max(opts.CapacityHint, 0)
	return &Writer{
		dbPath:  dbPath,
		bitmaps: make(map[string]*roaring.Bitmap, hint),
	}
}

// Add indexes all fields of ev (ContractID and up to 4 topics) under the given ordinal.
func (w *Writer) Add(ev *event.Event, id uint32) {
	if ev.ContractID != nil {
		w.add(bitmapindex.FieldContractID, ev.ContractID, id)
	}
	for i, topic := range ev.Topics {
		if i >= 4 {
			break
		}
		w.add(bitmapindex.FieldTopic0+bitmapindex.Field(i), topic, id)
	}
}

func (w *Writer) add(f bitmapindex.Field, key []byte, ordinal uint32) {
	ck := string(compositeKey(f, key))
	bm := w.bitmaps[ck]
	if bm == nil {
		bm = roaring.New()
		w.bitmaps[ck] = bm
	}
	bm.Add(ordinal)
}

// Finish builds the SST file and ingests it into a new RocksDB.
// ctx is accepted for interface conformance but not honored.
func (w *Writer) Finish(_ context.Context) error {
	if len(w.bitmaps) == 0 {
		return fmt.Errorf("rocksdb bitmapindex: no keys to index")
	}

	// Collect and sort entries lexicographically.
	type entry struct {
		key    string
		bitmap *roaring.Bitmap
	}
	entries := make([]entry, 0, len(w.bitmaps))
	for k, bm := range w.bitmaps {
		entries = append(entries, entry{key: k, bitmap: bm})
	}
	w.bitmaps = nil
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Write SST file.
	cfg := rocksdbutil.SSTWriteConfig{
		BlockSize:            4096,
		BlockRestartInterval: 16,
	}
	sstOpts, cleanup := rocksdbutil.NewSSTWriteOptions(cfg)
	defer cleanup()

	sstPath := filepath.Join(filepath.Dir(w.dbPath), "bitmapindex_ingest.sst")
	envOpts := grocksdb.NewDefaultEnvOptions()
	sfw := grocksdb.NewSSTFileWriter(envOpts, sstOpts)

	sstOK := false
	defer func() {
		sfw.Destroy()
		envOpts.Destroy()
		if !sstOK {
			os.Remove(sstPath)
		}
	}()

	if err := sfw.Open(sstPath); err != nil {
		return fmt.Errorf("rocksdb bitmapindex: sst open: %w", err)
	}

	for i := range entries {
		entries[i].bitmap.RunOptimize()
		val, err := entries[i].bitmap.ToBytes()
		if err != nil {
			return fmt.Errorf("rocksdb bitmapindex: serialize bitmap: %w", err)
		}
		entries[i].bitmap = nil
		if err := sfw.Add([]byte(entries[i].key), val); err != nil {
			return fmt.Errorf("rocksdb bitmapindex: sst add: %w", err)
		}
	}

	if err := sfw.Finish(); err != nil {
		return fmt.Errorf("rocksdb bitmapindex: sst finish: %w", err)
	}
	sstOK = true

	return rocksdbutil.IngestSST(w.dbPath, sstPath, cfg)
}
