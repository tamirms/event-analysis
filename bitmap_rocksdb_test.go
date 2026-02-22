package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/linxGnu/grocksdb"

	"github.com/tamir/events-analysis/bitmapindex"
	"github.com/tamir/events-analysis/eventstore"
)

// buildRocksDBFromEventStore streams all events and builds a RocksDB SSTable-based
// bitmap index for benchmarking against the MPHF+packfile approach.
//
// Key schema: field_byte (1B) || original_key (variable)
// This is intentionally different from the MPHF key schema (original_key || field_byte)
// because RocksDB benefits from field-prefix grouping for sorted scans, while MPHF
// only needs collision avoidance via the trailing discriminator.
func buildRocksDBFromEventStore(ctx context.Context, storePath, dbPath string) error {
	er, err := eventstore.Open(storePath)
	if err != nil {
		return fmt.Errorf("bitmapindex: open eventstore: %w", err)
	}
	defer er.Close()

	// Phase 1: Stream events, build bitmaps in memory.
	type compositeKey struct {
		field field
		key   string // stored as string for map key
	}
	bitmaps := make(map[compositeKey]*roaring.Bitmap, 600_000)

	ordinal := uint32(0)
	for event, err := range er.ReadEvents(0, er.EventCount()) {
		if err != nil {
			return fmt.Errorf("bitmapindex: read event %d: %w", ordinal, err)
		}
		for _, f := range allFields {
			key := extractKey(event, f)
			if key == nil {
				continue
			}
			ck := compositeKey{field: f, key: string(key)}
			bm := bitmaps[ck]
			if bm == nil {
				bm = roaring.New()
				bitmaps[ck] = bm
			}
			bm.Add(ordinal)
		}
		ordinal++
	}

	// Phase 2: Collect composite keys, sort lexicographically.
	type sortEntry struct {
		compositeKey []byte // field_byte || original_key
		bitmap       *roaring.Bitmap
	}
	entries := make([]sortEntry, 0, len(bitmaps))
	for ck, bm := range bitmaps {
		// RocksDB key: field_byte || original_key (field prefix for sorted scans).
		key := make([]byte, 1+len(ck.key))
		key[0] = byte(ck.field)
		copy(key[1:], ck.key)
		entries = append(entries, sortEntry{compositeKey: key, bitmap: bm})
	}
	bitmaps = nil // free map

	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].compositeKey) < string(entries[j].compositeKey)
	})

	// Phase 3: Write SST file via SSTFileWriter.
	sstFilePath := filepath.Join(filepath.Dir(dbPath), "bitmapindex_ingest.sst")

	writeOpts := grocksdb.NewDefaultOptions()
	writeOpts.SetCompression(grocksdb.ZSTDCompression)
	writeOpts.SetCompressionOptions(grocksdb.NewCompressionOptions(-14, 3, 0, 0))
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockSize(4096)
	bbto.SetBlockSizeDeviation(100)
	bbto.SetFormatVersion(5)
	bbto.SetBlockRestartInterval(16)
	writeOpts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()

	envOpts := grocksdb.NewDefaultEnvOptions()
	sfw := grocksdb.NewSSTFileWriter(envOpts, writeOpts)
	if err := sfw.Open(sstFilePath); err != nil {
		envOpts.Destroy()
		writeOpts.Destroy()
		return fmt.Errorf("bitmapindex: sst open: %w", err)
	}

	for i := range entries {
		entries[i].bitmap.RunOptimize()
		val, err := entries[i].bitmap.ToBytes()
		if err != nil {
			sfw.Destroy()
			envOpts.Destroy()
			writeOpts.Destroy()
			return fmt.Errorf("bitmapindex: serialize bitmap: %w", err)
		}
		entries[i].bitmap = nil
		if err := sfw.Add(entries[i].compositeKey, val); err != nil {
			sfw.Destroy()
			envOpts.Destroy()
			writeOpts.Destroy()
			return fmt.Errorf("bitmapindex: sst add: %w", err)
		}
	}

	if err := sfw.Finish(); err != nil {
		sfw.Destroy()
		envOpts.Destroy()
		writeOpts.Destroy()
		return fmt.Errorf("bitmapindex: sst finish: %w", err)
	}
	sfw.Destroy()
	envOpts.Destroy()
	writeOpts.Destroy()

	// Phase 4: Create DB and ingest SST file.
	dbOpts := grocksdb.NewDefaultOptions()
	dbOpts.SetCreateIfMissing(true)
	dbOpts.SetCompression(grocksdb.ZSTDCompression)
	dbBbto := grocksdb.NewDefaultBlockBasedTableOptions()
	dbBbto.SetNoBlockCache(true)
	dbBbto.SetBlockSize(4096)
	dbBbto.SetFormatVersion(5)
	dbOpts.SetBlockBasedTableFactory(dbBbto)
	dbBbto.Destroy()

	db, err := grocksdb.OpenDb(dbOpts, dbPath)
	if err != nil {
		dbOpts.Destroy()
		return fmt.Errorf("bitmapindex: open rocksdb: %w", err)
	}

	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	ingestOpts.SetMoveFiles(true)
	if err := db.IngestExternalFile([]string{sstFilePath}, ingestOpts); err != nil {
		ingestOpts.Destroy()
		db.Close()
		dbOpts.Destroy()
		return fmt.Errorf("bitmapindex: ingest: %w", err)
	}
	ingestOpts.Destroy()
	db.Close()
	dbOpts.Destroy()
	os.Remove(sstFilePath)

	return nil
}

// RocksDBReader provides bitmap lookups from a RocksDB SSTable-based index.
type RocksDBReader struct {
	db   *grocksdb.DB
	ro   *grocksdb.ReadOptions
	opts *grocksdb.Options
}

// openRocksDB opens a RocksDB bitmap index for read-only access.
func openRocksDBBitmap(dbPath string) (*RocksDBReader, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetSkipStatsUpdateOnDBOpen(true)
	opts.SetSkipCheckingSSTFileSizesOnDBOpen(true)
	opts.SetMaxFileOpeningThreads(1)
	opts.SetDisableAutoCompactions(true)
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	bbto.SetFormatVersion(5)
	opts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		opts.Destroy()
		return nil, fmt.Errorf("bitmapindex: open rocksdb: %w", err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(false)
	ro.SetFillCache(false)

	return &RocksDBReader{db: db, ro: ro, opts: opts}, nil
}

// Lookup returns the roaring bitmap for the given field/key.
// Returns ErrKeyNotFound if the key is not found.
func (r *RocksDBReader) Lookup(f field, key []byte) (*roaring.Bitmap, error) {
	// Construct composite key: field_byte || original_key.
	compositeKey := make([]byte, 1+len(key))
	compositeKey[0] = byte(f)
	copy(compositeKey[1:], key)

	val, err := r.db.GetPinned(r.ro, compositeKey)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: rocksdb get: %w", err)
	}
	defer val.Destroy()

	if !val.Exists() {
		return nil, bitmapindex.ErrKeyNotFound
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(val.Data()); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap: %w", err)
	}

	return bm, nil
}

// Close releases all RocksDB resources.
func (r *RocksDBReader) Close() {
	r.ro.Destroy()
	r.db.Close()
	r.opts.Destroy()
}
