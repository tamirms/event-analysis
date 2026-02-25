package rocksdbutil

import (
	"fmt"
	"os"

	"github.com/linxGnu/grocksdb"
)

// OpenReadOnly opens a RocksDB for read-only access with benchmark-tuned options:
// SkipStatsUpdateOnDBOpen, SkipCheckingSSTFileSizesOnDBOpen,
// MaxFileOpeningThreads(1), DisableAutoCompactions, NoBlockCache, FormatVersion(5).
// Caller owns and must Destroy both the returned db (via Close) and opts.
func OpenReadOnly(path string) (db *grocksdb.DB, opts *grocksdb.Options, err error) {
	opts = grocksdb.NewDefaultOptions()
	opts.SetSkipStatsUpdateOnDBOpen(true)
	opts.SetSkipCheckingSSTFileSizesOnDBOpen(true)
	opts.SetMaxFileOpeningThreads(1)
	opts.SetDisableAutoCompactions(true)
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	bbto.SetFormatVersion(5)
	opts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()
	db, err = grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}
	return db, opts, nil
}

// NewReadOptions returns ReadOptions tuned for benchmarks:
// VerifyChecksums(false), FillCache(false).
// Caller owns and must Destroy the returned ReadOptions.
func NewReadOptions() *grocksdb.ReadOptions {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(false)
	ro.SetFillCache(false)
	return ro
}

// SSTWriteConfig configures SST file creation and DB ingest options.
type SSTWriteConfig struct {
	BlockSize            int  // default 25600 (128 * ~200B avg event)
	BlockRestartInterval int  // default 128; bitmap uses 16
	BlockSizeDeviation   int  // default 100 (fill blocks fully)
	CompressionThreads   int  // 0 or 1 = serial; >1 = parallel zstd in SST
	NoCompression        bool // true -> NoCompression instead of ZSTD
}

func (c SSTWriteConfig) withDefaults() SSTWriteConfig {
	if c.BlockSize == 0 {
		c.BlockSize = 25600
	}
	if c.BlockRestartInterval == 0 {
		c.BlockRestartInterval = 128
	}
	if c.BlockSizeDeviation == 0 {
		c.BlockSizeDeviation = 100
	}
	return c
}

// NewSSTWriteOptions creates grocksdb Options for SSTFileWriter.
// When NoCompression is false, sets ZSTD compression with
// CompressionOptions(-14, 3, 0, 0).
// Returns options and a cleanup function that destroys all allocated resources.
func NewSSTWriteOptions(cfg SSTWriteConfig) (opts *grocksdb.Options, cleanup func()) {
	cfg = cfg.withDefaults()
	opts = grocksdb.NewDefaultOptions()
	if cfg.NoCompression {
		opts.SetCompression(grocksdb.NoCompression)
	} else {
		opts.SetCompression(grocksdb.ZSTDCompression)
		opts.SetCompressionOptions(grocksdb.NewCompressionOptions(-14, 3, 0, 0))
		if cfg.CompressionThreads > 1 {
			opts.SetCompressionOptionsParallelThreads(cfg.CompressionThreads)
		}
	}
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockSize(cfg.BlockSize)
	bbto.SetBlockSizeDeviation(cfg.BlockSizeDeviation)
	bbto.SetFormatVersion(5)
	bbto.SetBlockRestartInterval(cfg.BlockRestartInterval)
	opts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()
	cleanup = func() { opts.Destroy() }
	return opts, cleanup
}

// newDBOptions creates Options for opening a DB that will receive an ingested SST.
// Caller must call opts.Destroy() when done.
func newDBOptions(cfg SSTWriteConfig) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	if cfg.NoCompression {
		opts.SetCompression(grocksdb.NoCompression)
	} else {
		opts.SetCompression(grocksdb.ZSTDCompression)
	}
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetNoBlockCache(true)
	bbto.SetBlockSize(cfg.BlockSize)
	bbto.SetBlockSizeDeviation(cfg.BlockSizeDeviation)
	bbto.SetBlockRestartInterval(cfg.BlockRestartInterval)
	bbto.SetFormatVersion(5)
	opts.SetBlockBasedTableFactory(bbto)
	bbto.Destroy()
	return opts
}

// CreateEmptyDB creates an empty RocksDB at dbPath (no SST ingest needed).
func CreateEmptyDB(dbPath string, cfg SSTWriteConfig) error {
	cfg = cfg.withDefaults()
	dbOpts := newDBOptions(cfg)
	db, err := grocksdb.OpenDb(dbOpts, dbPath)
	if err != nil {
		dbOpts.Destroy()
		return fmt.Errorf("rocksdbutil: create empty db: %w", err)
	}
	db.Close()
	dbOpts.Destroy()
	return nil
}

// IngestSST creates a new RocksDB at dbPath and ingests the SST file at sstPath.
// Uses MoveFiles(true) to avoid copying. Removes the SST file on success.
// sstPath must be on the same filesystem as dbPath (rename(2) cannot
// cross filesystem boundaries with MoveFiles).
//
// IngestExternalFile internally fsyncs the SST data file after linking it into
// the DB directory (see ExternalSstFileIngestionJob::SyncIngestedFile in
// RocksDB source). This ensures crash-safe durability without any additional
// sync on the caller side.
func IngestSST(dbPath, sstPath string, cfg SSTWriteConfig) error {
	cfg = cfg.withDefaults()
	dbOpts := newDBOptions(cfg)
	db, err := grocksdb.OpenDb(dbOpts, dbPath)
	if err != nil {
		dbOpts.Destroy()
		return fmt.Errorf("rocksdbutil: open db: %w", err)
	}

	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	ingestOpts.SetMoveFiles(true)
	err = db.IngestExternalFile([]string{sstPath}, ingestOpts)
	ingestOpts.Destroy()
	db.Close()
	dbOpts.Destroy()
	if err != nil {
		return fmt.Errorf("rocksdbutil: ingest: %w", err)
	}
	os.Remove(sstPath)
	return nil
}
