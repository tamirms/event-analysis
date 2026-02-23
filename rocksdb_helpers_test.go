package main

import (
	"github.com/linxGnu/grocksdb"
)

// openReadOnlyRocksDB opens a RocksDB for read-only benchmarks with standard options:
// skip stats/size checks, single file-opening thread, no auto compaction, no block cache.
func openReadOnlyRocksDB(path string) (*grocksdb.DB, *grocksdb.Options, error) {
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
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}
	return db, opts, nil
}

// newBenchReadOptions creates ReadOptions for benchmark reads (no checksums, no cache fill).
func newBenchReadOptions() *grocksdb.ReadOptions {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetVerifyChecksums(false)
	ro.SetFillCache(false)
	return ro
}
