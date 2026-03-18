package rocksdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
	"github.com/tamir/events-analysis/eventstore"
	"github.com/tamir/events-analysis/rocksdbutil"
)

var _ eventstore.StoreWriter = (*Writer)(nil)

// WriterOptions configures the RocksDB eventstore writer.
type WriterOptions struct {
	BlockSize          int  // SST block size; 0 -> SSTWriteConfig default (25600)
	CompressionThreads int  // parallel ZSTD; 0 or 1 -> serial
	NoCompression      bool
}

// Writer writes events to a RocksDB via SST file ingest.
type Writer struct {
	dbPath  string
	sstPath string
	sfw     *grocksdb.SSTFileWriter
	envOpts *grocksdb.EnvOptions
	cleanup func() // destroys SST write options
	cfg    rocksdbutil.SSTWriteConfig
	count  int
	key    [4]byte
	closed bool
}

// Create starts writing a new RocksDB eventstore at dbPath.
func Create(dbPath string, opts WriterOptions) (*Writer, error) {
	cfg := rocksdbutil.SSTWriteConfig{
		BlockSize:          opts.BlockSize,
		CompressionThreads: opts.CompressionThreads,
		NoCompression:      opts.NoCompression,
	}
	sstOpts, cleanup := rocksdbutil.NewSSTWriteOptions(cfg)

	// SST file must be on the same filesystem as dbPath for MoveFiles.
	sstPath := filepath.Join(filepath.Dir(dbPath), "ingest.sst")
	envOpts := grocksdb.NewDefaultEnvOptions()
	sfw := grocksdb.NewSSTFileWriter(envOpts, sstOpts)
	if err := sfw.Open(sstPath); err != nil {
		sfw.Destroy()
		envOpts.Destroy()
		cleanup()
		return nil, fmt.Errorf("rocksdb eventstore: open sst: %w", err)
	}
	return &Writer{
		dbPath:  dbPath,
		sstPath: sstPath,
		sfw:     sfw,
		envOpts: envOpts,
		cleanup: cleanup,
		cfg:     cfg,
	}, nil
}

func (w *Writer) Append(event []byte) error {
	if w.closed {
		return errors.New("rocksdb eventstore: writer is closed")
	}
	if w.count > math.MaxUint32 {
		return fmt.Errorf("rocksdb eventstore: event count %d exceeds uint32 max", w.count)
	}
	binary.BigEndian.PutUint32(w.key[:], uint32(w.count))
	if err := w.sfw.Add(w.key[:], event); err != nil {
		return fmt.Errorf("rocksdb eventstore: sst add: %w", err)
	}
	w.count++
	return nil
}

func (w *Writer) Finish() error {
	if w.closed {
		return errors.New("rocksdb eventstore: writer is closed")
	}
	w.closed = true
	if w.count == 0 {
		// RocksDB cannot create an SST file with no entries.
		// Clean up SST resources and create an empty DB.
		w.destroySST()
		os.Remove(w.sstPath)
		return rocksdbutil.CreateEmptyDB(w.dbPath, w.cfg)
	}
	if err := w.sfw.Finish(); err != nil {
		w.destroySST()
		return fmt.Errorf("rocksdb eventstore: sst finish: %w", err)
	}
	w.destroySST()
	return rocksdbutil.IngestSST(w.dbPath, w.sstPath, w.cfg)
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.destroySST()
	os.Remove(w.sstPath)
	return nil
}

func (w *Writer) destroySST() {
	w.sfw.Destroy()
	w.envOpts.Destroy()
	w.cleanup()
}
