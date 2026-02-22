package bitmapindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/tamirms/streamhash"
	streamerrors "github.com/tamirms/streamhash/errors"

	"github.com/tamir/events-analysis/packfile"
	"github.com/tamir/events-analysis/zstd"
)

// Reader provides point lookups from an MPHF+packfile bitmap index.
// Thread-safe for concurrent Lookup calls after Open.
type Reader struct {
	mphf *streamhash.Index
	pack *packfile.Reader
}

// Option configures Reader behavior.
type Option func(*openConfig)

type openConfig struct {
	expectedLookups int
}

// WithExpectedLookups hints how many lookups will be performed,
// driving the MPHF prefetch decision.
func WithExpectedLookups(n int) Option {
	return func(c *openConfig) {
		c.expectedLookups = n
	}
}

// Open opens a bitmap index for querying.
// mphfPath is the streamhash MPHF file, dataPath is the packfile with bitmap records.
// The MPHF is adaptively prefetched based on file size and expected lookups.
func Open(mphfPath, dataPath string, opts ...Option) (*Reader, error) {
	cfg := openConfig{expectedLookups: 0}
	for _, o := range opts {
		o(&cfg)
	}

	// Open MPHF with adaptive prefetch.
	f, err := os.Open(mphfPath)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: open MPHF: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("bitmapindex: stat MPHF: %w", err)
	}
	fileSize := stat.Size()

	// Prefetch wins when expectedLookups+1 > ceil(fileSize/256KB).
	const ebsIOPSize = 256 * 1024
	readIOPs := (fileSize + ebsIOPSize - 1) / ebsIOPSize
	shouldPrefetch := int64(cfg.expectedLookups)+1 > readIOPs

	var mphf *streamhash.Index
	if shouldPrefetch {
		data, err := io.ReadAll(f)
		closeErr := f.Close()
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: read MPHF: %w", err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("bitmapindex: close MPHF: %w", closeErr)
		}
		mphf, err = streamhash.OpenBytes(data)
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: parse MPHF: %w", err)
		}
	} else {
		mphf, err = streamhash.OpenFile(f)
		closeErr := f.Close()
		if err != nil {
			return nil, fmt.Errorf("bitmapindex: mmap MPHF: %w", err)
		}
		if closeErr != nil {
			mphf.Close()
			return nil, fmt.Errorf("bitmapindex: close MPHF file: %w", closeErr)
		}
	}

	// Open packfile.
	pack, err := packfile.Open(dataPath)
	if err != nil {
		mphf.Close()
		return nil, fmt.Errorf("bitmapindex: open packfile: %w", err)
	}

	return &Reader{mphf: mphf, pack: pack}, nil
}

// bufPool reuses pread + decompression buffers across lookups.
var bufPool = sync.Pool{
	New: func() any {
		dec := zstd.NewDecompressor()
		lb := &lookupBuf{
			rec: make([]byte, 0, 4096),
			dec: dec,
		}
		runtime.SetFinalizer(lb, func(b *lookupBuf) { b.dec.Close() })
		return lb
	},
}

type lookupBuf struct {
	rec       []byte
	decompBuf []byte
	dec       *zstd.Decompressor
}

func getBuf() *lookupBuf {
	return bufPool.Get().(*lookupBuf)
}

func putBuf(b *lookupBuf) {
	b.rec = b.rec[:0]
	b.decompBuf = b.decompBuf[:0]
	bufPool.Put(b)
}

// Lookup returns the roaring bitmap for the given key, or ErrKeyNotFound
// if the key is not in the index (fingerprint mismatch).
// The key should be pre-composed via ComposeKey if field disambiguation is needed.
func (r *Reader) Lookup(key []byte) (*roaring.Bitmap, error) {
	// Hash key for MPHF query.
	var hk [16]byte
	streamhash.PreHashInPlace(key, hk[:])

	// Query MPHF for rank.
	rank, err := r.mphf.Query(hk[:])
	if err != nil {
		if errors.Is(err, streamerrors.ErrNotFound) || errors.Is(err, streamerrors.ErrFingerprintMismatch) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("bitmapindex: MPHF query: %w", err)
	}

	// Read record from packfile.
	lb := getBuf()
	defer putBuf(lb)

	lb.rec, err = r.pack.ReadRecordInto(int(rank), lb.rec)
	if err != nil {
		return nil, fmt.Errorf("bitmapindex: read record at rank %d: %w", rank, err)
	}

	rec := lb.rec
	if len(rec) < fingerprintSize+1+checksumSize {
		return nil, fmt.Errorf("bitmapindex: record too short at rank %d: %d bytes", rank, len(rec))
	}

	// Verify CRC32C before any other processing.
	storedCRC := binary.LittleEndian.Uint32(rec[len(rec)-checksumSize:])
	computedCRC := crc32.Checksum(rec[:len(rec)-checksumSize], crc32cTable)
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("bitmapindex: CRC32C mismatch at rank %d: stored=%08x computed=%08x", rank, storedCRC, computedCRC)
	}
	rec = rec[:len(rec)-checksumSize] // strip CRC for downstream parsing

	// Verify fingerprint: first 4 bytes of record vs first 4 bytes of hashed key.
	if rec[0] != hk[0] || rec[1] != hk[1] || rec[2] != hk[2] || rec[3] != hk[3] {
		return nil, ErrKeyNotFound
	}

	flags := rec[fingerprintSize]
	data := rec[fingerprintSize+1:]

	// Decompress if needed.
	if flags&flagCompressed != 0 {
		decoded, decErr := lb.dec.Decode(lb.decompBuf[:0], data)
		if decErr != nil {
			return nil, fmt.Errorf("bitmapindex: decompress bitmap at rank %d: %w", rank, decErr)
		}
		lb.decompBuf = decoded
		data = decoded
	}

	// Deserialize roaring bitmap. Must use UnmarshalBinary (copies data)
	// rather than FromBuffer, because data may reference the pool buffer
	// (uncompressed path) which is returned to sync.Pool on defer.
	bm := roaring.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("bitmapindex: deserialize bitmap at rank %d: %w", rank, err)
	}

	return bm, nil
}

// Close releases all resources.
func (r *Reader) Close() error {
	mErr := r.mphf.Close()
	pErr := r.pack.Close()
	return errors.Join(mErr, pErr)
}
