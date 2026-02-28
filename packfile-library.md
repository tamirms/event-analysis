# Packfile Library Design

## Problem

We need to store large collections of variable-length records (compressed ledgers, compressed event blocks) in immutable files and read individual records by ordinal index with minimal I/O. The files are written once and read many times over their lifetime.

The natural approach is a flat file: write records sequentially, keep an offset table at the end, look up records by index. This is simple, fast, and well-suited to immutable data. General-purpose storage engines like RocksDB or SQLite add key management, transactions, and mutable write paths that we don't need — overhead without benefit for immutable, ordinal-indexed data.

Packfile is a professional implementation of the flat file approach. It handles the details that a naive implementation gets wrong or leaves to the caller: compact index encoding using Frame of Reference (FOR) compression that scales with record size rather than file size, atomic writes, CRC32C-verified index integrity, and safe concurrent reads.

## What Packfile Does

Three packages:

- **`packfile`** — indexed random access to opaque byte records in immutable files. Pure Go. Depends only on `intpack`.
- **`intpack`** — FOR (Frame-of-Reference) integer encoding. Pure Go, no external dependencies. General-purpose codec — not packfile-specific.
- **`record`** — compression-aware decoder for multi-entry records with trailing FOR size indexes. Handles zstd decompression and CRC32C verification. Uses CGo via `zstd/`. Provides pooled `Decoder`, metadata encoding conventions, and entry extraction. Separate package — packfile treats records as opaque bytes.

A packfile is written once (append records, finalize) and read many times (random access by index). On open, the library reads a compact index and decodes it into an in-memory offset table. Every `ReadRecord` is then an array lookup and a single disk read.

## Usage

Error handling omitted for clarity. All functions return errors.

### Writing

```go
w, _ := packfile.Create("chunk-00042.pack", packfile.WriterOptions{})

for _, ledger := range ledgers {
    w.Append(zstd.Compress(encodeLedger(ledger)))
}

// Metadata is passed to Finish — known at finalization time (e.g. total count).
meta := chunkMeta{FirstLedger: 420000, Count: len(ledgers)}.Marshal()
trailer, _ := w.Finish(meta) // writes index, metadata, fsyncs, atomic rename
```

Records are appended in order. `Finish` builds the index, writes it to disk, fsyncs, and atomically renames the temp file to the final path. If the process crashes before `Finish`, no partial file is left behind.

### Reading: Point Lookup

```go
r := packfile.Open("chunk-00042.pack")
defer r.Close()

var buf []byte
buf, _ = r.ReadRecord(localIndex, buf)
ledger, _ := zstd.Decompress(buf)
```

`Open` returns immediately — all I/O runs in a background goroutine. Every `ReadRecord` is a single disk read — the record's position is already known from the in-memory offset table. The `buf` parameter enables buffer reuse across calls.

### Reading: Scattered Access

A bitmap query produces a set of record indices scattered across the file. `ReadScattered` reads them with parallel I/O, coalescing consecutive indices into batch reads:

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

// indices from bitmap intersection — sorted, unique, possibly non-contiguous
// e.g. [3, 4, 5, 12, 13, 45, 46, 47, 48]
indices := bitmapResult.ToSortedSlice()

err := r.ReadScattered(ctx, indices, runtime.NumCPU(),
    func(inputPos int, data []byte) error {
        rd := record.Get()
        defer record.Put(rd)
        blockIdx := indices[inputPos]
        n := record.ItemsInRecord(totalEvents, blockSize, blockIdx)
        if err := rd.Decode(data, n, compressed); err != nil {
            return err
        }
        // extract entries from rd...
        return nil
    },
)
```

`inputPos` is the position in `indices` — use `indices[inputPos]` to get the actual record index. `data` is borrowed from an internal buffer and must not be retained after `process` returns. Use `record.Get()`/`record.Put()` for per-callback decoders.

### Reading: Sequential Scan

`ReadRecords` reads a contiguous range in batches using a pooled 1MB buffer, yielding zero-copy subslices. Each yielded `[]byte` is valid only until the next iteration — copy it if you need to keep it.

```go
r := packfile.Open("chunk-00042.pack")
defer r.Close()

for raw, err := range r.ReadRecords(startIndex, count) {
    if err != nil {
        return err
    }
    ledger, _ := zstd.Decompress(raw)
}
```

Safe to break early. No cleanup required.

### Reading: Key-Based Access

Packfile indexes by ordinal position, not by key. For keyed data, the caller provides its own key → ordinal mapping. Any mapping works: a hash table, a sorted array with binary search, or a perfect hash function. Packfile doesn't know or care how the ordinal was determined.

Example using a minimal perfect hash function (MPHF) for bitmap storage:

```go
// Write — one bitmap per MPHF slot
w, _ := packfile.Create("bitmaps-00042.pack", packfile.WriterOptions{})
for slot := 0; slot < mphf.Len(); slot++ {
    w.Append(compressedBitmaps[slot])
}
w.Finish(nil) // no metadata

// Read — MPHF gives ordinal, packfile gives data
r := packfile.Open("bitmaps-00042.pack")
defer r.Close()

slot := mphf.Lookup("USD")
var buf []byte
buf, _ = r.ReadRecord(slot, buf)
bitmap := decodeBitmap(buf)
```

## Goals

- **O(1) random access by ordinal index.** Every `ReadRecord` is an array lookup and a single disk read.
- **Minimal I/O.** The full index loads in one disk read on open (~112KB for 68K event blocks). After that, one disk read per record, exact size, no over-read.
- **Compact index.** Index size depends on max record size, not file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.
- **Immutable after write.** No updates, no deletes. Simple, safe, predictable.
- **Concurrent reads.** Multiple goroutines can call `ReadRecord` on the same `Reader`. No locks in the read path.

## Non-Goals (of the packfile package)

The packfile package stores opaque byte records. Compression, checksums, and record framing are handled by layers above it — primarily the `record` package. This is a deliberate separation: packfile owns layout and I/O, the `record` package owns encoding.

- **Compression.** Packfile stores whatever bytes it's given. The `record` package handles zstd compression/decompression.
- **Per-record checksums.** Packfile verifies its index (CRC32C) but not individual records. The `record` package provides two integrity modes: zstd content checksums (compressed) and CRC32C (uncompressed).
- **Record framing.** Internal structure (e.g., multi-entry records with a trailing size index) is handled by the `record` package.
- **Key-based lookup.** Packfile is indexed by ordinal position, not by key. Key-based access is built on top by the caller (see key-based access usage example).
- **Mutability.** No append-after-finalize, no updates, no deletes.
- **Chunk management.** Directory layout, chunk-to-sequence mapping, rotation are caller concerns.
- **Caching.** No built-in LRU. Callers manage their own pool of open `Reader` instances.
- **Lazy index loading.** The full offset table is decoded eagerly, but `Open` is non-blocking — all I/O runs in a background goroutine that blocks on first access via `sync.Once`. This enables overlapped initialization (e.g., loading an MPHF in parallel with packfile open). At our chunk sizes (~10K ledgers, ~68K event blocks), the index fits in a single IOP (112KB) and the decoded table is 544KB.

## API Reference

### Writer

```go
package packfile

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
    // BytesPerSync initiates background writeback of dirty pages every N bytes
    // written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
    // is non-blocking — it tells the kernel to start flushing without waiting.
    // This spreads I/O across the write phase so the final fdatasync in Finish()
    // has less data to flush. 0 disables (default).
    BytesPerSync int
}

// Writer creates a new packfile. Records must be appended in order.
type Writer struct{ /* unexported */ }

// Create starts writing a new packfile at path.
// The file is not visible at path until Finish is called.
func Create(path string, opts WriterOptions) (*Writer, error)

// Append writes a single record. Records are opaque byte slices.
func (w *Writer) Append(record []byte) error

// Abort discards the in-progress packfile and removes the temp file.
// Safe to call after a failed Finish to clean up.
// No-op after a successful Finish or a previous Abort.
func (w *Writer) Abort() error

// Finish writes the index, metadata, and trailer, fsyncs, and
// atomically renames to the final path. metadata is opaque caller-defined
// bytes stored in the file (nil for no metadata). Returns an error if
// the writer has already been finished or aborted.
func (w *Writer) Finish(metadata []byte) (Trailer, error)
```

### Reader

```go
// ReadAtCloser is the minimal interface needed by Reader to access packfile data.
// *os.File satisfies this interface.
type ReadAtCloser interface {
    io.ReaderAt
    io.Closer
}

// Trailer holds the parsed trailer fields.
type Trailer struct {
    Version         uint8
    RecordCount     uint32
    IndexSize       uint32
    MetadataSize    uint32
    TrailerChecksum uint32
}

// Reader provides random access to records in a packfile.
// Safe for concurrent use by multiple goroutines.
type Reader struct{ /* unexported */ }

// Open returns a Reader immediately. All file I/O (open, stat, speculative
// read, trailer parse, index decode) runs in a background goroutine.
// Open never fails; errors are deferred to the first method that needs
// the result. Close must always be called.
func Open(path string) *Reader

// ReadRecord reads a single record into a caller-provided buffer.
// Returns a slice of buf (possibly reallocated if buf is too small).
// Caller must reassign: buf, err = r.ReadRecord(index, buf)
func (r *Reader) ReadRecord(index int, buf []byte) ([]byte, error)

// ReadRecords returns an iterator over count consecutive records
// starting at index. Reads in batches using a pooled 1MB buffer.
// Each yielded []byte is valid only until the next iteration —
// copy if you need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRecords(index, count int) iter.Seq2[[]byte, error]

// ReadScattered reads records at the given sorted, unique indices with
// parallel I/O. It coalesces consecutive indices into batch reads and
// distributes work across goroutines.
//
// process is called exactly once per element of indices:
//   - inputPos: position in the indices slice.
//   - data: the raw record bytes. Borrowed — do not retain after return.
//
// process may be called from multiple goroutines concurrently.
// indices must be sorted ascending with no duplicates (panics otherwise).
// Returns ErrIndexRange if any index is out of [0, RecordCount).
func (r *Reader) ReadScattered(
    ctx context.Context,
    indices []int,
    concurrency int,
    process func(inputPos int, data []byte) error,
) error

func (r *Reader) RecordCount() (int, error)
func (r *Reader) Trailer() (Trailer, error)
func (r *Reader) Metadata() ([]byte, error)

func (r *Reader) Close() error
```

### Errors

```go
var (
    ErrCorrupt      = errors.New("packfile: corrupt file")
    ErrMagic        = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
    ErrVersion      = fmt.Errorf("%w: unsupported version", ErrCorrupt)
    ErrChecksum     = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
    ErrSize         = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)
    ErrIndexRange   = errors.New("packfile: record index out of range")
)
```

### intpack (separate package)

Two formats: standard `[W][min][packed]` for streaming decode, and trailing `[min][packed][W]` for appending after variable-length data (used by record-level size indexes).

```go
package intpack

// EncodeGroup FOR-encodes values into one group: [1B W][4B min LE][packed residuals].
// W = bits.Len32(max - min), clamped to min 1. Pure codec — no CRC, no trailer.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte

// DecodeGroup FOR-decodes one group of n values from data into dst.
// Returns values (possibly reallocated if dst is too small), bytes consumed,
// and any error. data must have 7 bytes of overshoot past the encoded
// payload for safe 8-byte reads.
func DecodeGroup(data []byte, n int, dst []uint32) (values []uint32, size int, err error)

// EncodeTrailingGroup FOR-encodes values into trailing format:
// [4B min LE][packed residuals][1B W]
// W is the last byte, suitable for appending after variable-length data.
// Panics if len(values) == 0.
func EncodeTrailingGroup(values []uint32) []byte

// DecodeTrailingGroup decodes a trailing FOR group from the end of data.
// The trailing format is [4B min LE][packed residuals][1B W] appended after
// variable-length data entries. n is the number of values.
// Returns decoded sizes (possibly reallocated). Validates that sum(sizes)
// equals the data length preceding the trailing group.
func DecodeTrailingGroup(data []byte, n int, sizes []uint32) ([]uint32, error)
```

Note: the group size (128 values) is not fixed by intpack — it's a pure codec. The 128-value group size is a packfile format constant.

### record (separate package)

Compression-aware decoder for packfile records containing multiple entries with a trailing FOR-encoded size index. Handles zstd decompression (content checksum provides integrity) and CRC32C verification (uncompressed records). Uses CGo via `zstd/`.

```go
package record

// FlagNoCompression indicates records are stored uncompressed with CRC32C integrity.
const FlagNoCompression uint32 = 1 << 0

// FlagContentHash indicates metadata contains a 32-byte SHA-256 content hash
// after the standard 12-byte header.
const FlagContentHash uint32 = 1 << 1

var ErrChecksum = errors.New("record: checksum mismatch")

// ErrContentHashMismatch is returned when a file's content hash does not match
// the hash stored in metadata. Used by eventstore and bitmapindex Verify().
var ErrContentHashMismatch = errors.New("record: content hash mismatch")

// Decoder decodes packfile records containing multiple entries with a trailing
// FOR-encoded size index. Handles both zstd-compressed and CRC32C-verified
// uncompressed records.
type Decoder struct{ /* unexported */ }

// New creates a new Decoder with an initialized decompressor.
// Callers must call Close() when done to release CGO resources.
func New() *Decoder

// Close releases the decompressor. Must NOT be called on pooled decoders.
func (rd *Decoder) Close()

// Get returns a Decoder from the pool.
func Get() *Decoder

// Put returns a Decoder to the pool, resetting all buffers.
func Put(rd *Decoder)

// Decode decodes a record containing n entries.
// If compressed, the record is zstd-decompressed (content checksum provides integrity).
// If !compressed, the trailing 4 bytes are verified as CRC32C over the preceding payload.
func (rd *Decoder) Decode(data []byte, n int, compressed bool) error

// ReadAndDecode reads a record from the packfile and decodes it.
func (rd *Decoder) ReadAndDecode(pr *packfile.Reader, recordIdx, n int, compressed bool) error

// Entry returns the entry at index i within the decoded record.
// The returned slice is valid only until the next Decode call.
func (rd *Decoder) Entry(i int) []byte

// EntryCopy returns an owned copy of the entry at index i.
func (rd *Decoder) EntryCopy(i int) []byte

// EncodeMetadata encodes fixed-width 12-byte metadata: [u32 totalItems][u32 recordSize][u32 flags].
func EncodeMetadata(totalItems, recordSize int, flags uint32) []byte

// DecodeMetadata parses fixed-width 12-byte metadata.
func DecodeMetadata(meta []byte) (totalItems, recordSize int, flags uint32, err error)

// ItemsInRecord returns the number of items in the given record index.
// Handles the last record which may be partial.
func ItemsInRecord(totalItems, recordSize, recordIdx int) int

// ContentHash extracts the 32-byte SHA-256 content hash from metadata.
// Layout: [0:4] totalItems, [4:8] recordSize, [8:12] flags, [12:44] SHA-256 hash.
// Returns (hash, true) if FlagContentHash is set and metadata is long enough.
func ContentHash(meta []byte, flags uint32) ([32]byte, bool)
```

---

## File Format

Everything below is internal to the library. Callers don't need to know this — it's here for contributors and the curious.

Reads throughout this section use `pread` — positioned read at a specific file offset without moving a file pointer — which is safe for concurrent use and maps to Go's `os.File.ReadAt`.

### Layout

```
┌──────────────────────────────────┐  offset 0
│ record 0                         │
│ record 1                         │
│ ...                              │
│ record N-1                       │
├──────────────────────────────────┤  indexBase
│ FOR group 0                      │
│ FOR group 1                      │
│ ...                              │
│ FOR group G-1                    │
│ CRC32C (4 bytes)                 │
├──────────────────────────────────┤
│ metadata (optional)              │
├──────────────────────────────────┤
│ trailer (32 bytes)               │
└──────────────────────────────────┘  EOF
```

### Trailer (32 bytes at EOF)

```
Offset  Size  Type    Field
0       4     uint32  magic (0x534C4348)
4       1     uint8   version (1)
5       1     uint8   reserved (must be zero)
6       4     uint32  record_count
10      4     uint32  index_size (all groups + 4-byte CRC32C)
14      4     uint32  metadata_size
18      4     uint32  trailer_checksum (CRC32C of bytes 0..17)
22      10    -       reserved (must be zero)
```

The `trailer_checksum` covers bytes 0..17 — all fields needed to locate and interpret every section of the file. The group size (128) is a library constant; if it changes, the version is bumped.

### Index Encoding

A naive offset table stores one absolute byte offset per record. As the file grows, each offset needs more bits — a 50GB file requires 36-bit offsets even though individual records might only be 6KB. The offset table size becomes a function of the total file size rather than the record sizes.

Packfile avoids this by storing **record sizes** (deltas between consecutive offsets) instead of absolute positions. Deltas depend on the maximum record size, not the total file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.

Deltas are encoded using **Frame of Reference (FOR)** with groups of 128. Each group of 128 consecutive deltas is self-contained:

```
FOR Group (5 + ceil(128 × W' / 8) bytes):

  [00]     W': uint8
           Bit width of residuals in this group (bits.Len32 of max residual).

  [01-04]  groupMin: uint32 LE
           Minimum delta in this group.

  [05-XX]  residuals: 128 packed integers, W' bits each
           residual[j] = delta[j] - groupMin
           where delta[j] = byte size of record (groupIndex × 128 + j)
```

The writer computes `groupMin` and `W'` independently for each group:

```
groupMin  = min(delta[0], delta[1], ..., delta[127])
maxResid  = max(delta[j] - groupMin for j in group)
W'        = bits.Len32(maxResid)
```

The per-group minimum subtraction reduces the effective bit width. A group where all records are between 5,000 and 5,200 bytes needs only 8 bits for residuals (range of 200), regardless of the absolute delta magnitude. In practice, 99.6% of groups achieve `W' = 13` bits.

**Resolving record `i`:**

```
group    = i / 128
localIdx = i % 128
offset   = sum of all preceding record deltas
```

On open, all groups are decoded into a flat `[]int64` offset table. Each `ReadRecord` is then an array lookup. No per-read delta summation.

**Bit extraction (same for encode and decode):**

```go
bitPos  := uint64(j) * uint64(W)
bytePos := 5 + bitPos/8
shift   := bitPos % 8
raw     := binary.LittleEndian.Uint64(groupBuf[bytePos:])
residual := int64((raw >> shift) & ((1 << W) - 1))
delta   := residual + int64(groupMin)
```

Reads 8 bytes, shifts, masks, adds back the group minimum. Maximum usable `W'` is 57 (shift ≤ 7, shift + W' ≤ 64).

**Index sizes at typical chunk sizes (10K-ledger chunks):**

| Use case | Records | Groups | Index on disk | Decoded []int64 |
|----------|---------|--------|---------------|-----------------|
| Ledgers | 10K | 79 | ~20KB | 80KB |
| Event blocks | 68K | 538 | 112KB | 544KB |
| Bitmaps | ~50K | ~391 | ~60-95KB | ~400KB |

The event block index (the largest) fits in a single EBS IOP (112KB, well under the 256KB IOP unit). The decoded table is 544KB — modest relative to the record data being queried.

### Integrity

**Index checksum:** A CRC32C of the raw index bytes (all FOR groups, excluding the CRC itself) is stored at the end of the index section (after the last group, before metadata). On open, the library verifies this checksum before decoding. This catches any on-disk corruption in group headers or packed residuals. As an additional sanity check, after decoding, the library asserts that the running offset sum equals `indexBase` — an independent structural invariant that catches encode/decode logic bugs.

**Trailer checksum:** CRC32C of trailer bytes 0..17 protects all structural fields.

**Record checksums:** Provided by zstd's built-in content checksum (xxHash64), verified automatically during decompression. Callers using the `record` package get integrity for free. Uncompressed records use a trailing CRC32C instead.

### Edge Cases

**Last group:** If `RecordCount` is not a multiple of 128, remaining residual slots are zero-padded. The reader respects `RecordCount` and never accesses padding. The group's `W'` and `groupMin` are computed from actual deltas only.

**8-byte read overshoot:** The bit extraction reads 8 bytes at a time. For the last residual in a group (W'=13, j=127): bit position 1651, byte position `5 + 206 = 211`, 8-byte read spans bytes 211-218 — extending past the group's packed section (213 bytes). For middle groups, the overshoot lands in the next group's header, which is safe because the mask extracts only the relevant bits. For the last group, the overshoot may extend past the packed data into the 4-byte CRC that follows. The reader allocates `indexSize + 7` bytes for the read buffer and zeroes the trailing 7, so the overshoot always reads from valid memory.

**Zero records:** Valid. Index section is just 4 bytes (CRC32C of empty payload). All read operations return errors or empty results.

**Single record:** One group, one delta. The record's end offset is `indexBase` (start of the index section).

---

## Implementation

### Non-blocking Open

`Open` returns a `*Reader` immediately. A background goroutine performs all I/O: open file, stat, speculative read, trailer parse, index CRC verification, FOR decode. A `sync.Once` drains the goroutine result on the first query method call (`ReadRecord`, `RecordCount`, etc.). Errors are deferred to query time — `Open` itself never fails. This enables overlapped initialization: the caller can start loading an MPHF or opening other packfiles while the goroutine runs.

### Speculative Read

On open, the reader issues a single pread of the last `min(256KB, fileSize)` bytes. This usually captures the trailer, index, and metadata in one IOP. The trailer is parsed from the tail, and if the full index + metadata fit within the speculative buffer, no additional reads are needed. If the tail exceeds 256KB, a single fallback read fetches the remaining data. In practice, two reads only happen for files with >68K records (rare at our chunk sizes).

### Atomic Writes

`Create` writes to `{path}.tmp.{random}` (random int64 suffix prevents collisions). `Finish` writes FOR groups, CRC32C, metadata, and trailer, fsyncs, then renames. The parent directory is fsynced to ensure the rename is durable. Crash before rename: no file at final path. Crash after: complete valid packfile.

### ReadScattered

Three phases:

1. **Partition.** Sorted indices are grouped into consecutive runs. For example, `[3, 4, 5, 12, 13, 45]` becomes three runs: `[3,3], [12,2], [45,1]`. Long runs are split so no single worker gets a disproportionate share.

2. **Dispatch.** Workers claim runs via `atomic.Int64` (work-stealing). A single-record run uses `ReadRecord` with a pooled 1MB buffer. A multi-record run delegates to `ReadRecords`, which coalesces the range into sequential batch reads. The pooled buffer is released before entering `ReadRecords` (which has its own pool) to avoid doubling memory.

3. **Callback.** Each record is passed to the caller's `process` function with its `inputPos` (position in the indices slice). Callers use `record.Get()`/`record.Put()` for per-callback decoders.

### BytesPerSync

Optional background writeback via `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux (no-op on other platforms). Every `BytesPerSync` bytes written, the writer tells the kernel to start flushing dirty pages in the preceding range — non-blocking, returns immediately. This spreads I/O across the write phase so the final fsync in `Finish` has less data to flush.

### Index Decode OOM Guard

Before decoding FOR groups, the reader validates that `recordCount` is plausible given `indexSize`. Each FOR group of up to 128 records requires at least 6 bytes (1B width + 4B min + 1B packed). If `recordCount` exceeds `(indexSize - 4) / 6 * 128`, the file is rejected as corrupt. This prevents crafted trailers from causing huge allocations.

### Concurrency

Safe for concurrent use after `Open`. The decoded `[]int64` offset table is immutable. Each `ReadRecord` issues a stateless `ReadAt` (pread). `ReadRecords` borrows a pooled buffer and issues independent preads — each invocation is self-contained. `ReadScattered` coordinates parallel workers internally.

### Validation Summary

**On Open:** magic, version, `trailer_checksum`, file size consistency, index OOM guard, index CRC32C (raw bytes), final offset equals `indexBase`.

**On ReadRecord:** index in `[0, RecordCount)`.

**On ReadRecords:** entire range validated upfront (panics on out-of-range). Index integrity already verified on open.

**On ReadScattered:** sorted-unique invariant (panics on violation), bounds checked against `RecordCount` (returns `ErrIndexRange`).

---

## Application Patterns

Packfile stores opaque records by ordinal index. It has no concept of "events," "blocks," or "compression targets." The patterns below live entirely in the application layer above packfile.

### Event Blocking

Individual events are too small to compress effectively on their own (mean 221 bytes). They must be grouped into blocks and compressed together. Measured compression ratios at varying uncompressed block sizes show the curve flattens around ~28KB — below ~14KB, compression degrades noticeably; above ~28KB, it barely improves but read amplification grows (more events decompressed per point lookup).

The number of events per block is a fixed constant:

```go
const eventsPerBlock = 128
```

This is a design constant, not a tuning parameter. At current mean event size (221 bytes), blocks are ~28KB uncompressed — right at the knee of the compression curve (4.9×). The constant is safe across a wide range of event sizes because the compression curve is flat from ~16KB to ~128KB:

| Mean event size | Block size | Compression | In flat region? |
|---|---|---|---|
| 100 bytes | 12.8KB | 4.3× | Edge (still good) |
| 221 bytes (today) | 28.3KB | 4.9× | Yes (knee) |
| 500 bytes | 64KB | 4.9× | Yes |
| 1000 bytes | 128KB | 4.9× | Edge (still good) |

The compressed block would need to exceed 256KB (the EBS IOP unit) to break the I/O model. At 4.9× compression, that requires ~1.25MB uncompressed — a mean event size of ~9.8KB, or 44× current. No plausible protocol evolution reaches this.

**Why a fixed constant and not adaptive per chunk?** Adaptive approaches (compute `eventsPerBlock` from each chunk's mean event size) introduce a configuration management problem: the constant must be deterministic for reproducibility, must be versioned across protocol upgrades, and must be agreed upon by all nodes. A fixed constant avoids all of this. The flat compression curve provides a ~10× safety range (100-1000 byte mean event sizes) — wide enough that the constant never needs updating. Protocol upgrades that add new operation types or extend fields shift the mean incrementally; a 10× shift would require events to carry fundamentally different data.

**Why fixed count and not fixed byte size?** Fixed byte-size blocks (e.g., always 32KB) require a secondary index mapping event indices to block indices, since the number of events per block varies. With a fixed count, the mapping is O(1) arithmetic: `blockIdx = eventIdx / eventsPerBlock`. This eliminates an entire index structure. Bitmaps have one bit per event; bitmap intersection produces event indices; the query path needs `eventIndex → blockIndex` on every lookup. O(1) addressing here matters.

The block size is stored in the packfile's metadata for the reader:

```go
type EventChunkWriter struct {
    packer  *packfile.Writer
    pending []Event
    count   int
}

func NewEventChunkWriter(path string) (*EventChunkWriter, error) {
    w, err := packfile.Create(path, packfile.WriterOptions{})
    if err != nil {
        return nil, err
    }
    return &EventChunkWriter{packer: w}, nil
}

func (w *EventChunkWriter) AddEvent(e Event) error {
    w.pending = append(w.pending, e)
    w.count++

    if len(w.pending) >= eventsPerBlock {
        return w.flush()
    }
    return nil
}

func (w *EventChunkWriter) flush() error {
    block := encodeBlock(w.pending)
    compressed := zstd.Compress(block)
    if err := w.packer.Append(compressed); err != nil {
        return err
    }
    w.pending = w.pending[:0]
    return nil
}

func (w *EventChunkWriter) Finish() (packfile.Trailer, error) {
    // Build metadata after all events are written — total count now known.
    meta := record.EncodeMetadata(w.count, eventsPerBlock, 0)
    return w.packer.Finish(meta)
}
```

The reader maps event indices to block indices using the stored block size:

```go
type EventChunkReader struct {
    pack       *packfile.Reader
    blockSize  int  // from metadata
    compressed bool // from metadata flags
}

func OpenEventChunk(path string) (*EventChunkReader, error) {
    r := packfile.Open(path)
    meta, err := r.Metadata()
    if err != nil {
        r.Close()
        return nil, err
    }
    totalItems, blockSize, flags, err := record.DecodeMetadata(meta)
    if err != nil {
        r.Close()
        return nil, err
    }
    _ = totalItems
    return &EventChunkReader{
        pack:       r,
        blockSize:  blockSize,
        compressed: flags&record.FlagNoCompression == 0,
    }, nil
}

func (r *EventChunkReader) ReadEvent(eventIdx int) (Event, error) {
    blockIdx := eventIdx / r.blockSize
    localIdx := eventIdx % r.blockSize

    dec := record.Get()
    defer record.Put(dec)

    n := record.ItemsInRecord(totalEvents, r.blockSize, blockIdx)
    if err := dec.ReadAndDecode(r.pack, blockIdx, n, r.compressed); err != nil {
        return Event{}, err
    }

    return parseEvent(dec.Entry(localIdx)), nil
}
```

