# Packfile Library Design

## Problem

We need to store large collections of variable-length items (events, compressed bitmaps, ledgers) in immutable files and read individual items by ordinal index with minimal I/O. The files are written once and read many times over their lifetime.

Items are grouped into **records** for compression. Small items like events and bitmaps are batched together (e.g. 128 per record) because a single 200-byte event doesn't compress meaningfully, but 128 of them together do. Large items like ledgers are stored one per record since they're big enough to compress individually. Each record is compressed and written as a contiguous block on disk.

When a record contains multiple items, the reader needs to know where each item starts and ends within the decompressed data. A small size table appended to each multi-item record stores these byte lengths. Single-item records skip the size table entirely — the item is the whole payload.

The natural approach is a flat file: write items sequentially, keep an offset table at the end, look up any item by index. This is simple, fast, and well-suited to immutable data. General-purpose storage engines like RocksDB or SQLite add key management, transactions, and mutable write paths that we don't need — overhead without benefit for immutable, ordinal-indexed data.

Packfile is a production implementation of this approach. It handles the details that a naive implementation gets wrong or leaves to the caller: compact index encoding, parallel compression, SHA-256 content hashing, atomic writes, CRC32C integrity checks, and safe concurrent reads.

## What Packfile Does

Two packages:

- **`packfile`** — item-level random access to immutable files. Handles record grouping, zstd compression, CRC32C integrity, content hashing, and parallel I/O. Uses CGo via `zstd/` for compression.
- **`intpack`** — integer compression using Frame of Reference (FOR) encoding. Pure Go, no external dependencies. General-purpose codec — not packfile-specific.

Items are grouped into fixed-size records (default 128 items per record), compressed, and written sequentially. An offset table at the end of the file enables O(1) record lookup (encoded compactly using FOR — see [Index Encoding](#index-encoding) below). The `Writer` accumulates items and builds records automatically; the `Reader` maps item indices to records and extracts individual items.

## Usage

Error handling omitted for clarity. All functions return errors.

### Writing

```go
w, _ := packfile.Create("events-00042.pack", packfile.WriterOptions{
    RecordSize:  128,         // items per record (default 128)
    Concurrency: 4,           // parallel compression goroutines
    ContentHash: true,        // compute SHA-256 content hash
})

for _, event := range events {
    w.Append(event)
}

w.Finish(nil) // flushes partial record, writes index + trailer, fsyncs, atomic rename
```

Items are appended in order. `Finish` flushes any partial record, writes the offset index, optional app data, and a 64-byte trailer (containing total items, record size, flags, content hash), fsyncs, and atomically renames the temp file to the final path. If the process crashes before `Finish`, no partial file is left behind.

Multi-part items (e.g., fingerprint + bitmap data) can be appended as a single logical item:

```go
w.Append(fingerprint[:], bitmapData) // concatenated as one entry
```

### Reading: Point Lookup

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

r.ReadItem(42, func(event []byte) error {
    processEvent(event) // valid only for the duration of this call — copy if needed
    return nil
})
```

`Open` returns immediately — all I/O runs in a background goroutine. `ReadItem` maps the item index to its record, reads and decodes the record, and passes the item to the callback. The entry slice is borrowed from an internal decoder buffer and must not be retained after the callback returns.

### Reading: Sequential Scan

`ReadRange` reads a contiguous range of items in batches using a pooled 1MB buffer:

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

for event, err := range r.ReadRange(0, 1000) {
    if err != nil {
        return err
    }
    processEvent(event) // valid only until next iteration — copy if needed
}
```

Safe to break early. No cleanup required. Each yielded `[]byte` is valid only until the next iteration.

### Reading: Scattered Access

A bitmap query produces a set of item indices scattered across the file. `ReadItems` reads them with parallel I/O and decode, calling a callback for each item:

```go
r := packfile.Open("events-00042.pack", packfile.WithConcurrency(8))
defer r.Close()

// indices from bitmap intersection — sorted, unique, possibly non-contiguous
indices := bitmapResult.ToSortedSlice()

results := make([][]byte, len(indices))
r.ReadItems(ctx, indices, func(pos int, entry []byte) error {
    results[pos] = append([]byte(nil), entry...) // copy — entry is borrowed
    return nil
})
```

`ReadItems` groups indices by record, then partitions consecutive records into I/O batches (≤ 1MB each) upfront. Workers claim batches via a simple atomic counter. The callback is called concurrently from multiple goroutines in arbitrary order; `pos` identifies which index the entry corresponds to.

### Content Hash Verification

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

hash, ok, _ := r.ContentHash() // stored SHA-256, if present
if ok {
    err := r.Verify(ctx) // recompute and compare
}
```

### Reading: Key-Based Access

Packfile indexes by ordinal position, not by key. For keyed data, the caller provides its own key → ordinal mapping. Any mapping works: a hash table, a sorted array with binary search, or a perfect hash function. Packfile doesn't know or care how the ordinal was determined.

Example using a minimal perfect hash function (MPHF) for bitmap storage:

```go
// Write — one bitmap per MPHF slot, batched into records
w, _ := packfile.Create("bitmaps.pack", packfile.WriterOptions{RecordSize: 128})
for slot := 0; slot < mphf.Len(); slot++ {
    w.Append(bitmapData[slot])
}
w.Finish(nil)

// Read — MPHF gives ordinal, packfile gives data
r := packfile.Open("bitmaps.pack")
defer r.Close()

slot := mphf.Lookup("USD")
r.ReadItem(slot, func(data []byte) error {
    bitmap.UnmarshalBinary(data) // data is borrowed — copy if needed
    return nil
})
```

## Goals

- **O(1) random access by ordinal index.** Every `ReadItem` call maps index to record via arithmetic, then reads and decodes a single record.
- **Minimal I/O.** The full index loads in one disk read on open (~112KB for 68K event blocks). After that, one disk read per record, exact size, no over-read.
- **Compact index.** Index size depends on max record size, not file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.
- **Immutable after write.** No updates, no deletes. Simple, safe, predictable.
- **Concurrent reads.** Multiple goroutines can call `ReadItem` on the same `Reader`. No locks in the read path.

## Non-Goals

- **Key-based lookup.** Packfile is indexed by ordinal position, not by key. Key-based access is built on top by the caller (see key-based access usage example).
- **Mutability.** No append-after-finalize, no updates, no deletes.
- **Chunk management.** Directory layout, chunk-to-sequence mapping, rotation are caller concerns.
- **Caching.** No built-in LRU. Callers manage their own pool of open `Reader` instances.

## API Reference

### Writer

```go
package packfile

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
    // RecordSize is the number of items per record. 0 defaults to 128.
    RecordSize int

    // Format controls record encoding. Default (zero value) is Compressed.
    // Compressed: zstd with built-in integrity.
    // Uncompressed: raw records with CRC32C integrity.
    // Raw: raw records with no integrity wrapper.
    Format RecordFormat

    // Concurrency sets the number of compression goroutines.
    // 0 or 1 means serial. Ignored when Format is not Compressed.
    Concurrency int

    // ContentHash enables SHA-256 content hashing over the logical item stream.
    ContentHash bool

    // BytesPerSync initiates background writeback of dirty pages every N bytes
    // written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
    // is non-blocking — it tells the kernel to start flushing without waiting.
    // This spreads I/O across the write phase so the final fdatasync in Finish()
    // has less data to flush. 0 disables (default).
    BytesPerSync int
}

// Writer creates a new packfile. Items must be appended in order.
type Writer struct{ /* unexported */ }

// Create starts writing a new packfile at path.
// The file is not visible at path until Finish is called.
func Create(path string, opts WriterOptions) (*Writer, error)

// Append adds a single logical item. Parts are concatenated as one entry.
// Flushes a record when RecordSize items accumulate.
func (w *Writer) Append(parts ...[]byte) error

// Finish flushes any partial record, writes index + optional app data + trailer,
// fsyncs, and atomically renames to the final path. appData is optional
// caller-injected data stored between the index and trailer; pass nil for
// no app data. On failure, the caller should call Abort to clean up the temp file.
func (w *Writer) Finish(appData []byte) error

// Abort discards the in-progress packfile and removes the temp file.
// Safe to call after a failed Finish to clean up.
// No-op only after a successful Finish or a previous Abort.
func (w *Writer) Abort() error
```

### Reader

```go
// ReadAtCloser is the minimal interface needed by Reader to access packfile data.
// *os.File satisfies this interface.
type ReadAtCloser interface {
    io.ReaderAt
    io.Closer
}

// ReaderOption configures Reader behavior.
type ReaderOption func(*Reader)

// WithConcurrency sets the max parallel goroutines for ReadItems.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption

// Trailer holds the parsed trailer fields.
type Trailer struct {
    Version        uint8
    RecordCount    uint32
    TotalItems     uint32
    RecordSize     uint32
    IndexSize      uint32
    AppDataSize    uint32
    ContentHash    [32]byte
    Format         RecordFormat // decoded from on-disk flags (Compressed, Uncompressed, or Raw)
    HasContentHash bool         // decoded from on-disk flags bit 1
    Checksum       uint32
}

// Reader provides random access to items in a packfile.
// Safe for concurrent use by multiple goroutines.
type Reader struct{ /* unexported */ }

// Open returns a Reader immediately. All file I/O (open, stat, speculative
// read, trailer parse, index decode, app data read) runs in a background
// goroutine. Open never fails; errors are deferred to the first method that
// needs the result. Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error)

// ReadItem reads a single item by global index and passes it to fn.
// The []byte passed to fn is borrowed and must not be retained after fn
// returns — copy if needed. Returns ErrIndexRange if index is out of
// [0, TotalItems).
func (r *Reader) ReadItem(index int, fn func([]byte) error) error

// ReadRange returns an iterator over count contiguous items starting at start.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error]

// ReadItems reads items at scattered indices with parallel I/O and calls
// fn for each item. fn receives the position in the original indices slice
// and a borrowed entry slice valid only for the duration of the call — copy
// if needed.
//
// fn is called concurrently from multiple goroutines, in arbitrary order.
// The pos argument identifies which index the entry corresponds to.
//
// indices must be sorted ascending with no duplicates.
// Panics if any index is out of range or indices are not sorted/unique.
func (r *Reader) ReadItems(ctx context.Context, indices []int, fn func(pos int, entry []byte) error) error

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error)

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error)

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches.
func (r *Reader) Verify(ctx context.Context) error

func (r *Reader) Trailer() (Trailer, error)

func (r *Reader) Close() error
```

### Errors

```go
var (
    ErrCorrupt              = errors.New("packfile: corrupt file")
    ErrMagic                = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
    ErrVersion              = fmt.Errorf("%w: unsupported version", ErrCorrupt)
    ErrChecksum             = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
    ErrSize                 = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)
    ErrIndexRange           = errors.New("packfile: record index out of range")
    ErrContentHashMismatch  = errors.New("packfile: content hash mismatch")
)

// CRC32C computes CRC32C (Castagnoli) of b.
func CRC32C(b []byte) uint32
```

### intpack (separate package)

Frame of Reference (FOR) integer compression. Two formats: standard `[W][min][packed]` for streaming decode (used by the file-level offset index), and trailing `[min][packed][W]` for appending after variable-length data (used by record-level size tables).

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
│ offset index (FOR groups)        │
│ CRC32C (4 bytes)                 │
├──────────────────────────────────┤  (optional)
│ app data                         │
├──────────────────────────────────┤
│ trailer (64 bytes)               │
└──────────────────────────────────┘  EOF
```

### Records

Each record contains up to `RecordSize` items. When a record contains multiple items (`RecordSize > 1`), the items are concatenated and followed by a trailing size table that stores each item's byte length. This enables extracting individual items without scanning all preceding data. The size table is encoded using the `intpack` trailing format (a compact bit-packed representation — see [intpack](#intpack-separate-package) in the API reference).

```
Multi-item record layout (RecordSize > 1):
  [item_0 bytes][item_1 bytes]...[item_{N-1} bytes][trailing size table]

Trailing size table (intpack trailing format):
  [4B min LE][packed residuals][1B W]
```

**Single-item records** (`RecordSize=1`): The trailing size table is omitted entirely — the item's length equals the decompressed payload length. This saves 6 bytes per record and avoids a decode step on every read.

```
Single-item record layout (RecordSize=1):
  [item bytes]
```

**Compressed records** (default): The entire record payload is zstd-compressed. Integrity is provided by zstd's built-in content checksum (xxHash64), verified automatically during decompression.

**Uncompressed records** (`Format: Uncompressed`): The record is stored as-is with a trailing 4-byte CRC32C appended after the payload.

**Raw records** (`Format: Raw`): The record is stored as-is with no integrity wrapper. Use this when items are already compressed or checksummed.

### Content Hash

When `ContentHash: true`, the writer computes a chunked SHA-256 over the logical item stream:

```
chunkDigest_i = SHA-256([4B len][item_{i*K}] ... [4B len][item_{i*K+K-1}])
finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
K = RecordSize
```

The hash depends on record size (chunk boundaries), item order, and item content. Same items with the same record size in the same order produce the same hash. The hash is independent of compression and format version. For concurrent writes, per-worker hash goroutines compute chunk digests in parallel with zstd compression.

### Trailer (64 bytes at EOF)

```
Offset  Size  Type      Field
0       4     uint32    magic (0x534C4348)
4       1     uint8     version (1)
5       1     uint8     flags
6       4     uint32    recordCount
10      4     uint32    totalItems
14      4     uint32    recordSize
18      4     uint32    indexSize (all groups + 4-byte CRC32C)
22      4     uint32    appDataSize (0 if none)
26      32    [32]byte  contentHash (zeroed if flagContentHash not set)
58      2     -         reserved (zero)
60      4     uint32    CRC32C of (appData || trailer[0:60])
```

Flags (uint8):
- Bit 0 (`flagNoCompression`): records are uncompressed with CRC32C
- Bit 1 (`flagContentHash`): trailer contains a 32-byte SHA-256 content hash
- Bit 2 (`flagNoCRC`): per-record CRC32C is omitted (only with flagNoCompression)

The `Checksum` at offset 60 covers `appData || trailer[0:60]` — when no app data is present, just `trailer[0:60]`. The reader validates flags against `knownFlags` and rejects files with unknown flag bits. The on-disk flags byte is decoded into a `RecordFormat` value and `HasContentHash` boolean in the `Trailer` struct — callers never touch raw flag bits.

The group size (128) is a library constant; if it changes, the version is bumped.

### Index Encoding

A naive offset table stores one absolute byte offset per record. As the file grows, each offset needs more bits — a 50GB file requires 36-bit offsets even though individual records might only be 6KB. The offset table size becomes a function of the total file size rather than the record sizes.

Packfile avoids this by storing **record sizes** (deltas between consecutive offsets) instead of absolute positions. Deltas depend on the maximum record size, not the total file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.

Deltas are encoded using **Frame of Reference (FOR)** compression in groups of 128. FOR is a simple integer compression technique: subtract a per-group minimum from every value, then bit-pack the residuals at the minimum bit width needed. Each group of 128 consecutive deltas is self-contained:

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

The per-group minimum subtraction reduces the effective bit width. A group where all records are between 5,000 and 5,200 bytes needs only 8 bits for residuals (range of 200), regardless of the absolute delta magnitude.

**Resolving item `i`:**

```
recordIdx = i / RecordSize
localIdx  = i % RecordSize
offset    = offsets[recordIdx]   // from decoded offset table
```

On open, all groups are decoded into a flat `[]int64` offset table. Each `ReadItem` is an array lookup + single disk read + decode.

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

### Integrity

**Index checksum:** A CRC32C of the raw index bytes (all FOR groups, excluding the CRC itself) is stored at the end of the index section (after the last group, before metadata). On open, the library verifies this checksum before decoding. This catches any on-disk corruption in group headers or packed residuals. As an additional sanity check, after decoding, the library asserts that the running offset sum equals `indexBase` — an independent structural invariant that catches encode/decode logic bugs.

**Trailer checksum:** CRC32C of `appData || trailer[0:60]` protects all structural fields and optional app data.

**Trailer validation:** On open, the reader validates flags against `knownFlags`, rejects unknown flags, validates `recordSize > 0`, and cross-validates that `ceil(totalItems / recordSize) == recordCount`.

**Record checksums:** Compressed records use zstd's built-in content checksum (xxHash64), verified automatically during decompression. Uncompressed records use a trailing CRC32C. Raw records (`Format: Raw`) have no per-record integrity — use this only when items are already checksummed.

**Content hash verification:** `Verify(ctx)` recomputes the SHA-256 content hash by streaming all items via `ReadRange` and compares to the stored hash. Returns `ErrContentHashMismatch` on mismatch.

### Edge Cases

**Last group:** If `RecordCount` is not a multiple of 128, remaining residual slots are zero-padded. The reader respects `RecordCount` and never accesses padding. The group's `W'` and `groupMin` are computed from actual deltas only.

**Last record:** If `TotalItems` is not a multiple of `RecordSize`, the last record contains fewer items. `itemsInRecord()` handles this.

**8-byte read overshoot:** The bit extraction reads 8 bytes at a time. For the last residual in a group (W'=13, j=127): bit position 1651, byte position `5 + 206 = 211`, 8-byte read spans bytes 211-218 — extending past the group's packed section (213 bytes). For middle groups, the overshoot lands in the next group's header, which is safe because the mask extracts only the relevant bits. For the last group, the overshoot may extend past the packed data into the 4-byte CRC that follows. The reader allocates `indexSize + 7` bytes for the read buffer and zeroes the trailing 7, so the overshoot always reads from valid memory.

**Zero items:** Valid. No records. Index section is just 4 bytes (CRC32C of empty payload). All read operations return errors or empty results.

**Single item:** One record, one group, one delta. The record's end offset is `indexBase` (start of the index section).

---

## Implementation

### Non-blocking Open

`Open` returns a `*Reader` immediately. A background goroutine performs all I/O: open file, stat, speculative read, trailer parse, CRC verification, index decode, app data read. A `sync.Once` drains the goroutine result on the first query method call (`ReadItem`, `TotalItems`, etc.). Errors are deferred to query time — `Open` itself never fails. This enables overlapped initialization: the caller can start loading an MPHF or opening other packfiles while the goroutine runs.

### Speculative Read

On open, the reader issues a single pread of the last `min(256KB, fileSize)` bytes. This usually captures the trailer, app data, and index in one IOP. The trailer is parsed from the tail, and if the full index + app data fit within the speculative buffer, no additional reads are needed. If the tail exceeds 256KB, a single fallback read fetches the remaining data.

### Atomic Writes

`Create` writes to `{path}.tmp.{random}` (random int64 suffix prevents collisions). `Finish` writes remaining items, the offset index, CRC32C, optional app data, and 64-byte trailer, fsyncs, then renames. The parent directory is fsynced to ensure the rename is durable. Crash before rename: no file at final path. Crash after: complete valid packfile.

### ReadItem

Maps item index to record index (`i / RecordSize`), gets a pooled `decoder`, reads the record via `ReadAt`, decodes (decompresses + extracts item sizes from the trailing size table), and passes the item at `i % RecordSize` to the caller's callback as a borrowed slice. The decoder stays alive during the callback, so the entry is valid for its duration. Returns the decoder to the pool after the callback returns.

### ReadRange

Sequential iteration with batch I/O. Computes the first and last record for the requested range. Gets a pooled decoder and a pooled 1MB read buffer. Coalesces consecutive records that fit in the buffer into a single `ReadAt` call. Decodes each record and yields items in range. Oversized records (> 1MB) get one-off allocations.

### ReadItems

Single-pass setup then parallel execution:

**Setup**: Linear scan over sorted indices to partition into I/O batches. Consecutive records whose total bytes ≤ 1MB share a batch (single ReadAt). Non-consecutive records or buffer overflow start a new batch.

**Execution**: Workers (up to `concurrency` goroutines) claim batches via an atomic counter. Each worker reads the coalesced byte range with a single ReadAt into a pooled 1MB buffer, decodes each record in the batch, and calls `fn(pos, entry)` directly with the borrowed entry. Oversized single records get one-off allocations.

No channels, no reorder goroutine — workers call `fn` directly. The caller handles ordering via the `pos` argument (position in the original indices slice). On error or context cancellation, an atomic flag stops remaining workers. A `sync.Once` captures the first error.

### Parallel Compression Pipeline

When `Concurrency > 1`, the writer runs a streaming pipeline:

1. **Append** accumulates items into a buffer until `RecordSize` items.
2. **Flush** builds the uncompressed record and sends it to `workCh`.
3. **N compress workers** receive records from `workCh`, compress via zstd, optionally compute SHA-256 chunk digest in a parallel goroutine (overlapping with CGo zstd), and send results to `resultCh`.
4. **Writer goroutine** receives compressed records from `resultCh`, reorders by block ID, and writes sequentially. Chunk digests are appended to the digest buffer in order.

### BytesPerSync

Optional background writeback via `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux (no-op on other platforms). Every `BytesPerSync` bytes written, the writer tells the kernel to start flushing dirty pages in the preceding range — non-blocking, returns immediately. This spreads I/O across the write phase so the final fsync in `Finish` has less data to flush.

### Index Decode OOM Guard

Before decoding the index, the reader validates that `recordCount` is plausible given `indexSize`. Each index group of up to 128 records requires at least 6 bytes (1B width + 4B min + 1B packed). If `recordCount` exceeds `(indexSize - 4) / 6 * 128`, the file is rejected as corrupt. This prevents crafted trailers from causing huge allocations.

### Concurrency

Safe for concurrent use after `Open`. The decoded `[]int64` offset table and metadata are immutable. `ReadItem` issues a stateless `ReadAt` (pread) with a pooled decoder, calls the callback, and returns the decoder to the pool. `ReadRange` borrows a pooled buffer and decoder. `ReadItems` coordinates parallel workers internally — each worker has its own pooled decoder and buffer.

### Decoder Pool

A `sync.Pool` of `decoder` instances, each owning a `ZSTD_DCtx` (zstd decompression context) allocated via CGo. The pool avoids repeated CGo allocation/deallocation. Decoders are stateless between calls — `Decode` fully resets internal buffers.

### Validation Summary

**On open:** magic, version, trailer CRC32C (covers `appData || trailer[0:60]`), file size consistency, index OOM guard, index CRC32C (raw bytes), final offset equals `indexBase`, trailer flags against `knownFlags`, `recordSize > 0`, `ceil(totalItems / recordSize) == recordCount`.

**On ReadItem:** index in `[0, TotalItems)`.

**On ReadRange:** entire range validated upfront (panics on out-of-range).

**On ReadItems:** sorted-unique invariant (panics on violation), bounds checked against `TotalItems`.

**On Verify:** recomputes SHA-256 content hash over all items, compares to stored hash.

