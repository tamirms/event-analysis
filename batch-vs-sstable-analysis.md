# Fixed-N Batch vs RocksDB SSTable: Storage Layout Analysis

## Problem

We store Stellar Soroban contract events as binary-encoded blobs averaging 221 bytes
each. A production dataset of ~8.7 million events (1.84 GB raw) serves as the benchmark
corpus. The question: what is the most space-efficient on-disk layout for this data,
given that we need both sequential scanning and random access to individual events?

Two candidate layouts are compared:

1. **RocksDB SSTable** — The standard sorted key-value file format used by RocksDB and
   LevelDB-derived storage engines, produced by grocksdb's SSTFileWriter (RocksDB 10.9.1)
2. **Fixed-N Batch** — A custom format grouping a fixed count of events per block
   (the packfile/eventstore format used in our benchmarks)

Both use zstd block compression. The analysis focuses purely on disk space; read/write
performance is covered in BENCHMARKS.md.

## Layout Descriptions

### RocksDB SSTable

The benchmark uses grocksdb v1.10.7 (wrapping RocksDB 10.9.1) to produce SSTable files
via `SSTFileWriter`. Each event is stored as a KV entry with a 4-byte big-endian ordinal
key and the binary-encoded event as the value.

**Block structure** (uncompressed, before zstd):

```
For each entry in the block:
  [shared_key_prefix_len: varint]     <- prefix compression vs previous key
  [unshared_key_len:      varint]     <- remaining key bytes
  [value_len:             varint]     <- value length
  [unshared_key_bytes]                <- the non-shared portion of the key
  [internal_trailer:      8 bytes]    <- sequence number (7B) + key kind (1B)
  [value_bytes]                       <- the event payload
Every `restartInterval` entries:
  [restart_point:         4 bytes]    <- offset for binary search within block
```

The block is then zstd-compressed and written with a 5-byte trailer
(1B compression type + 4B checksum).

**File structure:**

```
[data block 0][5B trailer] [data block 1][5B trailer] ...
[index block(s)]           <- maps keys to data block offsets
[top-level index]          <- only for two-level indexes
[properties block]
[meta-index block]
[footer: 48 bytes]
```

**Per-entry overhead:**
- 4 bytes user key (ordinal)
- 8 bytes internal trailer (sequence number + key kind) -- mandatory in RocksDB SSTable format
- ~3 bytes varint framing (shared prefix len, unshared key len, value len)
- Total: **~15 bytes/entry** in the uncompressed stream

**SSTable configuration used for benchmarking:**
- Block size set to N x avgEventSize, matching the batch's uncompressed block size exactly
- `block_size_deviation = 100` (fill blocks completely, don't flush early)
- `block_restart_interval = 128` (one restart point per block, matching main benchmarks)
- These settings minimize SST overhead; defaults are more conservative

### Fixed-N Batch (Packfile/Eventstore)

Each batch groups exactly N consecutive events. The batch payload is
zstd-compressed as a single unit.

**Batch payload layout** (uncompressed, before zstd):

```
[event_0] [event_1] ... [event_{N-1}] [FOR-N intra-batch index]
```

The intra-batch index is a Frame-of-Reference (FOR) encoded array of N event sizes
(byte lengths), appended as a trailer within the compressed block. The on-disk byte
order within each eventstore block is:

```
[event data] [min_size: 4B LE] [packed residuals: ceil(W*N/8)] [W: 1B]
```

where W = bit width of residuals = `bits.Len32(max_size - min_size)`.

No separate CRC or integrity header -- the count N and group size are implicit
(fixed at write time), and zstd provides its own content checksum.

To locate event `i` within a decompressed batch:
1. Read W from the last byte, compute index size: `4 + ceil(W*N/8) + 1`
2. Decode the first `i` sizes via prefix sum to get the byte offset
3. Event `i` starts at `offset[i]`

**File structure:**

```
[zstd(batch_0)] [zstd(batch_1)] ...
[inter-batch index]                  <- FOR-128 encoded record sizes
[metadata: 12 bytes]                 <- event count, block size, flags
[trailer: 32 bytes]                  <- magic, version, checksums
```

The inter-batch index is a FOR-128 encoded array of compressed record sizes,
enabling O(1) positional lookup via prefix-sum decode.

**Per-entry overhead:**
- ~1 byte/event in the intra-batch FOR-N index (5 byte header + ~W bits/event packed residuals, amortized across N events)
- Inter-batch index: negligible (FOR-128 encoded record sizes for ~68K records)
- Total: **~1 byte/entry** in the uncompressed stream

## Benchmark Methodology

### Dataset

- Source: Stellar Soroban contract events from ledger range 006016
- 8,741,671 events across 10,000 ledgers
- Total raw size: 1,888,365 KB (1.84 GB)
- Average event size: 221 bytes

### Procedure

For each batch size N in {32, 64, 128, 256, 512}:

1. **Batch approach**: Write an eventstore via `eventstore.Create()` with `ItemsPerRecord=N`.
   Read back the packfile trailer to extract the inter-batch index size. Compute
   intra-batch FOR-N overhead by computing the FOR-encoded size of each block's
   event sizes. Total file size from `os.Stat()`.

2. **RocksDB SSTable approach**: Write all events as KV entries (4-byte ordinal key ->
   event value) into an SST file via grocksdb's `SSTFileWriter`. The SST block size is
   set to N x avgEventSize (same uncompressed block size as the batch). Configuration:
   `block_size_deviation=100`, `block_restart_interval=128`. Ingest into a temporary
   DB and extract properties via `db.GetProperty("rocksdb.aggregated-table-properties")`.

All numbers are produced by `TestBatchVsSSTAnalysis` in `sst_analysis_test.go`.

## Results

### Batch vs SSTable at equal uncompressed block sizes

Both formats use the same uncompressed block size (N x 221 bytes) for each row.

```
Method                        Compressed    InterIdx       Total    Ratio
----------------------------  ----------  ----------  ----------  -------
Batch N=32    (~6.9KB)         488824 KB     383 KB   489207 KB   3.86x
SST   bs=6.9KB                521227 KB       - KB   521227 KB   3.62x

Batch N=64    (~13.8KB)        447606 KB     205 KB   447812 KB   4.22x
SST   bs=13.8KB               473174 KB       - KB   473174 KB   3.99x

Batch N=128   (~27.6KB)        422897 KB     111 KB   423008 KB   4.46x
SST   bs=27.6KB               447291 KB       - KB   447291 KB   4.22x

Batch N=256   (~55.2KB)        406264 KB      60 KB   406324 KB   4.65x
SST   bs=55.2KB               429552 KB       - KB   429552 KB   4.40x

Batch N=512   (~110.5KB)       393365 KB      31 KB   393396 KB   4.80x
SST   bs=110.5KB              415927 KB       - KB   415927 KB   4.54x
```

At N=256, the delta is **23,228 KB (~5.7%)** -- consistent across all block sizes.

### SST file layout breakdown (N=128, block size = 27.6KB)

```
File size:        447,291 KB  (100.0%)
Data blocks:      446,455 KB  ( 99.8%)  [72,088 blocks]
Index blocks:       1,156 KB  (  0.3%)
Raw key size:     102,442 KB  (12.0 B/entry)
Raw value size: 1,888,365 KB  (221.2 B/entry)
```

**Per-entry key overhead:**
- User key bytes:          34,147 KB  (4.0 B/entry)
- Internal trailer bytes:  68,294 KB  (8.0 B/entry)
- Total key bytes:        102,442 KB  (12.0 B/entry)
- Key % of raw input:     5.4%

## Analysis

### The compression ratios on raw input are identical

The key insight is that zstd compresses both formats at essentially the same ratio
when measured against their respective raw inputs:

| Layout | Raw input | File size | Ratio on raw input |
|--------|-----------|-----------|---------------------|
| **Batch** (N=256) | 1,897,024 KB (events + 8,659 KB FOR-N index) | 406,324 KB | **4.67x** |
| **SSTable** (bs=55.2KB) | 1,990,806 KB (events + 102,442 KB keys) | 429,552 KB | **4.63x** |

Both achieve ~4.65x compression on their total raw input. The ~6% file size difference
comes entirely from the different amounts of metadata each format feeds into the
compressor.

### The gap is explained by per-entry metadata cost

The SSTable format requires 12 bytes of per-entry key metadata (4B user key + 8B
internal trailer), while the batch format requires only ~1 byte of per-entry
index metadata (FOR-N encoded event sizes).

```
Extra raw metadata in SSTable:
  102,442 KB (SST keys) - 8,659 KB (FOR-N index) = 93,783 KB

Compressed at ~4.65x:
  93,783 / 4.65 = 20,168 KB

Observed file size difference:
  429,552 - 406,324 = 23,228 KB
```

The predicted gap (20,168 KB) accounts for ~87% of the observed gap (23,228 KB).
The remaining ~3,060 KB comes from SSTable's per-block restart points and 5-byte
block trailers (compression type + CRC32c checksum), which scale with block count
rather than entry count.

### Where does RocksDB SSTable's 12 B/entry come from?

| Component | Size | Purpose | Eliminable? |
|-----------|------|---------|-------------|
| User key | 4 B | Event ordinal for point/range lookups | Could use 0B with ordinal-only access, but loses key-based lookups |
| Internal trailer | 8 B | Sequence number (7B) + key kind (1B) | **No** -- mandatory in RocksDB's SSTable format; enables MVCC, compaction, snapshots |

The 8-byte internal trailer is a structural requirement of the RocksDB SSTable format.
Even with zero-length user keys, every entry would still carry 8 bytes of metadata
that the batch format doesn't need.

**RocksDB format_version compatibility note:** The internal key format (8-byte trailer:
7B sequence + 1B kind), varint key/value framing, and prefix compression within data
blocks are identical across all format versions (2 through 6). The changes in later
versions affect only non-data-block structures:

| format_version | RocksDB version | Change | Affects data block entries? |
|---|---|---|---|
| 2 | 5.13 | Baseline | -- |
| 3 | 5.15 | Strips sequence numbers from **index block** keys | No |
| 4 | 5.16 | Delta-encodes **index block** values (block handles) | No |
| 5 | 6.6 | New **Bloom filter** implementation | No |
| 6 | recent | Context-aware **checksums**, index in metaindex | No |

The ~5% overhead finding applies to current RocksDB (10.9.1) -- confirmed directly
by these benchmarks using grocksdb's SSTFileWriter.

### Where does the batch format's ~1 B/entry come from?

For N=256 events with event sizes in the range ~100-400 bytes:

```
FOR-N index per batch:
  W  = bits.Len32(max_size - min_size) ~ 8 bits (sizes vary by ~256 bytes)
  Header:  4B (min) + ceil(8 x 256 / 8) (packed) + 1B (width) = 261 bytes
  Total:   261 bytes per batch = 1.02 B/event
```

This compresses well alongside the event data since zstd sees it as part of the
same byte stream.

### Are the two layouts isomorphic?

**Structurally, yes.** Both follow the same pattern:

```
events -> group into blocks -> compress each block -> two-level index
```

The two-level index structure is equivalent:
- **Intra-block**: SSTable uses KV prefix compression + restart points;
  batch uses FOR-N bit-packed event sizes
- **Inter-block**: SSTable uses index blocks (B-tree-like);
  batch uses a FOR-128 encoded record size array (O(1) positional lookup via prefix sum)

Both inter-block indexes are negligible (<0.3% of total file size).

The only non-equivalent piece is the intra-block random access mechanism:

| | RocksDB SSTable | Batch (Packfile) |
|---|---|---|
| Mechanism | Binary search on keys via restart points | Prefix-sum decode of FOR-N size array |
| Raw cost | ~15 B/event | ~1 B/event |

The measured file size delta (23,228 KB at N=256) divided by 8.7M events gives **~2.7 B/event**
of compressed overhead — the net cost of the SSTable's extra per-entry metadata after
zstd compression.

### Is the 5% gap a tuning issue or structural?

**Structural.** The benchmark uses `block_size_deviation=100` (fill blocks completely)
and `block_restart_interval=128` (one restart point per block, matching the main
benchmarks). These are close to the most favorable SST settings; defaults would
increase overhead further.

The batch-vs-SST delta stays consistent at ~5.7% across the full range of block sizes
tested (6.9KB to 110.5KB), confirming the gap scales with entry count, not block size.

The 8-byte internal trailer per entry is baked into the RocksDB SSTable format and
has not changed across any `format_version` (2 through 6). It cannot be configured
away.

### Confirmed by our RocksDB benchmark

Our actual benchmark file sizes (from BENCHMARKS.md) using RocksDB 10.9.1 via
grocksdb with block size 27.6KB and zstd compression:

| Format | Size | Ratio |
|--------|------|-------|
| Packfile (eventstore) | 413 MB | 4.6x |
| RocksDB (full DB) | 437 MB | 4.3x |

The RocksDB DB is 5.8% larger than the packfile. Of this, ~5.7% is the structural
per-entry overhead predicted by this analysis. The remaining fraction is DB-level
overhead (WAL, MANIFEST, CURRENT files) not present in a standalone SST.

## Conclusion

The fixed-N batch format and RocksDB SSTable are functionally equivalent layouts for an
append-only, ordinal-access event store. Both group events into compressed blocks
with a two-level index structure, and both achieve the same ~4.65x zstd compression
ratio on their raw input.

The batch format produces ~5.7% smaller files because it replaces RocksDB SSTable's
12 B/entry KV metadata (keys + internal trailers) with a ~1 B/entry FOR-N size index.
This 11 B/entry raw difference compresses proportionally, yielding the observed ~23 MB
gap on 8.7M events.

This gap is a fixed structural property of the RocksDB SSTable format -- it applies to
any RocksDB version, not just the specific version benchmarked. The per-entry metadata
exists to support LSM-tree features (compaction, MVCC snapshots, range deletions) that
an append-only event store does not use. The batch format trades away those capabilities
for a more compact representation.

For this specific workload -- append-only writes with ordinal-keyed random access --
the two approaches are near-equivalent in compression, with the batch format offering
a modest space advantage (~5.7%) at the cost of giving up the RocksDB ecosystem (bloom
filters, compaction, mature tooling). The more significant differences are in read/write
performance (see BENCHMARKS.md), where the packfile's simpler format delivers 3-6x
faster reads and 2.7x faster writes.
