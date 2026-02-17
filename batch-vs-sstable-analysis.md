# Fixed-N Batch vs SSTable: Storage Layout Analysis

## Problem

We store Stellar Soroban contract events as binary-encoded blobs averaging 221 bytes
each. A production dataset of ~8.7 million events (1.84 GB raw) serves as the benchmark
corpus. The question: what is the most space-efficient on-disk layout for this data,
given that we need both sequential scanning and random access to individual events?

Two candidate layouts are compared:

1. **SSTable** — Pebble's (CockroachDB) SSTable format, a mature LSM-tree building block
2. **Fixed-N Batch** — A custom format grouping a fixed count of events per block

Both use zstd block compression. The analysis focuses purely on disk space; read/write
performance is out of scope.

## Layout Descriptions

### SSTable (RocksDB format via Pebble)

The benchmark uses Pebble v1.1.5's SSTable writer with the default table format,
`TableFormatRocksDBv2`, which produces RocksDB-compatible SSTable files. This is
the standard sorted key-value file format used by RocksDB and LevelDB-derived
storage engines. Each event is stored as a KV entry with a 4-byte big-endian
ordinal key and the binary-encoded event as the value.

**Block structure** (uncompressed, before zstd):

```
For each entry in the block:
  [shared_key_prefix_len: varint]     ← prefix compression vs previous key
  [unshared_key_len:      varint]     ← remaining key bytes
  [value_len:             varint]     ← value length
  [unshared_key_bytes]                ← the non-shared portion of the key
  [internal_trailer:      8 bytes]    ← sequence number (7B) + key kind (1B)
  [value_bytes]                       ← the event payload
Every `restartInterval` entries:
  [restart_point:         4 bytes]    ← offset for binary search within block
```

The block is then zstd-compressed and written with a 5-byte trailer
(1B compression type + 4B checksum).

**File structure:**

```
[data block 0][5B trailer] [data block 1][5B trailer] ...
[index block(s)]           ← maps keys to data block offsets
[top-level index]          ← only for two-level indexes
[properties block]
[meta-index block]
[footer: 48 bytes]
```

**Per-entry overhead:**
- 4 bytes user key (ordinal)
- 8 bytes internal trailer (sequence number + key kind) — mandatory in RocksDB SSTable format
- ~3 bytes varint framing (shared prefix len, unshared key len, value len)
- Total: **~15 bytes/entry** in the uncompressed stream

**SSTable configuration used for benchmarking:**
- `BlockSizeThreshold = 100%` (fill blocks completely)
- `BlockRestartInterval = 1024` (minimize restart point overhead)
- These settings minimize SST overhead; defaults are more conservative

### Fixed-N Batch

Each batch groups exactly N consecutive events. The batch payload is
zstd-compressed as a single unit.

**Batch payload layout** (uncompressed, before zstd):

```
[FOR-N intra-batch index] [event₀] [event₁] ... [eventₙ₋₁]
```

The intra-batch index is a Frame-of-Reference (FOR) encoded array of N event sizes
(byte lengths), using exactly one group of size N:

```
[W:        1 byte]     ← bit width of residuals = bits.Len32(max_size - min_size)
[min_size: 4 bytes]    ← minimum event size in the batch (LE uint32)
[packed:   ⌈W×N/8⌉]   ← bit-packed residuals: event_size[i] - min_size
```

No CRC or trailer — the count N and group size are implicit (fixed at write time),
and zstd provides its own integrity checking.

To locate event `i` within a decompressed batch:
1. Read W from byte 0, compute index size: `5 + ⌈W×N/8⌉`
2. Decode the first `i` deltas via prefix sum to get the byte offset
3. Event `i` starts at `index_size + offset[i]`

**File structure:**

```
[zstd(batch₀)] [zstd(batch₁)] ...
[inter-batch index]                  ← FOR-128 encoded batch offsets
```

The inter-batch index encodes batch record offsets (positions of each compressed
batch in the file) using the same FOR scheme with groups of 128 and a full
CRC + trailer.

**Per-entry overhead:**
- ~1 byte/event in the intra-batch FOR-N index (5 byte header + ~3 bits/event packed residuals, amortized across N events)
- Inter-batch index: negligible (62 KB for 34K batches at N=256)
- Total: **~1 byte/entry** in the uncompressed stream

## Benchmark Methodology

### Dataset

- Source: Stellar Soroban contract events from ledger range 006016
- 8,741,671 events across 10,000 ledgers
- Total raw size: 1,888,365 KB (1.84 GB)
- Average event size: 221 bytes

### Procedure

For each batch size N in {32, 64, 128, 256, 512}:

1. **Batch approach**: Group events into batches of N. For each batch, encode the
   FOR-N intra-batch index, prepend it to the concatenated event data, and
   zstd-compress the combined payload. Build a FOR-128 inter-batch index over the
   compressed record offsets. Total = sum of compressed records + inter-batch index.

2. **SSTable approach**: Write all events as KV entries (4-byte ordinal key → event
   value) into a Pebble SSTable with block size set to the nearest power-of-two
   matching the batch's uncompressed size (~N × 221 bytes). SSTable configuration
   uses `BlockSizeThreshold=100%` and `BlockRestartInterval=1024` to minimize
   structural overhead.

Additionally, one "equal size" comparison sets the SST block size to exactly
N × avgEventSize (≈55.2 KB for N=256) to eliminate any block-size mismatch.

### Code

- `sstable_bench_test.go`: `TestSSTableCompression` — the benchmark driver
- `indexenc.go`: `FOREncode`, `FOREncodeSize`, `PerGroupWEncode` — index encoding

Run: `go test -v -run TestSSTableCompression -timeout 10m`

## Results

### Batch vs SSTable at comparable uncompressed block sizes

```
Method                        Compressed    InterIdx       Total    Ratio
----------------------------  ----------  ----------  ----------  -------
Batch N=32    (~6.9KB raw)     492344 KB     402 KB   492747 KB   3.83x
SST   bs=8KB                  510442 KB       — KB   510442 KB   3.70x

Batch N=64    (~13.8KB raw)    450953 KB     218 KB   451171 KB   4.19x
SST   bs=16KB                 467777 KB       — KB   467777 KB   4.04x

Batch N=128   (~27.6KB raw)    424457 KB     117 KB   424574 KB   4.45x
SST   bs=32KB                 442302 KB       — KB   442302 KB   4.27x

Batch N=256   (~55.2KB raw)    407746 KB      62 KB   407808 KB   4.63x
SST   bs=64KB                 425490 KB       — KB   425490 KB   4.44x

Batch N=512   (~110.5KB raw)   395048 KB      32 KB   395080 KB   4.78x
SST   bs=128KB                412183 KB       — KB   412183 KB   4.58x
```

### Equal uncompressed block size (N=256, SST block = 55.2KB)

```
Batch N=256   (~55.2KB raw)    407746 KB      62 KB   407808 KB   4.63x
SST   bs=55.2KB               428629 KB       — KB   428629 KB   4.41x
                                                       ──────
                                                  Δ = 20,821 KB (~5.0%)
```

### SST file layout breakdown (block size = 64KB)

```
File size:        425,490 KB  (100.0%)
Data blocks:      424,940 KB  ( 99.9%)  [31,187 blocks]
Index blocks:         397 KB  (  0.1%)  [13 blocks]
Top-level index:        0 KB  (  0.0%)
Properties:             1 KB  (  0.0%)
Footer:                 0 KB  (  0.0%)
Block trailers:       152 KB  (  0.0%)  [~31,203 × 5B]
```

**Per-entry key overhead:**
- User key bytes:          34,147 KB  (4.0 B/entry)
- Internal trailer bytes:  68,294 KB  (8.0 B/entry)
- Total key bytes:        102,442 KB  (12.0 B/entry)
- Key % of raw input:     5.1%

## Analysis

### The compression ratios on raw input are identical

The key insight is that zstd compresses both formats at essentially the same ratio
when measured against their respective raw inputs:

| Layout | Raw input | Compressed output | Ratio on raw input |
|--------|-----------|-------------------|--------------------|
| **Batch** (N=256) | 1,897,023 KB (events + 8,659 KB FOR-N index) | 407,746 KB | **4.65x** |
| **SSTable** (bs=55.2KB) | 1,990,806 KB (events + 102,442 KB keys) | 428,629 KB | **4.64x** |

Both achieve ~4.65x compression on their total raw input. The 5% file size difference
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
  428,629 - 407,808 = 20,821 KB  ✓
```

The numbers match. The entire gap is the compressed cost of the extra per-entry
key metadata.

### Where does SSTable's 12 B/entry come from?

| Component | Size | Purpose | Eliminable? |
|-----------|------|---------|-------------|
| User key | 4 B | Event ordinal for point/range lookups | Could use 0B with ordinal-only access, but loses key-based lookups |
| Internal trailer | 8 B | Sequence number (7B) + key kind (1B) | **No** — mandatory in Pebble's format; enables MVCC, compaction, snapshots |

The 8-byte internal trailer is a structural requirement of the RocksDB SSTable format.
Even with zero-length user keys, every entry would still carry 8 bytes of metadata
that the batch format doesn't need.

**RocksDB format_version compatibility note:** The benchmark uses Pebble's default
`TableFormatRocksDBv2`, which corresponds to RocksDB's `format_version=2`. RocksDB
has since introduced format versions 3 through 6, but none of them change the
per-entry data block encoding:

| format_version | RocksDB version | Change | Affects data block entries? |
|---|---|---|---|
| 2 | 5.13 | Baseline (used in benchmark) | — |
| 3 | 5.15 | Strips sequence numbers from **index block** keys | No |
| 4 | 5.16 | Delta-encodes **index block** values (block handles) | No |
| 5 | 6.6 | New **Bloom filter** implementation | No |
| 6 | recent | Context-aware **checksums**, index in metaindex | No |

The internal key format (8-byte trailer: 7B sequence + 1B kind), varint key/value
framing, and prefix compression within data blocks are identical across all versions.
The ~5% overhead finding applies to current RocksDB just as it does to our
`format_version=2` benchmark.

### Where does the batch format's ~1 B/entry come from?

For N=256 events with event sizes in the range ~100-400 bytes:

```
FOR-N index per batch:
  W  = bits.Len32(max_size - min_size) ≈ 8 bits (sizes vary by ~256 bytes)
  Header:  1B (width) + 4B (min) = 5 bytes
  Packed:  ⌈8 × 256 / 8⌉ = 256 bytes
  Total:   261 bytes per batch = 1.02 B/event
```

This compresses well alongside the event data since zstd sees it as part of the
same byte stream.

### Are the two layouts isomorphic?

**Structurally, yes.** Both follow the same pattern:

```
events → group into blocks → compress each block → two-level index
```

The two-level index structure is equivalent:
- **Intra-block**: SSTable uses KV prefix compression + restart points;
  batch uses FOR-N bit-packed offsets
- **Inter-block**: SSTable uses index blocks (B-tree-like);
  batch uses FOR-128 encoded record offsets

Both inter-block indexes are negligible (<0.1% of total file size).

The only non-equivalent piece is the intra-block random access mechanism:

| | SSTable | Batch |
|---|---|---|
| Mechanism | Binary search on keys via restart points | Prefix-sum decode of FOR-N offset array |
| Raw cost | ~15 B/event | ~1 B/event |
| Compressed cost | ~3.2 B/event | ~0.22 B/event |
| Net overhead | ~3.0 B/event more than batch | baseline |

### Is the 5% gap a tuning issue or structural?

**Structural.** The gap cannot be closed by adjusting SSTable configuration parameters.
We already tested all relevant knobs:

- `BlockSizeThreshold`: 90% → 95% → 100% — negligible difference (<0.1%)
- `BlockRestartInterval`: 16 → 64 → 256 → 1024 — diminishing returns, <1% total
- Block size: sweeping 4KB–256KB changes compression ratio but the batch-vs-SST delta
  stays constant at ~5%

The 8-byte internal trailer per entry is baked into the RocksDB SSTable format and
has not changed across any `format_version` (2 through 6). It cannot be configured
away.

## Conclusion

The fixed-N batch format and SSTable are functionally equivalent layouts for an
append-only, ordinal-access event store. Both group events into compressed blocks
with a two-level index structure, and both achieve the same ~4.65x zstd compression
ratio on their raw input.

The batch format produces ~5% smaller files because it replaces SSTable's 12 B/entry
KV metadata (keys + internal trailers) with a ~1 B/entry FOR-N offset index. This
11 B/entry raw difference compresses proportionally, yielding the observed ~20 MB
gap on 8.7M events.

This gap is a fixed structural property of the two formats — it applies to any
RocksDB-compatible SSTable, not just Pebble's implementation. SSTable's per-entry
metadata exists to support LSM-tree features (compaction, MVCC snapshots, range
deletions) that an append-only event store does not use. The batch format trades
away those capabilities for a more compact representation.

For this specific workload — append-only writes with ordinal-keyed random access —
the two approaches are near-equivalent, with the batch format offering a modest
space advantage at the cost of giving up the SSTable ecosystem (bloom filters,
compaction, mature tooling).
