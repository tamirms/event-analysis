# Benchmark Results (ARM64)

Machine: AWS Graviton2 (Neoverse-N1), 32 cores, 123GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), system libzstd 1.5.7
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

Both formats use **zstd level 3** compression with ~27.6KB block size (128 events). Compression uses a thin CGO wrapper linking system libzstd (see `zstd/` package doc for rationale).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB is a general-purpose sorted key-value format with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for either format (zero memory overhead beyond each format's intrinsic index).

**RocksDB optimizations applied** (encapsulated in `rocksdbutil`, `eventstore/rocksdb`, and `bitmapindex/rocksdb` packages)**:** FormatVersion(5) (shortest index keys, compact block handles), BlockSizeDeviation(100) (fill blocks to target size), SetMoveFiles(true) (atomic rename on ingest), BlockRestartInterval(128) (one restart point per block, minimizing per-entry overhead for sequential ordinal keys), explicit zstd level 3 via SetCompressionOptions, skip-stats-on-open, disabled auto-compactions, single file-opening thread. Read-side: `SetVerifyChecksums(false)`, `SetFillCache(false)`, `Iterator.ValueSlice()` (zero-alloc per-item access). Scattered reads use `BatchedMultiGetCF` with `SetAsyncIO(true)` split across N goroutines. All benchmarks use the `StoreReader`/`IndexReader` interfaces — packfile and RocksDB go through the same generic benchmark functions.

**Note on CGO overhead:** Both formats use CGO — packfile for zstd compress/decompress calls, RocksDB for all API access via grocksdb. The key difference is **API granularity**: packfile makes ~68K CGO calls for a full sequential scan (one decompress per block of 128 events, then pure Go iteration), while RocksDB makes ~26M calls (Valid + Next + ValueSlice per item). Each CGO crossing costs ~50-100ns, so this 382x difference in crossing count is significant for iterator-heavy access patterns. For point reads (single `Get` call), CGO overhead is negligible for both.

**Methodology:** Read benchmarks use `benchstat` with 5 samples per benchmark, each in a separate process. Write benchmarks use `TestWriteThroughput` (single run with internal median). Memory benchmarks are single-run with `GODEBUG=madvdontneed=1` and `GOGC=1`. See CLAUDE.md for instructions on running benchmarks.

## Zstd Compression

Comparison of zstd implementations on 68,295 blocks (avg 28,314 bytes each, ~1.9GB total raw data). All at level 3. The CGO wrapper links system libzstd 1.5.7; klauspost/compress is a pure-Go implementation.

| Benchmark | CGO (MB/s) | klauspost (MB/s) | Ratio |
|-----------|-----------|-----------------|-------|
| Compress (serial) | 345 | 83 | 4.1x |
| Compress (8 goroutines) | 2,700 | 642 | 4.2x |
| Decompress (serial) | 1,455 | 925 | 1.6x |
| Decompress (8 goroutines) | 11,237 | 7,191 | 1.6x |

Compression ratio: CGO 4.51x, klauspost 4.49x (negligible difference).

The 4x compression speed advantage comes from libzstd's hand-optimized NEON (ARM64) inner loops vs klauspost's generic Go implementation. The decompression gap is smaller (1.6x) because decompression is less compute-intensive. See the `zstd/` package doc for why we wrote a CGO wrapper instead of using existing Go bindings.

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 226 | 8.56 | 219 | 8.81 |
| PackfileWrite (4 goroutines) | 831 | 2.33 | 1,211 | 1.60 |
| **PackfileWrite (8 goroutines)** | **830** | **2.33** | **2,118** | **0.91** |
| PackfileWrite (16 goroutines) | 830 | 2.33 | 2,805 | 0.69 |
| PackfileWrite (24 goroutines) | 830 | 2.33 | 3,081 | 0.63 |
| PackfileWrite (32 goroutines) | 830 | 2.33 | 3,043 | 0.64 |
| RocksDBWrite | 190 | 10.17 | 200 | 9.69 |
| RocksDBWrite (4 threads) | 405 | 4.78 | 472 | 4.09 |
| RocksDBWrite (8 threads) | 442 | 4.38 | 509 | 3.80 |

Notes:
- **Packfile with 8 goroutines on NVMe (2,118 MB/s) is 4.2x faster than RocksDB's best (509 MB/s).** Scales to 3,081 MB/s at c=24.
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- RocksDB uses `SSTFileWriter` with `SetMoveFiles(true)` for atomic rename on ingest (avoids file copy overhead).
- **Serial writes are CPU-bound** (zstd compression dominates). With 4.6x compression, the actual disk write rate for serial packfile is only ~48 MB/s — trivial for both EBS and NVMe. Serial packfile is similarly fast on EBS and NVMe (226 vs 219 MB/s) because compression dominates and `sync_file_range` keeps writeback current at this pace.
- **EBS**: Packfile plateaus at ~830 MB/s (c=4+), faster than RocksDB's ~442 MB/s (t=8). Both are crash-safe: packfile fsyncs the data file + directory; RocksDB's `IngestExternalFile` fsyncs the SST file after linking it into the DB directory (via `SyncIngestedFile`). Packfile uses `sync_file_range(SYNC_FILE_RANGE_WRITE)` every 1MB during the append phase to initiate background writeback of dirty pages to EBS. **Per-phase timing:** the append phase takes ~2.27s (throttled by EBS bandwidth as writeback overlaps with compression), and `fdatasync()` finishes in ~62ms (total: 2.33s = 830 MB/s). Without `sync_file_range`, compression would complete faster but `fdatasync()` would need to flush all 414MB of dirty pages at once, adding ~2s of pure I/O wait. RocksDB's C-side SST builder naturally matches EBS writeback pace (4.21s append, 70ms finish). EBS c=4-32 numbers are measured from isolated phase tests (c=8) and validated by TestWriteThroughput first-concurrent-level (c=4=831 MB/s); same-process sequential runs on EBS show contamination from kernel dirty page throttling across iterations.
- **NVMe**: Packfile scales to ~3.1 GB/s at c=24. The `sync_file_range` optimization reduces the finish phase from an estimated ~80ms fdatasync to 3ms, which is significant when total time is <1s. The serial main goroutine (block building) becomes the bottleneck at c=16+.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 28 MB |
| PackfileWrite (8 goroutines) | 52 MB (+24 MB over serial) |
| RocksDBWrite (8 threads) | 31 MB |

The 28 MB packfile baseline includes the zstd C context and scratch buffer. Streaming compression adds ~24 MB for in-flight blocks across compress workers. RocksDB is slightly higher at 31 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process with `GODEBUG=madvdontneed=1`.

### Read Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileSeqRead (full 8.7M events) | 3.2 MB |
| PackfileReadIndices (1,000 scattered, c=8) | 1.9 MB |

Read memory is minimal — just pooled `blockBuf` decoders from `sync.Pool`, scaling linearly with concurrency.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 1,294 | 1,494M | 1.6MB / 28 |
| RocksDBSeqRead | 295 | 6,554M | 96B / 6 |

Packfile is 4.4x faster than RocksDB for sequential reads. The gap comes from two factors: (1) RocksDB's per-item iterator API requires ~26M CGO crossings (Valid + Next + ValueSlice per event) vs packfile's ~68K (one decompress per block, then pure Go iteration), and (2) RocksDB's prefix-compressed KV entry decoding is inherently more work per item than packfile's flat offset-array format. Both formats use the `StoreReader` interface via `ReadEvents`, adding `iter.Seq2` yield overhead per item. The no-compression benchmarks below show the gap widens to 11.7x without zstd, confirming the format overhead is the dominant factor.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 23,900 | 276B / 2 |
| RocksDBRandomRead | 31,700 | 289B / 7 |

Packfile is 1.33x faster. Both formats go through the `StoreReader` interface (`ReadEvent`), which copies the value into an owned slice. RocksDB's allocs are higher (289B/7 vs 276B/2) due to `GetPinned` + copy vs packfile's direct buffer extraction.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 911 | 230B / 1 |
| RocksDBParallelRead | 1,047 | 265B / 6 |

Packfile is 1.15x faster under parallel load. The gap comes from RocksDB's higher per-read allocation overhead through the `StoreReader` interface being amplified across 32 cores.

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 23,920 | 125B / 3 |
| RocksDBReadBatch128 | 129,634 | 96B / 6 |

Packfile is 5.4x faster than RocksDB for batch reads from a known offset. Both use `ReadEvents` via the `StoreReader` interface. Packfile decompresses one block (single CGO call) to yield all 128 events in a flat buffer. RocksDB iterates with 384 CGO crossings (3 per event x 128) plus per-entry prefix-compressed key decoding.

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 46,135 | 80B / 3 |
| RocksDBRangeScan128 | 133,902 | 96B / 6 |

Packfile is 2.9x faster than RocksDB. Both use `ReadEvents` via the `StoreReader` interface, creating a new iterator per call. Compared to batch-from-0 (23.9us), the random seek adds ~22us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 250,000 | 31KB / 80 |
| RocksDBReadIndices | 308,600 | 20.5KB / 271 |

Both use 8 internal goroutines for parallel I/O via the `StoreReader` interface. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input, `SetAsyncIO(true)`, and `SetFillCache(false)`. RocksDB's higher alloc count (271 vs 80) comes from copying each value into an owned `[]byte` slice (packfile returns slices from decompressed block buffers). Packfile is 1.23x faster.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 54,700 | 42KB / 81 |
| RocksDBParallelReadIndices | 60,100 | 20.5KB / 271 |

Under parallel scattered load packfile is 1.10x faster.

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

Both formats use the `StoreReader` interface (`ReadIndices` with `WithConcurrency(N)`). RocksDB internally uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and splits keys across N goroutines each calling `BatchedMultiGetCF` with sorted input, `SetAsyncIO(true)`, no checksum verification.

| Benchmark | Goroutines | NVMe (ms) | EBS (ms) |
|-----------|-----------|-----------|----------|
| Packfile | 1 | 171 | 617 |
| Packfile | 4 | 46 | 331 |
| Packfile | 8 | 25 | 330 |
| Packfile | 16 | 15 | 331 |
| Packfile | 32 | 10 | 331 |
| Packfile | 64 | 8.2 | 330 |
| Packfile | 128 | 7.4 | 330 |
| RocksDB | 1 | 183 | 695 |
| RocksDB | 8 | 31 | 331 |
| RocksDB | 32 | 16 | 331 |
| RocksDB | 64 | 15 | 331 |

Notes:
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (8.2ms) is 1.8x faster than RocksDB at c=64 (15ms). The gap is narrower than before (was 4.2x) because RocksDB's `BatchedMultiGetCF` with `SetAsyncIO(true)` dramatically improved cold read performance (c=64: was 35ms, now 15ms).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=4+, both formats converge to ~330ms (the 3,000 IOPS floor for 1,000 random reads). Packfile c=1 (617ms) vs RocksDB c=1 (695ms) shows packfile's faster open + lookup overhead (1.13x).
- Packfile c=128 on NVMe (7.4ms) shows diminishing returns — approaching NVMe's random I/O floor for 1,000 reads.

### Improving EBS Cold Cache Latency

The EBS bottleneck is **IOPS, not bandwidth**. 1,000 scattered reads x ~6.4KB per block = ~6.4MB total data (trivial bandwidth), but 1,000 random I/Os at 3,000 IOPS = ~333ms floor.

| Option | IOPS | Expected latency (c=32) | $/month (us-east-1) |
|--------|------|------------------------|---------------------|
| gp3 baseline | 3,000 | 333 ms | included |
| gp3 + provisioned IOPS | 16,000 | ~62 ms | +$65 (13K x $0.005) |
| io2 | 40,000 | ~25 ms | ~$2,725 (40K x $0.065 + 1TB storage) |
| io2 Block Express | 160,000 | ~6 ms | ~$9,520 |
| NVMe instance storage (i4i.xlarge) | 40,000 | 6.7 ms | ~$250 (instance cost) |

Provisioning gp3 to 16,000 IOPS ($65/month) is the best value — ~5x latency improvement. Beyond that, NVMe instance storage (i4i family) delivers io2-level IOPS at ~11x lower cost, but storage is ephemeral (lost on instance stop). io2 is only justified when you need both high IOPS and persistence without a replication strategy.

## Raw I/O (No Compression)

Compression disabled for both formats to isolate format overhead, block-building cost, and I/O patterns.

### Write

| Benchmark | NVMe (MB/s) | EBS (MB/s) |
|-----------|------------|-----------|
| Packfile (no zstd) | 1,018 | 130 |
| RocksDB (no zstd) | 427 | 132 |

Packfile raw I/O is 2.4x faster than RocksDB on NVMe. Without compression, both formats are **I/O-bound** — writing uncompressed data (1.9GB) dominates. RocksDB's gap on NVMe is SST block construction + key encoding + ingest overhead. On EBS both are equal (both hit EBS bandwidth limit). Packfile's `sync_file_range` eliminates the fdatasync penalty for the 1.9GB uncompressed output on NVMe.

Without compression, both formats are **slower** than with-compression at high concurrency (NVMe: packfile 1,018 vs 3,081 MB/s, RocksDB 427 vs 509 MB/s) — writing 4.6x more data to disk dominates.

### Read (warm page cache)

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 4,217 MB/s | 360 MB/s | 1,294 MB/s | 295 MB/s |
| Point read | 7.1 us | 16.1 us | 23.9 us | 31.7 us |
| Scattered 50 | 138 us | 230 us | 250 us | 309 us |
| Range scan 128 | 13 us | 102 us | 46.1 us | 134 us |

Notes:
- **Sequential reads**: Packfile 11.7x faster than RocksDB without compression (vs 4.4x with). Without compression, the gap reveals the raw format + API overhead: packfile iterates a flat buffer in pure Go (~68K blocks), while RocksDB decodes prefix-compressed entries via ~26M CGO crossings. With compression, both spend time decompressing (packfile via CGO, RocksDB internally in C++), which narrows the ratio.
- **Point reads**: Packfile is 2.3x faster without compression (7.1 vs 16.1 us). With compression, 1.33x (23.9 vs 31.7 us). Decompression cost equalizes the two — packfile's Go-side decompress + extract costs slightly more per byte than RocksDB's single C++ Get().
- **Scattered**: With compression, packfile is 1.23x faster (250 vs 309 us). Without compression, packfile is 1.7x faster (138 vs 230 us).
- **Range scan**: Packfile stays 7.8x faster without compression (13 vs 102 us). With compression, 2.9x faster (46.1 vs 134 us).

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 575,000 | 927KB / 14 |
| RocksDBOpen | 3,523,000 | 432B / 7 |

Packfile opens in 575us (reads index into memory). RocksDB opens in 3.5ms (with skip-stats optimizations). These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 414MB |
| RocksDB (zstd, bs=27.6KB) | 437MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.6x, RocksDB 4.3x.

## Bitmap Index

Bitmap indexes map (field, key) pairs to roaring bitmaps of event indices. Two implementations compared:

- **MPHF+packfile**: Minimal perfect hash function (BBHash) maps keys to ordinals → packfile stores bitmaps at positional offsets. O(1) lookup, mmap-based, no block cache. `LookupKeys` batch API resolves multiple keys with reduced I/O (sorts by offset, coalesces nearby reads).
- **RocksDB**: Standard key-value store with `field_byte || key` schema. Same tuning as eventstore benchmarks (format v5, `BlockRestartInterval=128`, zstd level 3, no block cache, no checksum verification).

Built from 50,000 events, yielding 10,852 unique (field, key) bitmap entries.

### File Sizes

| Format | Size |
|--------|------|
| MPHF hash (178KB) + packfile (53MB) | 53.1MB |
| RocksDB | 58.9MB |

MPHF+packfile is 10% smaller. Build time: MPHF 9.5s vs RocksDB 12.7s.

### Warm Cache — Single Key Lookup

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFLookup | 25,701 | 7,098B / 146 |
| BitmapRocksDBLookup | 42,637 | 6,769B / 152 |

MPHF is **1.7x faster** for single-key lookups. Both allocations are dominated by roaring bitmap deserialization.

### Warm Cache — Parallel Lookup (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFParallel15 | 1,520 | 6,444B / 145 |
| BitmapRocksDBParallel15 | 6,152 | 6,220B / 148 |

MPHF is **4.0x faster** under parallel load. The hash-to-offset lookup scales linearly with cores; RocksDB's block index traversal and CGO crossings create contention.

### Warm Cache — Batch Lookup (15 keys)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFLookupKeys15 | 200,032 | 144,286B / 2,194 |

`LookupKeys` resolves 15 keys in a single call (~13.3µs/key). RocksDB's `LookupKeys` uses `BatchedMultiGetCF` with sorted input and concurrent goroutines — see cold cache benchmarks below for comparison.

### Cold Cache — NVMe (drop page cache, open + lookup + close per iteration)

Each iteration drops file cache via `posix_fadvise FADV_DONTNEED` (MPHF/packfile) or directory walk (RocksDB), then times open + N lookups + close. All formats use the `IndexReader` interface. RocksDB parallel uses `LookupKeys` with `WithConcurrency(N)` which splits keys across goroutines calling `BatchedMultiGetCF`. 5 samples, median of last 4 (first iteration excluded as warmup).

| Lookups | MPHF serial (µs) | MPHF LookupKeys (µs) | RocksDB serial (µs) | RocksDB parallel (µs) |
|---------|------------------|-----------------------|---------------------|-----------------------|
| 1 | 897 | 1,016 | 2,376 | 2,372 |
| 5 | 1,618 | 1,049 | 3,043 | 2,611 |
| 15 | 3,356 | 1,283 | 4,834 | 2,809 |
| 50 | 10,022 | 2,241 | 10,776 | 3,141 |

Notes:
- **MPHF serial is 1.1-2.6x faster than RocksDB serial** across all lookup counts. At 1 lookup, the gap is dominated by open latency: MPHF opens in ~0.9ms (mmap hash + packfile) vs RocksDB ~2.4ms.
- **MPHF LookupKeys is the fastest option at 5+ lookups.** At 50 lookups, LookupKeys (2.2ms) is 4.5x faster than MPHF serial (10.0ms) — the batch API sorts by file offset and coalesces nearby reads, converting 50 random I/Os into fewer sequential ones.
- **RocksDB parallel converges with MPHF LookupKeys at high counts** (50 lookups: 3.1ms vs 2.2ms). Both amortize open cost and parallelize I/O, but LookupKeys has lower overhead (no goroutine spawn, sorted access pattern).
- **At 1 lookup, MPHF serial is fastest** (897µs vs LookupKeys 1,016µs) — the LookupKeys batch setup overhead exceeds the savings for a single key.

### Cold Cache — EBS (gp3, 3,000 IOPS baseline)

Same methodology as NVMe cold cache, but on EBS gp3 volume.

| Lookups | MPHF serial (µs) | MPHF LookupKeys (µs) | RocksDB serial (µs) | RocksDB parallel (µs) |
|---------|------------------|-----------------------|---------------------|-----------------------|
| 1 | 3,532 | 2,957 | 4,838 | 4,571 |
| 5 | 5,878 | 3,753 | 7,642 | 5,371 |
| 15 | 11,239 | 4,689 | 14,160 | 5,592 |
| 50 | 34,583 | 15,052 | 36,513 | 16,907 |

Notes:
- **EBS amplifies the I/O advantage of batch/parallel.** At 50 lookups, MPHF LookupKeys (15ms) is 2.3x faster than MPHF serial (35ms). RocksDB parallel (17ms) is 2.2x faster than serial (37ms).
- **MPHF serial vs RocksDB serial:** MPHF is 1.3-1.4x faster, consistent with NVMe results.
- **At 50 lookups, serial variants converge** (MPHF 34.6ms ≈ RocksDB 36.5ms) — both are IOPS-limited at 50 random reads on gp3.
- **LookupKeys vs RocksDB parallel stay close** (15.1ms vs 16.9ms at 50 lookups) — EBS IOPS ceiling equalizes formats when I/O dominates.
- **EBS latencies are 3-4x higher than NVMe** across the board, consistent with gp3 single-digit-ms access latency vs NVMe's sub-ms.
