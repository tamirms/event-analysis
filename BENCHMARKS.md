# Benchmark Results

Machine: Intel Xeon Platinum 8375C @ 2.90GHz, 32 cores, 61GB RAM
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
| Compress (serial) | 591 | 127 | 4.7x |
| Compress (8 goroutines) | 4,689 | 809 | 5.8x |
| Decompress (serial) | 3,131 | 2,389 | 1.3x |
| Decompress (8 goroutines) | 23,159 | 17,866 | 1.3x |

Compression ratio: CGO 4.56x, klauspost 4.53x (negligible difference).

The 4.7x compression speed advantage comes from libzstd's hand-optimized SSE/AVX inner loops vs klauspost's generic Go implementation. The decompression gap is smaller (1.3x) because decompression is less compute-intensive. See the `zstd/` package doc for why we wrote a CGO wrapper instead of using existing Go bindings.

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 449 | 4.31 | 449 | 4.31 |
| PackfileWrite (4 goroutines) | 831 | 2.33 | 1,752 | 1.10 |
| **PackfileWrite (8 goroutines)** | **821** | **2.30** | **2,805** | **0.69** |
| PackfileWrite (16 goroutines) | 821 | 2.30 | 3,089 | 0.63 |
| PackfileWrite (24 goroutines) | 821 | 2.30 | 3,090 | 0.63 |
| PackfileWrite (32 goroutines) | 821 | 2.30 | 3,066 | 0.63 |
| RocksDBWrite | 356 | 5.44 | 362 | 5.34 |
| RocksDBWrite (4 threads) | 756 | 2.56 | 859 | 2.25 |
| RocksDBWrite (8 threads) | 756 | 2.56 | 926 | 2.09 |

Notes:
- **Packfile with 8 goroutines on NVMe (2,805 MB/s) is 3.0x faster than RocksDB's best (926 MB/s).** Scales to 3,089 MB/s at c=16.
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- RocksDB uses `SSTFileWriter` with `SetMoveFiles(true)` for atomic rename on ingest (avoids file copy overhead).
- **Serial writes are CPU-bound** (zstd compression dominates). With 4.6x compression, the actual disk write rate for serial packfile is only ~96 MB/s — trivial for both EBS and NVMe. Serial packfile is equally fast on EBS and NVMe (449 MB/s both) because compression dominates and `sync_file_range` keeps writeback current at this pace.
- **EBS**: Packfile plateaus at ~821 MB/s (c=4+), faster than RocksDB's ~756 MB/s (t=4-8). Both are crash-safe: packfile fsyncs the data file + directory; RocksDB's `IngestExternalFile` fsyncs the SST file after linking it into the DB directory (via `SyncIngestedFile`). Packfile uses `sync_file_range(SYNC_FILE_RANGE_WRITE)` every 1MB during the append phase to initiate background writeback of dirty pages to EBS. **Per-phase timing:** without `sync_file_range`, packfile's parallel compression completes in ~879ms, accumulating 412MB of dirty pages — then `fdatasync()` must flush all 412MB at once (2,311ms, total: 3.19s = 607 MB/s). With `sync_file_range`, writeback overlaps with compression: the append phase is throttled to ~2.3s by EBS bandwidth, but `fdatasync()` finishes in ~55ms (total: 2.30s = 821 MB/s). RocksDB's C-side SST builder naturally matches EBS writeback pace (2.45s append, 108ms finish); adding `bytes_per_sync` to RocksDB had no measurable effect. EBS c=4-32 numbers are measured from isolated phase tests (c=8) and validated by TestWriteThroughput first-concurrent-level (c=4=831 MB/s); same-process sequential runs on EBS show contamination from kernel dirty page throttling across iterations.
- **NVMe**: Packfile scales to ~3.1 GB/s at c=16-32, up from ~2.5 GB/s before `sync_file_range`. The 156ms fdatasync savings (from ~158ms to ~2ms) is significant when total time is <1s. The serial main goroutine (block building) becomes the bottleneck at c=16+.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 30 MB |
| PackfileWrite (8 goroutines) | 55 MB (+25 MB over serial) |
| RocksDBWrite (8 threads) | 29 MB |

The 30 MB packfile baseline includes the zstd C context and scratch buffer. Streaming compression adds ~25 MB for in-flight blocks across compress workers. RocksDB is slightly lower at 29 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process with `GODEBUG=madvdontneed=1`.

### Read Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileSeqRead (full 8.7M events) | 3.0 MB |
| PackfileReadIndices (1,000 scattered, c=8) | 1.9 MB |

Read memory is minimal — just pooled `blockBuf` decoders from `sync.Pool`, scaling linearly with concurrency.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 2,561 | 755M | 800KB / 15 |
| RocksDBSeqRead | 487 | 3,970M | 96B / 6 |

Packfile is 5.3x faster than RocksDB for sequential reads. The gap comes from two factors: (1) RocksDB's per-item iterator API requires ~26M CGO crossings (Valid + Next + ValueSlice per event) vs packfile's ~68K (one decompress per block, then pure Go iteration), and (2) RocksDB's prefix-compressed KV entry decoding is inherently more work per item than packfile's flat offset-array format. Both formats use the `StoreReader` interface via `ReadEvents`, adding `iter.Seq2` yield overhead per item (~34ns/event, ~8% for RocksDB, ~3% for packfile). The no-compression benchmarks below show the gap widens to 9.1x without zstd, confirming the format overhead is the dominant factor.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 11,300 | 256B / 2 |
| RocksDBRandomRead | 13,400 | 289B / 7 |

Packfile is 1.19x faster. Both formats go through the `StoreReader` interface (`ReadEvent`), which copies the value into an owned slice. RocksDB's allocs are higher (289B/7 vs 256B/2) due to `GetPinned` + copy vs packfile's direct buffer extraction.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 687 | 230B / 1 |
| RocksDBParallelRead | 853 | 265B / 6 |

Packfile is 1.24x faster under parallel load. The gap (vs 1.19x for single-threaded random reads) comes from RocksDB's higher per-read allocation overhead through the `StoreReader` interface being amplified across 32 cores.

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 10,690 | 91B / 3 |
| RocksDBReadBatch128 | 63,300 | 96B / 6 |

Packfile is 5.9x faster than RocksDB for batch reads from a known offset. Both use `ReadEvents` via the `StoreReader` interface. Packfile decompresses one block (single CGO call) to yield all 128 events in a flat buffer. RocksDB iterates with 384 CGO crossings (3 per event x 128) plus per-entry prefix-compressed key decoding.

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 22,800 | 114B / 3 |
| RocksDBRangeScan128 | 72,000 | 96B / 6 |

Packfile is 3.2x faster than RocksDB. Both use `ReadEvents` via the `StoreReader` interface, creating a new iterator per call. Compared to batch-from-0 (10.7us), the random seek adds ~12us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 158,000 | 35KB / 81 |
| RocksDBReadIndices | 206,000 | 20.5KB / 271 |

Both use 8 internal goroutines for parallel I/O via the `StoreReader` interface. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input, `SetAsyncIO(true)`, and `SetFillCache(false)`. RocksDB's higher alloc count (271 vs 81) comes from copying each value into an owned `[]byte` slice (packfile returns slices from decompressed block buffers). Packfile is 1.30x faster.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 42,000 | 41KB / 81 |
| RocksDBParallelReadIndices | 45,000 | 20.5KB / 271 |

Under parallel scattered load packfile is 1.07x faster — effectively tied.

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

Both formats use the `StoreReader` interface (`ReadIndices` with `WithConcurrency(N)`). RocksDB internally uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and splits keys across N goroutines each calling `BatchedMultiGetCF` with sorted input, `SetAsyncIO(true)`, no checksum verification.

| Benchmark | Goroutines | NVMe (ms) | EBS (ms) |
|-----------|-----------|-----------|----------|
| Packfile | 1 | 106 | 772 |
| Packfile | 8 | 15.8 | 269 |
| Packfile | 32 | 6.6 | 344 |
| Packfile | 64 | 5.2 | 332 |
| RocksDB | 1 | 111 | 790 |
| RocksDB | 8 | 19.1 | 257 |
| RocksDB | 32 | 10.0 | 333 |
| RocksDB | 64 | 9.9 | 334 |

Notes:
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (5.2ms) is 1.9x faster than RocksDB at c=64 (9.9ms).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=8, both formats are similar (packfile 269ms, RocksDB 257ms). At c=32+, both converge to ~333ms (the 3,000 IOPS floor).

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
| Packfile (no zstd) | 1,225 | 129 |
| RocksDB (no zstd) | 656 | 124 |

Packfile raw I/O is 1.9x faster than RocksDB on NVMe. Without compression, both formats are **I/O-bound** — writing uncompressed data (1.9GB) dominates. RocksDB's gap on NVMe is SST block construction + key encoding + ingest overhead. On EBS both are equal (both hit EBS bandwidth limit). Packfile's improvement over the pre-`sync_file_range` number (1,002 → 1,225 MB/s on NVMe) comes from eliminating the fdatasync penalty for the 1.9GB uncompressed output.

Without compression, both formats are **slower** than with-compression at high concurrency (NVMe: packfile 1,225 vs 2,805 MB/s, RocksDB 656 vs 926 MB/s) — writing 4.6x more data to disk dominates.

### Read (warm page cache)

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 5,232 MB/s | 567 MB/s | 2,561 MB/s | 487 MB/s |
| Point read | 6.0 us | 8.8 us | 11.3 us | 13.4 us |
| Scattered 50 | 129 us | 185 us | 158 us | 206 us |
| Range scan 128 | 11 us | 62 us | 22.8 us | 72.0 us |

Notes:
- **Sequential reads**: Packfile 9.2x faster than RocksDB without compression (vs 5.3x with). Without compression, the gap reveals the raw format + API overhead: packfile iterates a flat buffer in pure Go (~68K blocks), while RocksDB decodes prefix-compressed entries via ~26M CGO crossings. With compression, both spend ~350ms decompressing (packfile via CGO, RocksDB internally in C++), which narrows the ratio.
- **Point reads**: Packfile is 1.5x faster without compression (6.0 vs 8.8 us). With compression, 1.19x (11.3 vs 13.4 us). Decompression cost equalizes the two — packfile's Go-side decompress + extract costs slightly more than RocksDB's single C++ Get().
- **Scattered**: With compression, packfile is 1.30x faster (158 vs 206 us). Without compression, packfile is 1.4x faster (129 vs 185 us). RocksDB's `BatchedMultiGetCF` with sorted input is efficient for no-compression point lookups where block-index traversal cost per key is lower (no decompression step).
- **Range scan**: Packfile stays 5.6x faster without compression (11 vs 62 us). With compression, 3.2x faster (22.8 vs 72.0 us).

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 320,000 | 927KB / 14 |
| RocksDBOpen | 1,951,000 | 537B / 7 |

Packfile opens in 320us (reads index into memory). RocksDB opens in 2.0ms (with skip-stats optimizations). These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 413MB |
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

MPHF+packfile is 10% smaller. Build time: MPHF 6.1s vs RocksDB 7.5s.

### Warm Cache — Single Key Lookup

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFLookup | 12,876 | 6,953B / 147 |
| BitmapRocksDBLookup | 17,091 | 6,453B / 149 |

MPHF is **1.3x faster** for single-key lookups. Both allocations are dominated by roaring bitmap deserialization. The gap is narrower than ARM64 (1.5x) because x86's faster single-core performance benefits both implementations, compressing the ratio.

### Warm Cache — Parallel Lookup (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFParallel15 | 1,234 | 6,446B / 145 |
| BitmapRocksDBParallel15 | 4,578 | 6,346B / 148 |

MPHF is **3.7x faster** under parallel load. The hash-to-offset lookup scales linearly with cores; RocksDB's block index traversal and CGO crossings create contention.

### Warm Cache — Batch Lookup (15 keys)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| BitmapMPHFLookupKeys15 | 140,336 | 137,193B / 2,199 |

`LookupKeys` resolves 15 keys in a single call (~9.4µs/key). RocksDB's `LookupKeys` uses `BatchedMultiGetCF` with sorted input and concurrent goroutines — see cold cache benchmarks below for comparison.

### Cold Cache — NVMe (drop page cache, open + lookup + close per iteration)

Each iteration drops file cache via `posix_fadvise FADV_DONTNEED` (MPHF/packfile) or directory walk (RocksDB), then times open + N lookups + close. All formats use the `IndexReader` interface. RocksDB parallel uses `LookupKeys` with `WithConcurrency(N)` which splits keys across goroutines calling `BatchedMultiGetCF`. 5 samples, median of last 4 (first iteration excluded as warmup).

| Lookups | MPHF serial (µs) | MPHF LookupKeys (µs) | RocksDB serial (µs) | RocksDB parallel (µs) |
|---------|------------------|-----------------------|---------------------|-----------------------|
| 1 | 666 | 730 | 1,420 | 1,267 |
| 5 | 1,037 | 745 | 1,838 | 1,545 |
| 15 | 2,186 | 909 | 2,952 | 1,795 |
| 50 | 6,111 | 1,543 | 6,462 | 2,116 |

Notes:
- **MPHF serial is 1.1-2.1x faster than RocksDB serial** across all lookup counts. At 1 lookup, the gap is dominated by open latency: MPHF opens in ~0.6ms (mmap hash + packfile) vs RocksDB ~1.4ms.
- **MPHF LookupKeys is the fastest option at all counts.** At 50 lookups, LookupKeys (1.5ms) is 4.0x faster than MPHF serial (6.1ms) — the batch API sorts by file offset and coalesces nearby reads, converting 50 random I/Os into fewer sequential ones.
- **RocksDB parallel converges with MPHF LookupKeys at high counts** (50 lookups: 2.1ms vs 1.5ms). Both amortize open cost and parallelize I/O, but LookupKeys has lower overhead (no goroutine spawn, sorted access pattern).
- All latencies are ~1.5-1.6x faster than ARM64 (NVMe I/O is similar; x86 CPU handles hash computation, mmap setup, and bitmap deserialization faster).

### Cold Cache — EBS (gp3, 3,000 IOPS baseline)

Same methodology as NVMe cold cache, but on EBS gp3 volume. Median of 7 samples (first excluded as warmup) for 15- and 50-lookup benchmarks to reduce EBS variance.

| Lookups | MPHF serial (µs) | MPHF LookupKeys (µs) | RocksDB serial (µs) | RocksDB parallel (µs) |
|---------|------------------|-----------------------|---------------------|-----------------------|
| 1 | 3,590 | 3,622 | 4,822 | 5,078 |
| 5 | 6,799 | 3,816 | 7,866 | 5,170 |
| 15 | 15,691 | 6,115 | 14,993 | 5,166 |
| 50 | 42,703 | 16,180 | 41,974 | 17,294 |

Notes:
- **EBS amplifies the I/O advantage of batch/parallel.** At 50 lookups, MPHF LookupKeys (16ms) is 2.6x faster than MPHF serial (43ms). RocksDB parallel (17ms) is 2.4x faster than serial (42ms).
- **MPHF serial vs RocksDB serial:** MPHF is 1.3x faster at 1 lookup (open latency dominates). At 15+ lookups both converge as EBS IOPS becomes the bottleneck.
- **At 50 lookups, serial variants converge** (MPHF 42.7ms ≈ RocksDB 42.0ms) — both are IOPS-limited at 50 random reads on gp3.
- **LookupKeys vs RocksDB parallel stay close** (16.2ms vs 17.3ms at 50 lookups) — EBS IOPS ceiling equalizes formats when I/O dominates.
- **EBS latencies are 3-7x higher than NVMe** across the board, consistent with gp3 single-digit-ms access latency vs NVMe's sub-ms.
