# Benchmark Results

Machine: Intel Xeon Platinum 8375C @ 2.90GHz, 32 cores, 61GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), system libzstd 1.5.7
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

Both formats use **zstd level 3** compression with ~27.6KB block size (128 events). Compression uses a thin CGO wrapper linking system libzstd (see `zstd/` package doc for rationale).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB is a general-purpose sorted key-value format with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for either format (zero memory overhead beyond each format's intrinsic index).

**RocksDB optimizations applied:** FormatVersion(5) (shortest index keys, compact block handles), BlockSizeDeviation(100) (fill blocks to target size), SetMoveFiles(true) (atomic rename on ingest), BlockRestartInterval(128) (one restart point per block, minimizing per-entry overhead for sequential ordinal keys), explicit zstd level 3 via SetCompressionOptions, skip-stats-on-open, disabled auto-compactions, single file-opening thread. Read-side: `SetVerifyChecksums(false)`, `SetFillCache(false)`, `Iterator.ValueSlice()` (zero-alloc per-item access) — packfile verifies integrity via zstd's built-in content checksum during decompression; RocksDB does the same (zstd checksum verified during decompression) so the additional CRC32c verification is redundant for this comparison.

**Note on CGO overhead:** Both formats use CGO — packfile for zstd compress/decompress calls, RocksDB for all API access via grocksdb. The key difference is **API granularity**: packfile makes ~68K CGO calls for a full sequential scan (one decompress per block of 128 events, then pure Go iteration), while RocksDB makes ~26M calls (Valid + Next + ValueSlice per item). Each CGO crossing costs ~50-100ns, so this 382x difference in crossing count is significant for iterator-heavy access patterns. For point reads (single `Get` call), CGO overhead is negligible for both.

**Methodology:** Read benchmarks use `benchstat` with 5 samples per benchmark, each in a separate process. Write benchmarks use `TestWriteThroughput` (single run with internal median). Memory benchmarks are single-run with `GODEBUG=madvdontneed=1` and `GOGC=1`. See CLAUDE.md for instructions on running benchmarks.

## Zstd Compression

Comparison of zstd implementations on 68,295 blocks (avg 28,314 bytes each, ~1.9GB total raw data). All at level 3. The CGO wrapper links system libzstd 1.5.7; klauspost/compress is a pure-Go implementation.

| Benchmark | CGO (MB/s) | klauspost (MB/s) | Ratio |
|-----------|-----------|-----------------|-------|
| Compress (serial) | 591 | 127 | 4.7x |
| Compress (8 goroutines) | 4,689 | 972 | 4.8x |
| Decompress (serial) | 3,131 | 2,389 | 1.3x |
| Decompress (8 goroutines) | 23,159 | 17,866 | 1.3x |

Compression ratio: CGO 4.56x, klauspost 4.53x (negligible difference).

The 4.7x compression speed advantage comes from libzstd's hand-optimized SSE/AVX inner loops vs klauspost's generic Go implementation. The decompression gap is smaller (1.3x) because decompression is less compute-intensive. See the `zstd/` package doc for why we wrote a CGO wrapper instead of using existing Go bindings.

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 297 | 6.50 | 439 | 4.41 |
| PackfileWrite (4 goroutines) | 575 | 3.36 | 1,639 | 1.18 |
| **PackfileWrite (8 goroutines)** | **593** | **3.26** | **2,532** | **0.76** |
| PackfileWrite (16 goroutines) | 593 | 3.26 | 2,535 | 0.76 |
| PackfileWrite (24 goroutines) | 589 | 3.28 | 2,488 | 0.78 |
| PackfileWrite (32 goroutines) | 594 | 3.26 | 2,517 | 0.77 |
| RocksDBWrite | 356 | 5.44 | 362 | 5.34 |
| RocksDBWrite (4 threads) | 756 | 2.56 | 859 | 2.25 |
| RocksDBWrite (8 threads) | 552 | 3.50 | 926 | 2.09 |

Notes:
- **Packfile with 8 goroutines on NVMe (2,532 MB/s) is 2.7x faster than RocksDB's best (926 MB/s).**
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- RocksDB uses `SSTFileWriter` with `SetMoveFiles(true)` for atomic rename on ingest (avoids file copy overhead).
- **Serial writes are CPU-bound** (zstd compression dominates). With 4.6x compression, the actual disk write rate for serial packfile is only ~96 MB/s — trivial for both EBS and NVMe.
- **EBS**: Packfile plateaus at ~593 MB/s (c=8+). RocksDB peaks at 756 MB/s (t=4), then drops at t=8 — SST file construction + ingest becomes the bottleneck before EBS bandwidth is saturated.
- **NVMe**: Packfile plateaus at ~2.5 GB/s (c=8-16) where the serial main goroutine (block building) becomes the bottleneck.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 30 MB |
| PackfileWrite (8 goroutines) | 55 MB (+25 MB over serial) |
| RocksDBWrite (8 threads) | 34 MB |

The 30 MB packfile baseline includes the zstd C context and scratch buffer. Streaming compression adds ~25 MB for in-flight blocks across compress workers. RocksDB is comparable at 34 MB (C-side buffers + Go CGO overhead).

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
| PackfileSeqRead | 2,624 | 737M | 782KB / 20 |
| RocksDBSeqRead | 527 | 3,670M | 8B / 1 |

Packfile is 5.0x faster than RocksDB for sequential reads. The gap comes from two factors: (1) RocksDB's per-item iterator API requires ~26M CGO crossings (Valid + Next + ValueSlice per event) vs packfile's ~68K (one decompress per block, then pure Go iteration), and (2) RocksDB's prefix-compressed KV entry decoding is inherently more work per item than packfile's flat offset-array format. The no-compression benchmarks below show the gap widens to 9.1x without zstd, confirming the format overhead is the dominant factor.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 11,300 | 256B / 2 |
| RocksDBRandomRead | 12,930 | 48B / 4 |

Packfile is 1.14x faster. With both formats using optimized system libzstd, decompression is no longer the bottleneck. RocksDB's single CGO call (Get) handles block lookup, decompression, and value extraction entirely in C++, while packfile requires a Go-C round-trip for decompression plus Go-side event extraction. RocksDB uses BlockRestartInterval(128) which trades binary search within blocks for less per-entry overhead — optimal for this workload with small sequential keys.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 687 | 230B / 1 |
| RocksDBParallelRead | 746 | 24B / 3 |

Under parallel load both formats are within ~10% (~700ns per read).

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 10,690 | 237B / 7 |
| RocksDBReadBatch128 | 63,970 | 0B / 0 |

Packfile is 6.0x faster than RocksDB for batch reads from a known offset. Packfile decompresses one block (single CGO call) to yield all 128 events in a flat buffer. RocksDB iterates with 384 CGO crossings (3 per event x 128) plus per-entry prefix-compressed key decoding.

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 22,320 | 264B / 7 |
| RocksDBRangeScan128 | 67,410 | 0B / 0 |

Packfile is 3.0x faster than RocksDB. Compared to batch-from-0 (10.7us), the random seek adds ~12us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 156,200 | 35KB / 81 |
| RocksDBReadIndices | 187,200 | 5.5KB / 155 |

Both use 8 internal goroutines for parallel I/O. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input and `SetFillCache(false)`. Packfile is 1.20x faster.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 42,150 | 42KB / 81 |
| RocksDBParallelReadIndices | 40,090 | 5.5KB / 155 |

Under parallel scattered load RocksDB is 1.05x faster — effectively tied.

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

RocksDB uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and `BatchedMultiGetCF` with sorted input, `SetAsyncIO(true)`, no checksum verification. Parallel variants split keys across N goroutines each calling `BatchedMultiGetCF`.

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
| Packfile (no zstd) | 1,002 | 128 |
| RocksDB (no zstd) | 656 | 124 |

Packfile raw I/O is 1.5x faster than RocksDB on NVMe. Without compression, both formats are **I/O-bound** — writing uncompressed data (1.9GB) dominates. RocksDB's gap on NVMe is SST block construction + key encoding + ingest overhead. On EBS both are equal (both hit EBS bandwidth limit).

Without compression, both formats are **slower** than with-compression at high concurrency (NVMe: packfile 1,002 vs 2,532 MB/s, RocksDB 656 vs 926 MB/s) — writing 4.6x more data to disk dominates.

### Read (warm page cache)

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 5,296 MB/s | 584 MB/s | 2,624 MB/s | 527 MB/s |
| Point read | 4.1 us | 6.6 us | 11.3 us | 12.9 us |
| Scattered 50 | 105 us | 374 us | 156 us | 187 us |
| Range scan 128 | 10 us | 57 us | 22.3 us | 67.4 us |

Notes:
- **Sequential reads**: Packfile 9.1x faster than RocksDB without compression (vs 5.0x with). Without compression, the gap reveals the raw format + API overhead: packfile iterates a flat buffer in pure Go (~68K blocks), while RocksDB decodes prefix-compressed entries via ~26M CGO crossings. With compression, both spend ~350ms decompressing (packfile via CGO, RocksDB internally in C++), which narrows the ratio.
- **Point reads**: Packfile is 1.6x faster without compression (4.1 vs 6.6 us). With compression, 1.14x (11.3 vs 12.9 us). Decompression cost equalizes the two — packfile's Go-side decompress + extract costs slightly more than RocksDB's single C++ Get().
- **Scattered**: With compression, packfile is 1.20x faster (156 vs 187 us). Without compression, packfile is 3.6x faster (105 vs 374 us) — offset array lookup vs block index traversal. Decompression cost narrows the gap when both use optimized libzstd.
- **Range scan**: Packfile stays 5.7x faster without compression (10 vs 57 us). With compression, 3.0x faster (22.3 vs 67.4 us).

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 320,000 | 905KB / 11 |
| RocksDBOpen | 1,951,000 | 40B / 2 |

Packfile opens in 320us (reads index into memory). RocksDB opens in 2.0ms (with skip-stats optimizations). These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 413MB |
| RocksDB (zstd, bs=27.6KB) | 437MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.6x, RocksDB 4.3x.
