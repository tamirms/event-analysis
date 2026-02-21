# Benchmark Results (ARM64)

Machine: AWS Graviton2 (Neoverse-N1), 32 cores, 123GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), system libzstd 1.5.7
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

Both formats use **zstd level 3** compression with ~27.6KB block size (128 events). RocksDB SST uses format version 5, `BlockRestartInterval=128`, `BlockSizeDeviation=100`. Compression uses a thin CGO wrapper linking system libzstd (see `zstd/` package doc for rationale).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB is a general-purpose sorted key-value format with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for either format (zero memory overhead beyond each format's intrinsic index).

**Note on RocksDB numbers:** RocksDB is accessed via grocksdb (CGO bindings). Each CGO boundary crossing costs ~50-100ns. For sequential and batch reads with millions of per-item calls (Valid/Next/ValueSlice), CGO overhead accounts for an estimated 10-25% of measured RocksDB time. This is inherent to using RocksDB from Go and representative of real-world Go application performance, but the raw C API would be faster. Checksum verification is disabled for RocksDB (`SetVerifyChecksums(false)`), matching packfile which relies on zstd's built-in content checksum only.

**Methodology:** Read benchmarks use 5 samples per benchmark, each in a separate process, except where noted. Write benchmarks use `TestWriteThroughput` (single run with internal median). Memory benchmarks are single-run with `GODEBUG=madvdontneed=1` and `GOGC=1`. See CLAUDE.md for instructions on running benchmarks.

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
| PackfileWrite | 191 | 10.11 | 227 | 8.50 |
| PackfileWrite (4 goroutines) | 498 | 3.88 | 1,082 | 1.79 |
| PackfileWrite (8 goroutines) | 598 | 3.23 | 1,936 | 1.00 |
| PackfileWrite (16 goroutines) | 567 | 3.41 | 2,390 | 0.81 |
| **PackfileWrite (24 goroutines)** | **595** | **3.25** | **2,313** | **0.84** |
| PackfileWrite (32 goroutines) | 600 | 3.22 | 2,244 | 0.86 |
| RocksDBWrite | 181 | 10.68 | 201 | 9.62 |
| RocksDBWrite (4 threads) | 428 | 4.52 | 486 | 3.98 |
| RocksDBWrite (8 threads) | 464 | 4.17 | 535 | 3.62 |

Notes:
- **Packfile with 24 goroutines on NVMe (2,313 MB/s) is 4.3x faster than RocksDB's best (535 MB/s).**
- **Serial writes are nearly tied:** Packfile 227 MB/s vs RocksDB 201 MB/s on NVMe (1.13x). On EBS, 191 vs 181 MB/s (1.05x). The serial bottleneck is zstd compression, which dominates identically for both formats.
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- RocksDB's `SetCompressionOptionsParallelThreads` parallelizes block compression within the SSTFileWriter, but the serial `Add()` loop (8.7M CGO crossings) limits scaling: t=4→t=8 yields only 486→535 MB/s (+10%).
- **NVMe plateaus at ~2.3-2.4 GB/s (c=24-32)** where the serial main goroutine (block building) becomes the bottleneck.
- **EBS plateaus at ~600 MB/s (c=8+)** — gp3 write bandwidth ceiling.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 44 MB |
| PackfileWrite (8 goroutines) | 75 MB (+31 MB over serial) |
| RocksDBWrite | 32 MB |

The 44 MB packfile baseline includes the zstd context and scratch buffer. Streaming compression adds ~31 MB for in-flight blocks across compress workers. RocksDB is leanest at 32 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process with `GODEBUG=madvdontneed=1`.

### Read Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileSeqRead (full 8.7M events) | 4.1 MB |
| PackfileReadIndices (1,000 scattered, c=8) | 1.7 MB |

Read memory is minimal — just pooled `blockBuf` decoders from `sync.Pool`, scaling linearly with concurrency.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 1,282 | 1,510M | 1.6MB / 33 |
| RocksDBSeqRead | 291 | 6,641M | 8B / 1 |

Packfile is 4.4x faster than RocksDB for sequential reads. RocksDB uses `ValueSlice` (zero-copy) and iterator reuse to minimize Go-side allocations (8B / 1 alloc vs previous 560MB / 35M), but CGO overhead from ~17.5M boundary crossings (Valid/Next/ValueSlice) still dominates.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 23,300 | 272B / 2 |
| RocksDBRandomRead | 27,300 | 48B / 4 |

Packfile is 1.17x faster. RocksDB's `BlockRestartInterval=128` reduces per-block restart-point overhead (improving write throughput and compression) but increases linear scanning within blocks during point lookups. With the default restart interval (16), RocksDB was slightly faster (22.6us vs 24.0us).

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 1,473 | 231B / 1 |
| RocksDBParallelRead | 1,540 | 24B / 3 |

Effectively tied under parallel load (packfile 1.05x faster).

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 24,890 | 293B / 7 |
| RocksDBReadBatch128 | 122,235 | 0B / 0 |

Packfile is 4.9x faster than RocksDB for batch reads from a known offset. One block decompress gives all 128 events, while RocksDB iterates with 128 CGO boundary crossings. RocksDB shows zero Go-side allocations thanks to `ValueSlice` (zero-copy from C++ memory).

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 46,156 | 343B / 7 |
| RocksDBRangeScan128 | 128,234 | 0B / 0 |

Packfile is 2.8x faster than RocksDB. Compared to batch-from-0 (24.9us), the random seek adds ~21us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 241,533 | 33KB / 80 |
| RocksDBReadIndices | 299,134 | 5.5KB / 155 |

Both use 8 internal goroutines for parallel I/O. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input and `SetFillCache(false)`. Packfile is 1.24x faster.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 97,910 | 38KB / 81 |
| RocksDBParallelReadIndices | 90,822 | 5.5KB / 155 |

Effectively tied under parallel scattered load (RocksDB 1.08x faster). RocksDB's lower per-request allocation overhead (5.5KB vs 38KB) gives it a slight edge when 32 goroutines compete for memory.

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

RocksDB uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and `BatchedMultiGetCF` with sorted input, no checksum verification. Parallel variants split keys across N goroutines each calling `BatchedMultiGetCF`.

| Benchmark | Goroutines | NVMe (ms) | EBS (ms) |
|-----------|-----------|-----------|----------|
| Packfile | 1 | 178 | 695 |
| Packfile | 4 | 48 | 192 |
| Packfile | 8 | 26 | 331 |
| Packfile | 16 | 16 | 338 |
| Packfile | 32 | 10 | 331 |
| Packfile | 64 | 8.3 | 331 |
| Packfile | 128 | 7.2 | 331 |
| RocksDB | 1 (8 internal) | 284 | 641 |
| RocksDB | 8 | 61 | 262 |
| RocksDB | 32 | 38 | 333 |
| RocksDB | 64 | 35 | 333 |

Notes:
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (8.3ms) is 4.2x faster than RocksDB at c=64 (35ms). The gap comes from packfile's simpler index lookup (in-memory offset array vs block index traversal).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=8+, both formats converge to ~331ms (the 3,000 IOPS floor for 1,000 random reads). Packfile c=4 (192ms) is the sweet spot before hitting the IOPS ceiling.
- Packfile c=128 on NVMe (7.2ms) shows diminishing returns — approaching NVMe's random I/O floor for 1,000 reads.
- Cold cache numbers are from the previous fixture format (pre-optimization). Re-running would show faster RocksDB open latency but similar I/O-bound results.

## Raw I/O (No Compression)

Compression and CRC disabled for both formats to isolate format overhead, block-building cost, and I/O patterns.

### Write

| Benchmark | NVMe (MB/s) | EBS (MB/s) |
|-----------|------------|-----------|
| Packfile (no zstd) | 827 | 124 |
| RocksDB (no zstd) | 461 | 132 |

Packfile raw I/O is 1.8x faster than RocksDB on NVMe (was 3.6x before `BlockRestartInterval=128`). The larger restart interval dramatically reduced RocksDB's per-block overhead — fewer restart points means less metadata per block.

On EBS, RocksDB no-compression (132 MB/s) slightly exceeds packfile (124 MB/s) — both are EBS bandwidth-limited and the difference is noise.

Without compression, both formats are **slower** than with-compression at high concurrency (NVMe: packfile 827 vs 2,313 MB/s, RocksDB 461 vs 535 MB/s) — writing 4.4x more data to disk dominates.

### Read (warm page cache)

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 4,088 MB/s | 299 MB/s | 1,282 MB/s | 291 MB/s |
| Point read | 6.9 us | 12.4 us | 23.3 us | 27.3 us |
| Scattered 50 | 125 us | 546 us | 242 us | 299 us |
| Range scan 128 | 14 us | 118 us | 46.2 us | 128 us |

Notes:
- **Sequential reads**: Packfile 13.7x faster than RocksDB without compression (vs 4.4x with). The format advantage widens because decompression is no longer amortized over sequential access.
- **Point reads**: Packfile is 1.8x faster without compression (6.9 vs 12.4 us). With compression, packfile is 1.17x faster (23.3 vs 27.3 us). The `BlockRestartInterval=128` tradeoff is visible here — RocksDB point reads got slower to pay for better write performance.
- **Scattered**: Packfile 1.24x faster with compression (242 vs 299 us). Without compression, packfile is 4.4x faster (125 vs 546 us) — offset array lookup vs block index traversal. Decompression cost narrows the gap when both use optimized libzstd.
- **Range scan**: Packfile stays 8.4x faster without compression (14 vs 118 us). With compression, 2.8x faster (46.2 vs 128 us).
- No-compression read benchmarks use the previous fixture format. Sequential, scattered, and range scan numbers are expected to be similar with the new format since the optimizations primarily affect write path and block metadata, not read-dominant I/O patterns.

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 835,000 | 927KB / 11 |
| RocksDBOpen | 5,100,000 | 40B / 2 |

Packfile opens in ~0.8ms (reads offset array into memory). RocksDB opens in ~5.1ms with `SkipStatsUpdateOnDBOpen` and `SkipCheckingSSTFileSizesOnDBOpen` (was ~12ms without these optimizations). These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 413MB |
| RocksDB (zstd, bs=27.6KB) | 437MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.6x, RocksDB 4.3x. The gap narrowed from 4.4x/4.2x with the previous format — `BlockRestartInterval=128` and `BlockSizeDeviation=100` reduce per-block metadata overhead in RocksDB, improving compression.
