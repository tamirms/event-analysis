# Benchmark Results (ARM64)

Machine: AWS Graviton2 (Neoverse-N1), 32 cores, 123GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), system libzstd 1.5.7
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

Both formats use **zstd level 3** compression with ~27.6KB block size (128 events). Compression uses a thin CGO wrapper linking system libzstd (see `zstd/` package doc for rationale).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB is a general-purpose sorted key-value format with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for either format (zero memory overhead beyond each format's intrinsic index).

**Note on RocksDB numbers:** RocksDB is accessed via grocksdb (CGO bindings). Each CGO boundary crossing costs ~50-100ns. For sequential and batch reads with millions of per-item calls (Valid/Next/Value), CGO overhead accounts for an estimated 10-25% of measured RocksDB time. This is inherent to using RocksDB from Go and representative of real-world Go application performance, but the raw C API would be faster. Checksum verification is disabled for RocksDB (`SetVerifyChecksums(false)`), matching packfile which relies on zstd's built-in content checksum only.

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
| PackfileWrite | 189 | 10.23 | 234 | 8.27 |
| PackfileWrite (4 goroutines) | 241 | 8.04 | 1,115 | 1.73 |
| PackfileWrite (8 goroutines) | 278 | 6.96 | 1,935 | 1.00 |
| PackfileWrite (16 goroutines) | 294 | 6.57 | 2,261 | 0.86 |
| **PackfileWrite (24 goroutines)** | **423** | **4.57** | **2,368** | **0.82** |
| PackfileWrite (32 goroutines) | — | — | 2,345 | 0.82 |
| RocksDBWrite | 67 | 29.07 | 174 | 11.12 |
| RocksDBWrite (4 threads) | 106 | 18.16 | 376 | 5.14 |
| RocksDBWrite (8 threads) | — | — | 396 | 4.88 |

Notes:
- **Packfile with 24 goroutines on NVMe (2,368 MB/s) is 6.0x faster than RocksDB's best (396 MB/s).**
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- **Serial writes are CPU-bound** (zstd compression dominates). With 4.4x compression, the actual disk write rate for serial packfile is only ~53 MB/s — trivial for both EBS and NVMe.
- **NVMe plateaus at ~2.3-2.4 GB/s (c=24-32)** where the serial main goroutine (block building) becomes the bottleneck.
- EBS results for c=32 and RocksDB t=8 are omitted due to anomalous drops (likely EBS burst credit exhaustion during the long benchmark run).

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
| PackfileSeqRead | 1,257 | 3,077M | 1.6MB / 33 |
| RocksDBSeqRead | 253 | 15,265M | 560MB / 35M |

Packfile is 5.0x faster than RocksDB for sequential reads. RocksDB's per-item CGO overhead (~35M boundary crossings for 17.5M events) contributes significantly to the gap.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 24,040 | 267B / 2 |
| RocksDBRandomRead | 22,635 | 48B / 4 |

Point reads are effectively tied (RocksDB 1.06x faster). With both formats using optimized system libzstd, decompression is no longer the bottleneck. RocksDB's single CGO call (Get) handles block lookup, decompression, and value extraction entirely in C++, while packfile requires a Go→C round-trip for decompression plus Go-side event extraction.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 1,973 | 235B / 1 |
| RocksDBParallelRead | 1,619 | 24B / 3 |

Under parallel load RocksDB is 1.22x faster, for the same reasons as single-threaded point reads.

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 23,534 | 279B / 7 |
| RocksDBReadBatch128 | 122,067 | 4KB / 256 |

Packfile is 5.2x faster than RocksDB for batch reads from a known offset. Here packfile's advantage returns: one block decompress gives all 128 events, while RocksDB iterates with 256 CGO boundary crossings.

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 46,811 | 346B / 7 |
| RocksDBRangeScan128 | 131,092 | 4KB / 256 |

Packfile is 2.8x faster than RocksDB. Compared to batch-from-0 (23.5us), the random seek adds ~23us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 247,676 | 30KB / 80 |
| RocksDBReadIndices | 289,819 | 7.9KB / 214 |

Both use 8 internal goroutines for parallel I/O. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input and `SetFillCache(false)`. Packfile is 1.17x faster — effectively similar.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 121,350 | 34KB / 81 |
| RocksDBParallelReadIndices | 124,524 | 7.9KB / 214 |

Effectively tied under parallel scattered load (packfile 1.03x faster).

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

## Raw I/O (No Compression)

Compression and CRC disabled for both formats to isolate format overhead, block-building cost, and I/O patterns.

### Write

| Benchmark | NVMe (MB/s) | EBS (MB/s) |
|-----------|------------|-----------|
| Packfile (no zstd) | 822 | 55 |
| RocksDB (no zstd) | 226 | 36 |

Packfile raw I/O is 3.6x faster than RocksDB on NVMe. The gap is intrinsic format overhead: flat append + offset array vs SST block construction + key encoding + ~8.7M CGO Add calls + file ingest.

Without compression, both formats are **slower** than with-compression at high concurrency (NVMe: packfile 822 vs 2,368 MB/s, RocksDB 226 vs 396 MB/s) — writing 4.4x more data to disk dominates.

### Read (warm page cache)

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 4,088 MB/s | 299 MB/s | 1,257 MB/s | 253 MB/s |
| Point read | 6.9 us | 12.4 us | 24.0 us | 22.6 us |
| Scattered 50 | 125 us | 546 us | 248 us | 290 us |
| Range scan 128 | 14 us | 118 us | 46.8 us | 131 us |

Notes:
- **Sequential reads**: Packfile 13.7x faster than RocksDB without compression (vs 5.0x with). The format advantage widens because decompression is no longer amortized over sequential access.
- **Point reads**: Packfile is 1.8x faster without compression (6.9 vs 12.4 us). With compression, they're tied (24.0 vs 22.6 us). The gap inversion shows that decompression cost, while fast, is the equalizer — packfile's Go-side decompress + extract costs slightly more than RocksDB's single C++ Get().
- **Scattered**: With compression, both formats are nearly tied (248 vs 290 us). Without compression, packfile is 4.4x faster (125 vs 546 us) — offset array lookup vs block index traversal. Decompression cost equalizes them when both use optimized libzstd.
- **Range scan**: Packfile stays 8.4x faster without compression (14 vs 118 us). With compression, 2.8x faster (46.8 vs 131 us).

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 1,049,000 | 1.55MB / 11 |
| RocksDBOpen | 11,699,000 | 136B / 4 |

Packfile opens in ~1ms (reads offset array into memory — larger for 2x dataset). RocksDB opens in ~12ms. These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 425MB |
| RocksDB (zstd, bs=27.6KB) | 454MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.4x, RocksDB 4.2x.
