# Benchmark Results

Machine: Intel Xeon Platinum 8375C @ 2.90GHz, 32 cores, 61GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), Pebble v1.1.5
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

All three formats use **zstd level 3** compression with ~27.6KB block size (128 events).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB and Pebble SSTable are general-purpose sorted key-value formats with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for any format (zero memory overhead beyond each format's intrinsic index).

**Note on RocksDB numbers:** RocksDB is accessed via grocksdb (CGO bindings). Each CGO boundary crossing costs ~50-100ns. For sequential and batch reads with millions of per-item calls (Valid/Next/Value), CGO overhead accounts for an estimated 10-25% of measured RocksDB time. This is inherent to using RocksDB from Go and representative of real-world Go application performance, but the raw C API would be faster. Checksum verification is disabled for RocksDB (`SetVerifyChecksums(false)`), matching packfile which relies on zstd's built-in content checksum only.

See CLAUDE.md for instructions on running benchmarks (separate-process execution, avoiding GOGC contamination, etc.).

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 125 | 15.5 | 103 | 18.7 |
| PackfileWrite (4 goroutines) | 393 | 4.9 | 482 | 4.0 |
| PackfileWrite (8 goroutines) | 542 | 3.6 | 1,027 | 1.9 |
| PackfileWrite (16 goroutines) | 590 | 3.3 | 1,681 | 1.2 |
| **PackfileWrite (24 goroutines)** | **589** | **3.3** | **1,742** | **1.1** |
| PackfileWrite (32 goroutines) | 587 | 3.3 | 1,692 | 1.1 |
| SSTWrite | — | — | 47 | 40.8 |
| RocksDBWrite | 40 | 48.6 | 45 | 42.9 |
| RocksDBWrite (4 threads) | 95 | 20.3 | 132 | 14.7 |
| RocksDBWrite (8 threads) | 141 | 13.7 | 239 | 8.1 |

Notes:
- **Packfile with 24 goroutines on NVMe (1,742 MB/s) is 7.3x faster than RocksDB's best (239 MB/s).**
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- Pebble SSTable is fully CPU-bound (no NVMe benefit, 47 MB/s).
- **Serial writes are CPU-bound** (zstd compression dominates). At ~110 MB/s raw throughput with 4.4x compression, the actual disk write rate is only ~25 MB/s — trivial for both EBS and NVMe. EBS/NVMe serial numbers are within noise of each other.
- **EBS plateaus at ~590 MB/s (c=16+)** due to EBS bandwidth limits. **NVMe plateaus at ~1.7-2.1 GB/s (c=16-24)** where the serial main goroutine (block building) becomes the bottleneck.
- Throughput benchmarks do NOT use `GOGC=1` — they measure actual ingestion speed. See memory benchmarks below for working set measurements.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 75 MB |
| PackfileWrite (8 goroutines) | 94 MB (+19 MB over serial) |
| RocksDBWrite (8 threads) | 35 MB |

The 75 MB packfile baseline is the zstd encoder's internal buffer pool (klauspost/compress retains window/match buffers). Streaming compression adds ~19 MB for in-flight blocks across compress workers. RocksDB is leanest at 35 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process.

### Read Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileSeqRead (full 8.7M events) | 3.9 MB |
| PackfileReadIndices (1,000 scattered, c=1) | 0.6 MB |
| PackfileReadIndices (1,000 scattered, c=8) | 3.0 MB |
| PackfileReadIndices (1,000 scattered, c=32) | 6.8 MB |

Read memory is minimal — just pooled `blockBuf` decoders (~200KB each) from `sync.Pool`, scaling linearly with concurrency.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 2053 | 942M | 10KB / 11 |
| SSTSeqRead | 403 | 4802M | 6.4KB / 8 |
| RocksDBSeqRead | 239 | 8077M | 280MB / 17.5M |

Packfile is 5.1x faster than SSTable and 8.6x faster than RocksDB for sequential reads. The RocksDB gap includes significant CGO per-item overhead (~26M boundary crossings for 8.7M events).

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 14,313 | 254B / 2 |
| SSTRandomRead | 425,992 | 0B / 0 |
| RocksDBRandomRead | 63,973 | 48B / 4 |

Packfile is 4.5x faster than RocksDB and 30x faster than SSTable for random reads.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 839 | 234B / 1 |
| SSTParallelRead | 22,905 | 4B / 0 |
| RocksDBParallelRead | 3,390 | 24B / 3 |

Packfile is 4.0x faster than RocksDB and 27x faster than SSTable under parallel load.

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 11,815 | 219B / 7 |
| SSTReadBatch128 | 479,708 | 0B / 0 |
| RocksDBReadBatch128 | 173,831 | 4KB / 256 |

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 28,206 | 282B / 7 |
| SSTRangeScan128 | 496,336 | 0B / 0 |
| RocksDBRangeScan128 | 182,688 | 4KB / 256 |

Packfile is 6.5x faster than RocksDB and 18x faster than SSTable. Compared to batch-from-0 (12μs), the random seek adds ~16μs for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 182,026 | 58KB / 83 |
| SSTReadIndices | 3,396,999 | 2.3KB / 23 |
| RocksDBReadIndices | 575,864 | 7.7KB / 214 |

All three use 8 internal goroutines for parallel I/O. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input and `SetFillCache(false)`. SSTable splits keys across goroutines each with its own iterator.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 45,713 | 44KB / 82 |
| SSTParallelReadIndices | 1,167,340 | 2.8KB / 24 |
| RocksDBParallelReadIndices | 174,131 | 7.7KB / 214 |

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

RocksDB uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and `BatchedMultiGetCF` with sorted input, no checksum verification. Parallel variants split keys across N goroutines each calling `BatchedMultiGetCF`.

| Benchmark | Goroutines | NVMe (ms) | EBS (ms) |
|-----------|-----------|-----------|----------|
| Packfile | 1 | 109 | 760 |
| Packfile | 8 | 16 | 256 |
| Packfile | 32 | 6.7 | 332 |
| Packfile | 64 | 5.2 | 331 |
| RocksDB | 1 | 175 | 868 |
| RocksDB | 8 | 38 | 238 |
| RocksDB | 32 | 25 | 333 |
| RocksDB | 64 | 23 | 339 |

Notes:
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (5.2ms) is 4.4x faster than RocksDB at c=64 (23ms). The gap comes from packfile's lighter-weight open (0.5ms vs ~5ms) and simpler index lookup (in-memory offset array vs block index traversal).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=8, RocksDB (238ms) is slightly faster than packfile (256ms) — both saturate the IOPS budget, and per-block overhead differences are negligible. At c=32+, both converge to ~333ms (the 3,000 IOPS floor).
- **Mmap reads (`SetAllowMmapReads`) hurt** both NVMe and EBS due to per-page fault overhead exceeding explicit pread cost.
- Serial (c=1) performance reflects the raw per-I/O latency: NVMe ~87μs vs EBS ~738μs.

### Improving EBS Cold Cache Latency

The EBS bottleneck is **IOPS, not bandwidth**. 1,000 scattered reads × ~6.4KB per block = ~6.4MB total data (trivial bandwidth), but 1,000 random I/Os at 3,000 IOPS = ~333ms floor.

| Option | IOPS | Expected latency (c=32) | $/month (us-east-1) |
|--------|------|------------------------|---------------------|
| gp3 baseline | 3,000 | 332 ms | included |
| gp3 + provisioned IOPS | 16,000 | ~62 ms | +$65 (13K × $0.005) |
| io2 | 40,000 | ~25 ms | ~$2,725 (40K × $0.065 + 1TB storage) |
| io2 Block Express | 160,000 | ~6 ms | ~$9,520 |
| NVMe instance storage (i4i.xlarge) | 40,000 | 6.7 ms | ~$250 (instance cost) |

Provisioning gp3 to 16,000 IOPS ($65/month) is the best value — ~5x latency improvement. Beyond that, NVMe instance storage (i4i family) delivers io2-level IOPS at ~11x lower cost, but storage is ephemeral (lost on instance stop). io2 is only justified when you need both high IOPS and persistence without a replication strategy.

## Open Latency

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 500,313 | 927KB / 11 |
| SSTOpen | 12,269 | 4.8KB / 34 |
| RocksDBOpen | 14,422,215 | 136B / 4 |

SSTable opens fastest (12us). Packfile is 500us (reads index into memory). RocksDB is slowest at 14ms.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 425MB |
| SSTable (zstd, bs=27.6KB) | 447MB |
| RocksDB (zstd, bs=27.6KB) | 454MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.4x, SSTable 4.2x, RocksDB 4.2x.
