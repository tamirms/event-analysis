# Benchmark Results

Machine: Intel Xeon Platinum 8375C @ 2.90GHz, 32 cores, 61GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7), Pebble v1.1.5
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

All three formats use **zstd level 3** compression with ~27.6KB block size (128 events).

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 136 | 14.2 | 162 | 11.9 |
| PackfileWrite (4 goroutines) | 321 | 6.0 | 499 | 3.9 |
| **PackfileWrite (8 goroutines)** | **423** | **4.6** | **814** | **2.4** |
| SSTWrite | 47 | 40.8 | 47 | 40.8 |
| RocksDBWrite | 40 | 48.0 | 46 | 42.4 |
| RocksDBWrite (4 threads) | 96 | 20.1 | 133 | 14.5 |
| RocksDBWrite (8 threads) | 143 | 13.6 | 239 | 8.1 |

Notes:
- **Packfile with 8 goroutines on NVMe (814 MB/s) is 3.4x faster than RocksDB's best (239 MB/s).**
- Parallel packfile uses batched compression: 256 blocks are compressed across N goroutines, then written sequentially.
- Pebble SSTable is fully CPU-bound (no NVMe benefit, 47 MB/s on both).
- NVMe benefit scales with parallelism: packfile serial +20%, packfile 8-goroutine +93%, RocksDB 8-thread +67%.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 76 MB |
| PackfileWrite (8 goroutines) | 100 MB (+24 MB over serial) |
| RocksDBWrite (8 threads) | 35 MB |

The 76 MB packfile baseline is the zstd encoder's internal buffer pool (klauspost/compress retains window/match buffers). Parallel compression adds ~24 MB for the batch of 256 blocks. RocksDB is leanest at 35 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GODEBUG=madvdontneed=1` and `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 2051 | 943M | 1.1MB / 37 |
| SSTSeqRead | 394 | 4907M | 6.4KB / 12 |
| RocksDBSeqRead | 265 | 7285M | 280MB / 17.5M |

Packfile is 5.2x faster than SSTable and 7.7x faster than RocksDB for sequential reads.

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 14,365 | 241B / 1 |
| SSTRandomRead | 424,547 | 0B / 0 |
| RocksDBRandomRead | 65,962 | 24B / 3 |

Packfile is 4.6x faster than RocksDB and 30x faster than SSTable for random reads.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 747 | 245B / 1 |
| SSTParallelRead | 22,522 | 4B / 0 |
| RocksDBParallelRead | 3,424 | 24B / 3 |

Packfile is 4.6x faster than RocksDB and 30x faster than SSTable under parallel load.

## Batch Read (128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 11,679 | 219B / 7 |
| SSTReadBatch128 | 480,268 | 0B / 0 |
| RocksDBReadBatch128 | 164,856 | 4.1KB / 257 |

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 28,154 | 236B / 7 |
| SSTRangeScan128 | 504,035 | 0B / 0 |
| RocksDBRangeScan128 | 190,882 | 4.1KB / 257 |

Packfile is 6.8x faster than RocksDB and 18x faster than SSTable. Compared to batch-from-0 (12μs), the random seek adds ~16μs for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 183,209 | 60KB / 83 |
| SSTReadIndices | 21,197,755 | 1.6KB / 4 |
| RocksDBReadIndices | 724,028 | 6.7KB / 204 |

Both packfile and RocksDB use 8 internal goroutines for parallel I/O. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input, `SetVerifyChecksums(false)`, `SetFillCache(false)`. Packfile `ReadIndices` uses work-stealing parallel pread.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 45,059 | 40KB / 81 |
| RocksDBParallelReadIndices | 174,769 | 6.7KB / 204 |

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
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (5.2ms) is 4.4x faster than RocksDB at c=64 (23ms). The gap comes from packfile's lighter-weight open (0.5ms vs ~5ms) and simpler index lookup (in-memory offset array vs block index traversal + block decompression overhead).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=8, RocksDB (238ms) is slightly faster than packfile (256ms) — both saturate the IOPS budget, and per-block overhead differences are negligible. At c=32+, both converge to ~333ms (the 3,000 IOPS floor).
- **Mmap reads (`SetAllowMmapReads`) hurt** both NVMe and EBS due to per-page fault overhead exceeding explicit pread cost.
- Serial (c=1) performance reflects the raw per-I/O latency: NVMe ~87μs vs EBS ~738μs.

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
