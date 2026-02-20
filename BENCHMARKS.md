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
| PackfileWrite (serial) | ~0 MB (single ~27KB block in flight) |
| PackfileWrite (8 goroutines) | 11 MB (batch of 256 blocks) |
| RocksDBWrite (8 threads) | 0.2 MB (C-side compression buffers) |

Measured via `RssAnon` from `/proc/self/status` with `GODEBUG=madvdontneed=1`, sampling every 5ms. Each benchmark runs in a separate process. Delta is taken against a baseline after `runtime.GC()` with allEvents already resident.

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

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 164,888 | 19KB / 80 |
| SSTReadIndices | 21,197,755 | 1.6KB / 4 |
| RocksDBReadIndices | 3,278,573 | 2.8KB / 154 |

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 37,763 | 18.5KB / 80 |
| RocksDBParallelReadIndices | 177,604 | 2.8KB / 154 |

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
