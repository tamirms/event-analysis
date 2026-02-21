# Benchmark Results

Machine: Intel Xeon Platinum 8375C @ 2.90GHz, 32 cores, 61GB RAM
Go 1.26.0, RocksDB 10.9.1 (grocksdb v1.10.7)
Dataset: 8,741,671 events, 1,888,365KB total raw (avg 221B/event)

Both formats use **zstd level 3** compression with ~27.6KB block size (128 events).

**Comparison context:** The packfile is a specialized append-only format with positional access (O(1) offset-array lookup). RocksDB is a general-purpose sorted key-value format with key-ordered access (O(log N) block-index traversal). The packfile trades key-based lookup, range deletion, merge semantics, and snapshots for faster positional reads and writes. No block cache or bloom filters are used for either format (zero memory overhead beyond each format's intrinsic index).

**Note on RocksDB numbers:** RocksDB is accessed via grocksdb (CGO bindings). Each CGO boundary crossing costs ~50-100ns. For sequential and batch reads with millions of per-item calls (Valid/Next/Value), CGO overhead accounts for an estimated 10-25% of measured RocksDB time. This is inherent to using RocksDB from Go and representative of real-world Go application performance, but the raw C API would be faster. Checksum verification is disabled for RocksDB (`SetVerifyChecksums(false)`), matching packfile which relies on zstd's built-in content checksum only.

**Methodology:** Read benchmarks use `benchstat` with 5 samples per benchmark, each in a separate process. Write benchmarks use `TestWriteThroughput` (single run with internal median). Memory benchmarks are single-run with `GODEBUG=madvdontneed=1`. See CLAUDE.md for instructions on running benchmarks (separate-process execution, avoiding GOGC contamination, etc.).

## Write / Ingestion

| Benchmark | EBS (MB/s) | EBS (s) | NVMe (MB/s) | NVMe (s) |
|-----------|-----------|---------|-------------|----------|
| PackfileWrite | 90 | 21.4 | 151 | 12.8 |
| PackfileWrite (4 goroutines) | 312 | 6.2 | 671 | 2.9 |
| PackfileWrite (8 goroutines) | 478 | 4.0 | 1,022 | 1.9 |
| PackfileWrite (16 goroutines) | 592 | 3.3 | 1,686 | 1.2 |
| **PackfileWrite (24 goroutines)** | **591** | **3.3** | **1,823** | **1.1** |
| PackfileWrite (32 goroutines) | 591 | 3.3 | 1,757 | 1.1 |
| RocksDBWrite | 49 | 39.3 | 57 | 34.1 |
| RocksDBWrite (4 threads) | 112 | 17.3 | 167 | 11.6 |
| RocksDBWrite (8 threads) | 157 | 12.3 | 287 | 6.7 |

Notes:
- **Packfile with 24 goroutines on NVMe (1,823 MB/s) is 6.4x faster than RocksDB's best (287 MB/s).**
- Parallel packfile uses streaming compression: each full block is sent to one of N compress goroutines via a buffered channel. A dedicated writer goroutine receives compressed blocks and uses a reorder buffer to emit them in original order.
- **Serial writes are CPU-bound** (zstd compression dominates). With 4.4x compression, the actual disk write rate is only ~23-34 MB/s — trivial for both EBS and NVMe.
- **EBS plateaus at ~590 MB/s (c=16+)** due to EBS bandwidth limits. **NVMe plateaus at ~1.7-1.8 GB/s (c=16-32)** where the serial main goroutine (block building) becomes the bottleneck.
- Throughput benchmarks do NOT use `GOGC=1` — they measure actual ingestion speed. See memory benchmarks below for working set measurements.

### Write Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileWrite (serial) | 68 MB |
| PackfileWrite (8 goroutines) | 85 MB (+17 MB over serial) |
| RocksDBWrite (8 threads) | 34 MB |

The 68 MB packfile baseline is the zstd encoder's internal buffer pool (klauspost/compress retains window/match buffers). Streaming compression adds ~17 MB for in-flight blocks across compress workers. RocksDB is leanest at 34 MB (C-side buffers + Go CGO overhead).

Measured via `RssAnon` from `/proc/self/status` with `GOGC=1` (minimizes GC headroom to capture actual working set). Each benchmark runs in a separate process.

### Read Peak Memory (RssAnon delta, excluding page cache)

| Benchmark | Peak Delta |
|-----------|-----------|
| PackfileSeqRead (full 8.7M events) | 3.6 MB |
| PackfileReadIndices (1,000 scattered, c=1) | 0.7 MB |
| PackfileReadIndices (1,000 scattered, c=8) | 2.9 MB |
| PackfileReadIndices (1,000 scattered, c=32) | 6.8 MB |

Read memory is minimal — just pooled `blockBuf` decoders (~200KB each) from `sync.Pool`, scaling linearly with concurrency.

## Sequential Read

| Benchmark | Throughput (MB/s) | ns/op | Allocs |
|-----------|------------------|-------|--------|
| PackfileSeqRead | 2,042 | 947M | 1.0MB / 43 |
| RocksDBSeqRead | 255 | 7,594M | 267MB / 17.5M |

Packfile is 8.0x faster than RocksDB for sequential reads. The gap includes significant CGO per-item overhead (~26M boundary crossings for 8.7M events).

## Random Point Read

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRandomRead | 14,450 | 275B / 2 |
| RocksDBRandomRead | 57,810 | 48B / 4 |

Packfile is 4.0x faster than RocksDB for random reads.

## Parallel Read (32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelRead | 835 | 233B / 1 |
| RocksDBParallelRead | 3,328 | 24B / 3 |

Packfile is 4.0x faster than RocksDB under parallel load.

## Batch Read (128 events from offset 0)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadBatch128 | 11,830 | 230B / 7 |
| RocksDBReadBatch128 | 163,200 | 4KB / 256 |

Packfile is 13.8x faster than RocksDB for batch reads from a known offset.

## Range Scan (seek to random offset + read 128 events)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileRangeScan128 | 28,720 | 304B / 7 |
| RocksDBRangeScan128 | 172,100 | 4KB / 256 |

Packfile is 6.0x faster than RocksDB. Compared to batch-from-0 (12us), the random seek adds ~17us for packfile (locating and decompressing the target block).

## Scattered Read (50 random indices)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileReadIndices | 184,900 | 63KB / 83 |
| RocksDBReadIndices | 521,200 | 7.7KB / 214 |

Both use 8 internal goroutines for parallel I/O. Packfile `ReadIndices` uses work-stealing parallel pread. RocksDB splits keys across goroutines each calling `BatchedMultiGetCF` with sorted input and `SetFillCache(false)`.

## Parallel Scattered Read (50 indices, 32 cores)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileParallelReadIndices | 45,450 | 43KB / 82 |
| RocksDBParallelReadIndices | 157,400 | 7.7KB / 214 |

Packfile is 3.5x faster than RocksDB under parallel scattered load.

## Cold Cache Scattered Read (1,000 indices on distinct blocks, includes open)

Each iteration drops page cache (via `posix_fadvise FADV_DONTNEED`), then times open + 1,000 scattered reads + close. All 1,000 indices land on different blocks, forcing 1,000 separate disk I/Os.

RocksDB uses optimized open (`SkipStatsUpdateOnDBOpen`, `SkipCheckingSSTFileSizesOnDBOpen`, single file-opening thread) and `BatchedMultiGetCF` with sorted input, no checksum verification. Parallel variants split keys across N goroutines each calling `BatchedMultiGetCF`.

| Benchmark | Goroutines | NVMe (ms) | EBS (ms) |
|-----------|-----------|-----------|----------|
| Packfile | 1 | 110 | 790 |
| Packfile | 8 | 16 | 257 |
| Packfile | 32 | 6.7 | 332 |
| Packfile | 64 | 5.3 | 332 |
| RocksDB | 1 | 164 | 839 |
| RocksDB | 8 | 31 | 247 |
| RocksDB | 32 | 17 | 335 |
| RocksDB | 64 | 17 | 332 |

Notes:
- **NVMe scales with concurrency** for both formats. Packfile at c=64 (5.3ms) is 3.1x faster than RocksDB at c=64 (17ms). The gap comes from packfile's simpler index lookup (in-memory offset array vs block index traversal).
- **EBS is IOPS-limited at ~3,000 IOPS.** At c=8, both formats are similar (~250ms). At c=32+, both converge to ~332ms (the 3,000 IOPS floor).
- **Mmap reads (`SetAllowMmapReads`) hurt** both NVMe and EBS due to per-page fault overhead exceeding explicit pread cost.

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

## Raw I/O (No Compression)

Compression and CRC disabled for both formats to isolate format overhead, block-building cost, and I/O patterns.

### Write (NVMe)

| Benchmark | Serial (MB/s) | Note |
|-----------|--------------|------|
| Packfile (no zstd) | 951 | Concurrency has no effect — nothing to parallelize |
| RocksDB (no zstd) | 345 | Parallel compression threads have no effect |

Packfile raw I/O is 2.8x faster than RocksDB. The gap is intrinsic format overhead: flat append + offset array vs SST block construction + key encoding + ~8.7M CGO Add calls + file ingest.

### Write (EBS)

| Benchmark | Serial (MB/s) | Note |
|-----------|--------------|------|
| Packfile (no zstd) | 128 | EBS bandwidth-limited writing ~1.9GB uncompressed |
| RocksDB (no zstd) | 41 | SST construction overhead + EBS bandwidth |

Without compression, both formats are **slower** than with-compression at high concurrency (packfile 128 vs 591 MB/s, RocksDB 41 vs 157 MB/s) — writing 4.4x more data to disk dominates.

### Read

| Benchmark | Packfile (no zstd) | RocksDB (no zstd) | Packfile (zstd) | RocksDB (zstd) |
|-----------|-------------------|-------------------|----------------|----------------|
| Sequential | 4,774 MB/s | 470 MB/s | 2,042 MB/s | 255 MB/s |
| Point read | 4.0 us | 5.3 us | 14.5 us | 57.8 us |
| Scattered 50 | 103 us | 299 us | 185 us | 521 us |
| Range scan 128 | 10 us | 67 us | 28.7 us | 172 us |

Notes:
- **Sequential reads**: Packfile 10.2x faster than RocksDB without compression (vs 8.0x with). The format advantage widens because decompression is no longer amortized over sequential access.
- **Point reads**: RocksDB improves 10.9x without compression (57.8→5.3 us) — zstd decompression dominated its per-read cost. Packfile improves 3.6x (14.5→4.0 us). Without compression, they're nearly competitive (4.0 vs 5.3 us).
- **Scattered/range**: Packfile stays 2.9–6.7x faster — offset array lookup vs block index traversal.

## Open Latency (warm page cache)

| Benchmark | ns/op | Allocs |
|-----------|-------|--------|
| PackfileOpen | 342,000 | 927KB / 11 |
| RocksDBOpen | 8,025,000 | 136B / 4 |

Packfile opens in 342us (reads index into memory). RocksDB opens in 8ms. These are warm-cache numbers; cold-cache open is included in the cold cache benchmarks above.

## File Sizes

| Format | Size |
|--------|------|
| Packfile (eventstore) | 425MB |
| RocksDB (zstd, bs=27.6KB) | 454MB |
| Raw data | 1,888MB |

Compression ratios: Packfile 4.4x, RocksDB 4.2x.
