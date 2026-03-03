# Packfile vs RocksDB: Comprehensive Comparison

This document compares the custom packfile format against RocksDB SSTable format across
performance, code maintainability, dependency burden, and future architecture viability.
Both are used for event storage and bitmap indexing in this project.

## 1. Performance Summary

### Eventstore (8.7M events, 1.9GB raw, zstd level 3)

Numbers from BENCHMARKS.md (Intel Xeon 32-core). ARM64 ratios are similar (see BENCHMARKS-arm64.md).

| Operation | Packfile | RocksDB | Winner |
|-----------|----------|---------|--------|
| **Sequential read** | 2,490 MB/s | 510 MB/s | Packfile (4.9x) |
| **Point read** | 12.3 µs | 14.3 µs | Packfile (1.17x) |
| **Batch 128 events** | 10.8 µs | 64.5 µs | Packfile (5.9x) |
| **Range scan 128** | 23.4 µs | 73.4 µs | Packfile (3.1x) |
| **Scattered 50 indices** | 155 µs | 212 µs | Packfile (1.37x) |
| **Parallel point (32 cores)** | 723 ns | 914 ns | Packfile (1.26x) |
| **Parallel scattered (32 cores)** | 33.5 µs | 48.6 µs | Packfile (1.45x) |
| **Consecutive 1000 blocks** | 1.84 ms | 2.38 ms | Packfile (1.29x) |
| **Write (NVMe, 8 goroutines)** | 2,805 MB/s | 926 MB/s | Packfile (3.0x) |
| **Write (EBS, 8 goroutines)** | 821 MB/s | 756 MB/s | Packfile (1.09x) |
| **Open latency (warm)** | 317 µs | 2,181 µs | Packfile (6.9x) |
| **File size** | 413 MB | 437 MB | Packfile (5.7% smaller) |
| **Write peak memory** | 55 MB | 29 MB | RocksDB (1.9x less) |
| **Read peak memory** | 3 MB | — | Both minimal |

### Bitmap Index (50K events, 10,852 unique keys)

| Operation | MPHF+Packfile | RocksDB | Winner |
|-----------|--------------|---------|--------|
| **Single lookup (warm)** | 12.0 µs | 17.4 µs | MPHF (1.45x) |
| **Parallel 15 (32 cores)** | 1.0 µs | 4.3 µs | MPHF (4.3x) |
| **Cold 1 lookup (NVMe)** | 646 µs | 1,421 µs | MPHF (2.2x) |
| **Cold 50 lookups (NVMe)** | 1,417 µs | 2,139 µs | MPHF LookupKeys (1.5x) |
| **Cold 50 lookups (EBS)** | 16.2 ms | 17.3 ms | Tie (IOPS-limited) |
| **File size** | 53.1 MB | 58.9 MB | MPHF (10% smaller) |

### Why Packfile is Faster

The performance gap is structural, not tunable:

1. **CGO crossing count.** A full sequential scan: packfile makes ~68K CGO calls (one zstd
   decompress per block of 128 events), then pure Go iteration. RocksDB makes ~26M CGO calls
   (Valid + Next + ValueSlice per event). Each crossing costs ~50-100ns. This 382x difference
   dominates iterator-heavy access patterns.

2. **Index structure.** Packfile uses an O(1) offset array (FOR-128 encoded, decoded at open).
   RocksDB uses a B-tree block index requiring O(log N) traversal per lookup. For positional
   access by integer ordinal, the offset array is strictly superior.

3. **Per-entry overhead.** Packfile: ~1 byte/entry (FOR-128 encoded item sizes in the per-record index) + ~4 bytes/record (FOR index CRC32C, amortized over up to 128 items). RocksDB: ~15 bytes/entry (4B ordinal key + 8B internal trailer + 3B varint framing). The 8-byte internal trailer is baked into the SSTable format and cannot be configured away.

4. **Compression input.** Packfile compresses raw event bytes only (the FOR index is uncompressed). RocksDB
   compresses events + ordinal keys + internal trailers + varint framing. More metadata in the
   input means more bytes to compress and slightly worse ratios (4.6x vs 4.3x).

### Where RocksDB Converges

- **EBS cold cache:** At 3,000 IOPS (gp3 baseline), both formats converge because I/O latency
  dominates CPU overhead. 1,000 scattered reads: ~333ms for both.
- **Consecutive block reads (warm):** 1,000 consecutive blocks: packfile 1.84ms vs
  RocksDB 2.38ms (packfile 1.29x faster). Both are decompression-bound (~1,000 zstd
  block decompressions each). Packfile splits large consecutive runs across workers for
  parallel decompression; RocksDB benefits from sequential prefetch in its block cache.
  On cold EBS, packfile pulls ahead (47ms vs IOPS-limited scattered reads at 98ms)
  because batch ReadRange is bandwidth-optimal.
- **EBS writes:** Packfile at 8 goroutines (821 MB/s) is now 1.09x faster than RocksDB at 8
  threads (756 MB/s) on EBS, thanks to `sync_file_range(SYNC_FILE_RANGE_WRITE)` which initiates
  background writeback every 1MB during the append phase. Before this optimization, RocksDB was
  1.25x faster (756 vs 607 MB/s) because packfile's fast compression accumulated 412MB of dirty
  pages that had to be flushed all at once by `fdatasync()` (2,311ms). With `sync_file_range`,
  writeback overlaps with compression — the append phase takes ~2.3s (throttled by EBS bandwidth)
  but `fdatasync()` finishes in ~55ms (total: 2.30s vs old 3.19s). RocksDB's C-side SST builder
  naturally matches EBS writeback pace and doesn't benefit from `bytes_per_sync` (tested: <1%
  change). Both are crash-safe (packfile fsyncs data file + directory; RocksDB fsyncs via
  `SyncIngestedFile`).
- **Write memory:** RocksDB uses 29MB vs packfile's 55MB for parallel writes, because SST
  construction happens in a single C++ buffer rather than N in-flight Go compression goroutines.

## 2. Code Maintainability

### Lines of Code

| Component | Packfile Stack | RocksDB Stack |
|-----------|---------------|---------------|
| Core format (packfile/ + intpack/) | ~1,770 | — |
| Compression (zstd/) | 219 | — |
| Shared helpers (rocksdbutil/) | — | 156 |
| Eventstore impl (thin facade) | ~170 | 401 |
| Bitmapindex impl | ~570 | 343 |
| **Total implementation** | **~2,720** | **900** |

RocksDB requires **~3x less code** because RocksDB handles block management, compression,
checksums, index construction, and file format details internally. The packfile stack implements
all of these:

- Frame-of-Reference encoding/decoding (intpack/: 125 lines)
- Record decoding with zstd + CRC32C + FOR index (packfile/decoder.go)
- Item accumulation, record building, and streaming compression pipeline (packfile/writer.go)
- Item-level access with pooled decoders (packfile/reader.go — ReadItem, ReadRange, ReadItems)
- Work-stealing parallel I/O with direct callback (packfile/reader.go — ReadItems)
- Content hashing (packfile/contenthash.go)
- MPHF construction and query (bitmapindex/writer.go, reader.go)
- Atomic write with temp file + rename + directory fsync (packfile/writer.go)
- CRC32C checksums and trailer parsing (packfile/reader.go, packfile.go)

Note: eventstore is now a thin facade (~170 lines total for reader + writer) that delegates
entirely to packfile. The record-level logic (compression, batching, content hashing) that
was previously in eventstore has been absorbed into packfile.

### Complexity Comparison

| Aspect | Packfile | RocksDB |
|--------|----------|---------|
| **Compression pipeline** | Bounded channel + N workers + reorder buffer (packfile.Writer) | `SetCompressionOptionsParallelThreads(N)` |
| **Parallel read** | ReadItems: record grouping + work-stealing parallel I/O + direct callback | `BatchedMultiGetCF` with sorted input |
| **Block buffer management** | sync.Pool for Decoder (owns ZSTD_DCtx) + read buffers | Handled internally by RocksDB |
| **Index format** | FOR-128 encoding, speculative read at open | B-tree block index (automatic) |
| **Write atomicity** | Manual temp file + rename + dir fsync | `IngestSST` with `MoveFiles(true)` |
| **Checksums** | Manual CRC32C of index + trailer | Automatic per-block (configurable) |

The packfile's complexity is concentrated in two areas that are difficult to get right:
(1) the concurrent compression pipeline with in-order reordering (packfile/writer.go), and
(2) ReadItems parallel I/O (work-stealing workers calling a callback directly with borrowed
entries in packfile/reader.go). Both are well-tested and shared by all callers (eventstore
and bitmapindex are thin facades). This consolidation reduces maintenance surface compared
to the previous architecture where this logic was duplicated.

RocksDB delegates these concerns to a mature, battle-tested C++ library. The tradeoff is
a heavy CGO dependency (see Dependencies below) and less control over performance-critical
code paths.

### Test Coverage

| Package | Tests | Lines |
|---------|-------|-------|
| eventstore (packfile) | 34 | 1,005 |
| eventstore/rocksdb | 17 | 446 |
| bitmapindex (packfile) | in root tests | — |
| bitmapindex/rocksdb | 9 | 314 |
| packfile | 47 | 1,336 |

The packfile tests are more comprehensive because the format has more edge cases to cover
(block boundaries, partial blocks, FOR encoding corner cases, compression failures). RocksDB
tests focus on the Go-side API contract since RocksDB's internals are tested upstream.

## 3. Dependencies

### Packfile Stack

- **zstd/ CGO wrapper** — Links system libzstd >= 1.5.7 via pkg-config. ~60 lines of C glue.
  Single shared library, no transitive deps.
- **streamhash** — Pure Go MPHF library for bitmap index. No CGO.
- **roaring/v2** — Pure Go roaring bitmap. Shared by both implementations.

### RocksDB Stack

- **grocksdb v1.10.7** — CGO bindings to RocksDB. Requires:
  - RocksDB 10.9.1 built from source (system package is incompatible)
  - libzstd 1.5.7 built from source (system package version mismatch)
  - System packages: libsnappy-dev, liblz4-dev, libbz2-dev
  - Custom RPATH configuration for runtime library resolution
- Build time: RocksDB from source takes ~10 minutes. Any version mismatch between
  grocksdb and librocksdb causes cryptic linker or runtime errors.
- Binary size: CGO binaries linking RocksDB are ~40MB larger.

### Dependency Risk Assessment

| Risk | Packfile | RocksDB |
|------|----------|---------|
| **Build complexity** | Low (system libzstd) | High (custom RocksDB build) |
| **Version coupling** | Loose (zstd ABI stable) | Tight (grocksdb ↔ librocksdb) |
| **Cross-compilation** | Straightforward | Requires matching C++ toolchain + all deps |
| **CI/CD setup** | `apt install libzstd-dev` | Multi-step build from source |
| **Debugging** | Go stack traces + simple C calls | Mixed Go/C++ stack traces |
| **Memory safety** | Go + thin C layer | Complex CGO lifecycle (Destroy, Free, Close ordering) |

## 4. Future Architecture Viability

### Remote Storage (GCS / S3)

Both formats are append-only and immutable after creation, making them candidates for
remote object storage.

**Packfile advantages for remote storage:**

- **Single-file format.** Each eventstore or bitmap index is one file. Upload/download is a
  single object PUT/GET. No directory structure to manage.
- **Byte-range reads.** The offset array (loaded at open) gives exact byte positions for any
  event. A point read is a single HTTP Range request. A scattered read of N events is N
  independent Range requests (parallelizable).
- **Trailer-first open.** Reading the last 256KB (speculative read) loads the index + trailer.
  One Range request to open, then O(1) lookups.
- **Predictable I/O.** Each block is self-contained. No internal dependencies between blocks.
  Every read translates to exactly one byte range.

**RocksDB challenges for remote storage:**

- **Multi-file format.** A RocksDB database is a directory containing MANIFEST, SST files,
  OPTIONS, CURRENT, and potentially WAL files. Even a single-SST ingested database has 5+
  files. Requires either (a) tar/zip packaging (adds open latency), or (b) a virtual
  filesystem layer.
- **No native remote support.** RocksDB's `Env` abstraction theoretically supports custom
  filesystems, but grocksdb doesn't expose custom Env implementations from Go. Would require
  writing C++ Env adapters and additional CGO wrappers.
- **Internal block index traversal.** A single Lookup/Get may require reading the block index,
  then the data block — two sequential reads. Can't be parallelized because the second depends
  on the first.
- **`BatchedMultiGetCF` assumes local I/O.** The async I/O optimization (`SetAsyncIO`) uses
  io_uring / readahead for local files. On remote storage, each internal I/O becomes a network
  round-trip. The performance model changes fundamentally.

**Viability assessment:**

| Aspect | Packfile | RocksDB |
|--------|----------|---------|
| Remote read latency (point) | 1 Range request | 2+ Range requests |
| Remote read latency (batch) | 1 Range request per block | Iterator requires sequential block access |
| Open latency | 1 Range request (tail) | Download entire DB or build VFS layer |
| Upload complexity | Single PUT | Multi-file upload or archive |
| Cost (S3 GET pricing) | Predictable (1 request/read) | Higher (2+ requests/read, block index) |

**Verdict:** Packfile is significantly better suited for remote storage. The single-file format
with byte-addressable blocks maps directly to HTTP Range requests. A remote eventstore reader
could be built with ~100 lines wrapping an HTTP client. RocksDB would require either
downloading the entire database (defeating the purpose) or building a complex VFS layer with
caching — a substantial engineering effort with uncertain performance.

**Estimated latency: LookupKeys (15 bitmaps) on S3/GCS**

Assumes packfile-based bitmap index. S3 Standard / GCS first-byte latency ~100ms per Range
GET. S3 Express One Zone ~10ms.

The I/O pattern for LookupKeys (15 keys):
1. **Open phase** (one-time, amortizable across queries):
   - GET MPHF file (178KB): 1 request
   - GET packfile tail (256KB, loads offset array + trailer): 1 Range request
   - These are parallel → 1 round trip
2. **MPHF query**: hash 15 keys → 15 ranks → 15 batch indices (in-memory, ~0ms)
3. **Data reads**: Each batch index maps to one packfile record (via the offset array loaded
   at open). 15 keys hitting K distinct batches → K parallel Range GETs. Worst case K=15
   (all different batches), best case K=1 (all in same batch). Typical: K=10-15 for random
   keys across 10,852 unique entries with batch size 128 (~85 batches total).
4. **Deserialize**: parse batch record + extract roaring bitmaps (in-memory, ~0ms)

| Scenario | S3 Standard | S3 Express | GCS |
|----------|-------------|------------|-----|
| Cold (include open) | ~200ms (2 round trips) | ~20ms | ~150ms |
| Warm (index cached) | ~100ms (1 round trip) | ~10ms | ~75ms |

With index caching (MPHF + offset array in memory, ~434KB total), every LookupKeys call
is a single round of parallel Range GETs — the minimum possible for any format. RocksDB
cannot match this: each `Get` requires reading the block index *then* the data block
(2 sequential round trips per lookup), and `BatchedMultiGetCF` doesn't expose byte-range
targeting for remote I/O.

**Estimated latency: ReadIndices (1000 scattered events) on S3/GCS**

Assumes packfile-based eventstore. The offset array (loaded at open) gives exact byte ranges
for every block. 1000 events scattered across B distinct blocks → B parallel Range GETs.

Worst case: 1000 events on 1000 different blocks (each event on a different 128-event block).
Realistic case for random access: most events land on distinct blocks.

Practical parallelism limit: an HTTP/2 connection to S3/GCS supports ~100-128 concurrent
streams. With HTTP/1.1 connection pooling, ~256 concurrent requests is achievable.

| Scenario | Blocks | S3 Standard (c=128) | S3 Express (c=128) | GCS (c=128) |
|----------|--------|---------------------|-----------------------|-------------|
| Cold (include open) | 1000 | ~900ms (1 open + 8 data rounds) | ~90ms | ~700ms |
| Warm (index cached) | 1000 | ~800ms (8 rounds × ~100ms) | ~80ms | ~600ms |
| Warm, clustered | 200 | ~200ms (2 rounds) | ~20ms | ~150ms |
| Warm, highly clustered | 50 | ~100ms (1 round) | ~10ms | ~75ms |

Breakdown for 1000 scattered blocks, S3 Standard, c=128:
- ceil(1000/128) = 8 rounds of parallel Range GETs
- Each round: ~100ms first-byte latency + ~6KB transfer (negligible)
- Total data phase: ~800ms

**Comparison with local storage (latency):**

| Storage | 1000 scattered reads | 15 bitmap lookups |
|---------|---------------------|-------------------|
| NVMe i4i (c=64) | 5.2ms | ~0.13ms (warm) |
| EBS gp3 baseline (c=64) | 332ms | ~5ms (cold) |
| EBS gp3 + 16K IOPS (c=64) | ~62ms | ~2ms (cold) |
| **S3 Standard (c=128)** | **~800ms** | **~100ms** |
| **S3 Express One Zone (c=128)** | **~80ms** | **~10ms** |
| **GCS Standard (c=128)** | **~600ms** | **~75ms** |

**Cost comparison (us-east-1 / us-central1, all prices as of early 2026):**

Assumes packfile-based format. Dataset: ~500MB (413MB eventstore + 53MB bitmap index).

*Storage costs:*

| Option | $/GB/month | 500MB dataset | Notes |
|--------|-----------|---------------|-------|
| S3 Standard | $0.023 | $0.01 | |
| S3 Express One Zone | $0.110 | $0.06 | Single AZ, after Apr 2025 31% reduction |
| GCS Standard | $0.020 | $0.01 | |
| GCS Rapid Storage | TBD | TBD | Preview only (announced Apr 2025, not yet GA) |
| EBS gp3 | $0.080 | $4.00 | 50GB minimum practical size |
| EBS io2 | $0.125 | $6.25 | 50GB minimum |
| NVMe (i4i.xlarge) | included | included | 937GB ephemeral, instance cost below |

*Request costs:*

| Option | GET cost | Per single read | Per 1K-read query | Per 15-bitmap query |
|--------|---------|----------------|-------------------|-------------------|
| S3 Standard | $0.0004/1K | $0.0000004 | $0.0004 | $0.000006 |
| S3 Express One Zone | $0.0011/1K | $0.0000011 | $0.0011 | $0.000017 |
| GCS Standard | $0.004/10K | $0.0000004 | $0.0004 | $0.000006 |
| GCS Rapid Storage | TBD | TBD | TBD | TBD |
| EBS gp3 / io2 / NVMe | N/A | N/A | N/A | N/A |

Request costs are negligible for this workload. At 1,000 queries/day (each doing 15 bitmap
lookups + 1000 event reads = ~1015 GETs), monthly request cost is:
- S3 Standard / GCS Standard: ~$12/month (30M GETs × $0.0004/1K)
- S3 Express One Zone: ~$33/month (30M GETs × $0.0011/1K)

EBS and NVMe have no per-request cost — you pay for provisioned IOPS or the instance.

**GCS low-latency equivalent:** Google announced **Rapid Storage** (zonal buckets with
sub-1ms latency, up to 6 TB/s throughput) at Cloud Next April 2025. It's the direct
competitor to S3 Express One Zone. However, as of February 2026 it remains in **preview**
with no public pricing. The table includes it as TBD. Once GA, expect pricing comparable
to S3 Express One Zone ($0.10-0.15/GB storage, similar per-request costs).

*IOPS costs (EBS only):*

| Option | Provisioned IOPS | $/IOPS/month | Monthly IOPS cost |
|--------|-----------------|-------------|-------------------|
| EBS gp3 baseline | 3,000 (free) | $0 | $0 |
| EBS gp3 + 16K IOPS | +13,000 | $0.005 | $65 |
| EBS io2 40K IOPS | 40,000 | $0.065 | $2,600 |

*Compute costs (EC2 instance to run the query service):*

| Option | Instance | $/month | Notes |
|--------|----------|---------|-------|
| S3/GCS backend | t3.medium | ~$30 | Minimal compute, storage is remote |
| EBS backend | t3.medium | ~$30 | Same instance, storage attached |
| NVMe backend | i4i.xlarge | ~$250 | Must use storage-optimized instance |

*Total monthly cost (storage + requests + compute):*

| Option | $/month | Latency (1K reads) | Latency (15 bitmaps) |
|--------|---------|-------------------|---------------------|
| S3 Standard | ~$42 | ~800ms | ~100ms |
| S3 Express One Zone | ~$63 | ~80ms | ~10ms |
| GCS Standard | ~$42 | ~600ms | ~75ms |
| GCS Rapid Storage | TBD | ~10ms (est.) | ~1ms (est.) |
| EBS gp3 baseline | ~$34 | 332ms | ~5ms |
| EBS gp3 + 16K IOPS | ~$99 | ~62ms | ~2ms |
| EBS io2 40K IOPS | ~$2,636 | ~25ms | ~1ms |
| NVMe i4i.xlarge | ~$250 | 5.2ms | ~0.13ms |

Key observations:
- **Request costs are trivial** — even at 30M GETs/month, S3/GCS request fees are $12-33.
  The cost difference between storage options is dominated by storage pricing, provisioned
  IOPS, or instance costs — not per-request charges.
- **S3 Express One Zone ($63/month, ~80ms) beats EBS gp3 baseline ($34/month, 332ms)** for
  scattered reads. For ~2x the cost, you get ~4x better latency. This is because S3 has no
  IOPS limit — you can issue 1000 parallel Range GETs in ~8 rounds, while EBS gp3 at 3K IOPS
  must serialize 1000 random I/Os.
- **EBS gp3 + 16K IOPS ($99/month, ~62ms)** is comparable to S3 Express for scattered reads,
  but better for bitmap lookups (~2ms vs ~10ms) due to lower single-request latency.
- **NVMe ($250/month, 5.2ms)** is 15x faster than S3 Express but ~4x the cost. Worth it for
  latency-sensitive workloads. Storage is ephemeral — data must be regenerable or replicated.
- **io2 ($2,636/month)** is never cost-effective vs NVMe i4i ($250/month) for this workload.
  Only justified when you need both high IOPS and durable block storage without replication.
- **S3 Standard and GCS Standard (~$42/month)** are the cheapest options. Acceptable for
  batch/async workloads where 800ms latency is tolerable.
- **GCS Rapid Storage** will likely be the GCS equivalent of S3 Express when it reaches GA.
  Sub-1ms latency with 20M QPS is more aggressive than S3 Express's single-digit-ms claim.

The key insight: packfile's offset array turns every read into an independently addressable
byte range. With sufficient parallelism, remote latency is dominated by a single round-trip
time × ceil(requests / concurrency). No sequential dependencies, no block-index traversal,
no multi-step lookups. This is why S3 Express can beat EBS — the parallelism ceiling is
much higher for object storage than for block storage IOPS.

**Ingestion cost for full Stellar history on S3/GCS:**

The benchmark dataset covers 10,000 ledgers with 8.7M events (~1.9GB raw → 413MB compressed
packfile). Full Stellar history is ~61.4M ledgers. Event density varies across history —
Soroban smart contracts launched at a specific protocol upgrade, so earlier ledgers have
zero contract events. The table below parametrizes by total events:

Assumes one packfile per ~10K-ledger chunk (matching benchmark dataset granularity). Each
chunk produces one eventstore packfile + one bitmap index (2 objects per chunk). Full history
at 61.4M ledgers ≈ 6,139 chunks.

| Total events | Raw size | Packfile size (4.6x) | Bitmap size (est.) | Chunks | S3 objects |
|-------------|---------|---------------------|-------------------|--------|-----------|
| 100M | 22 GB | 4.8 GB | 0.6 GB | ~700 | ~1,400 |
| 1B | 220 GB | 48 GB | 6 GB | ~6,100 | ~12,200 |
| 10B | 2.2 TB | 478 GB | 60 GB | ~6,100 | ~12,200 |
| 50B | 11 TB | 2.4 TB | 300 GB | ~6,100 | ~12,200 |

*One-time upload (PUT) cost:*

| Scenario | Objects | S3 Standard PUT | S3 Express PUT | GCS Class A |
|----------|---------|----------------|---------------|-------------|
| 1B events | ~12,200 | $0.06 | $0.03 | $0.12 |
| 10B events | ~12,200 | $0.06 | $0.03 | $0.12 |
| 50B events | ~12,200 | $0.06 | $0.03 | $0.12 |

PUT request costs are negligible (under $1) regardless of dataset size — the number of
objects is fixed by the chunking strategy (~6,100 chunks × 2 files), not by event count.

*Monthly storage cost:*

| Total events | Packfile + bitmap | S3 Standard | S3 Express | GCS Standard |
|-------------|------------------|-------------|------------|--------------|
| 100M | 5.4 GB | $0.12 | $0.59 | $0.11 |
| 1B | 54 GB | $1.24 | $5.94 | $1.08 |
| 10B | 538 GB | $12.37 | $59.18 | $10.76 |
| 50B | 2.7 TB | $62 | $297 | $54 |

*Data transfer cost (upload):*

S3: free for uploads (PUT data transfer is free). GCS: free for uploads to same region.
Cross-region or internet egress adds cost but is a one-time operation.

*Total one-time ingestion cost* is dominated by compute time to build the packfiles, not
by S3/GCS charges. Building one 10K-ledger chunk takes ~3.5s on NVMe (from write throughput
benchmarks: 2.8 GB/s at c=8). Building all 6,139 chunks sequentially: ~6 hours. With
parallel chunk building across cores, a 32-core instance could finish in ~15 minutes.
The EC2 cost for this is ~$0.50-2.00 (minutes of i4i.xlarge time).

### Content-Addressable Storage

Content-addressable storage (CAS) stores objects by their hash (e.g., SHA-256). This enables
deduplication, integrity verification, and immutable references.

**The zstd reproducibility problem (affects both formats):**

Neither format produces bitwise-reproducible output across zstd library versions. Zstd does
not guarantee that the same input at the same compression level produces identical compressed
bytes across versions — the encoder may change internal heuristics, table sizes, or match
strategies. The decompressed content is always identical, but the on-disk compressed bytes
(and thus any whole-file content hash) can differ.

This means rebuilding the same packfile or SST with a different libzstd version produces a
**different file hash**, even though the logical content is identical. The project already
acknowledges this: `fixtureFormatVersion` is bumped when format-affecting changes occur
(including RocksDB option changes that alter SST structure), invalidating cached fixtures.

**Implications for CAS:**

- **Whole-file CAS is fragile for both formats.** Any change to the compression library,
  compression level, or format options invalidates hashes. This rules out naive
  "hash the file, store by hash" approaches if files might be regenerated by different
  build environments.
- **Logical-content CAS is possible but requires extra work.** You'd hash the uncompressed
  logical content (e.g., SHA-256 of the sorted event stream) rather than the on-disk bytes.
  Both formats could support this by computing a content hash during writes and storing it
  in metadata. The packfile metadata section stores this; RocksDB could store it in a properties
  block.
- **Event-level CAS** (hash per event) sidesteps the compression problem entirely — individual
  events are hashed before compression. But this requires an external index mapping
  event_hash → (file, offset), which is a significant additional component.

**Packfile structural advantages for CAS (independent of the zstd problem):**

- Immutable single-file format — once sealed, never modified.
- The trailer already contains CRC32C checksums; adding a content hash is natural.
- Simple to enumerate all logical content for hash computation (sequential scan).

**RocksDB structural disadvantages for CAS:**

- Multi-file mutable directory. Even ingested-SST databases have MANIFEST, CURRENT, OPTIONS
  files that change across RocksDB versions.
- Content-addressing a database would require hashing a canonical representation (sorted
  key-value dump), not the raw files (which vary by RocksDB version and internal state).
- The SST file itself is closer to content-addressable, but RocksDB doesn't expose SST-level
  hashing.

**Verdict:** Neither format gives you free whole-file CAS due to zstd non-reproducibility.
The workaround (logical content hash) works identically for both — compute SHA-256 over
uncompressed logical content during writes. Packfile's immutable single-file design makes
storing and retrieving the hash slightly simpler (metadata field vs RocksDB properties key), but the
fundamental CAS capability is equivalent.

**Implemented: logical content hash via SHA-256 (packfile format):**

Both eventstore and bitmapindex support opt-in SHA-256 content hashing, enabled via
`ContentHash: true` in writer options. The hash is computed incrementally during writes
and stored in the packfile metadata (32 bytes after the standard 12-byte header).

Both use a shared chunked hash scheme (packfile's internal `ContentHasher`). Entries are
length-prefixed and grouped into chunks aligned with record boundaries (RecordSize for
eventstore, BatchSize for bitmapindex). Each chunk produces a SHA-256 digest; the
final hash is SHA-256 of the concatenated chunk digests:

```
chunkDigest_i = SHA-256([4B len][entry_{i*K}] ... [4B len][entry_{i*K+K-1}])
finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
K = RecordSize (BatchSize for bitmapindex)
```

The hash depends on record size (chunk boundaries), entry order, and entry content.
Same events with the same record size in the same order = same hash. The hash is
independent of compression and format version. For concurrent writes, per-worker
hash goroutines compute chunk digests in parallel with zstd compression.

For bitmap index: entries are hashed in rank order (0, 1, ..., N-1). Each entry
is `[fingerprint || bitmap]`, hashed as one length-prefixed unit.

SHA-256 was chosen for cross-platform hardware acceleration: Go's `crypto/sha256`
uses ARM64 SHA2 instructions (~2.4 GB/s on Apple M-series / AWS Graviton) and
x86 SHA-NI (~2-4 GB/s on Intel Ice Lake+ / AMD Zen+). No external dependency needed.

Verification via `Verify(ctx)` recomputes the hash by streaming all content and
comparing to the stored hash. The `ContentHash()` method exposes the stored hash
for CAS identity without requiring a full verification scan.

For RocksDB, the same approach would work: compute the hash during `SSTFileWriter.Add()`
calls and store it in a dedicated metadata key or SST properties block.

### Streaming / Append-Only Architectures

If the architecture evolves toward streaming ingestion (e.g., events arriving in real-time
from a Stellar node):

**Packfile:** Naturally append-only. A writer can buffer incoming events and periodically
seal a packfile. Each sealed packfile is immutable. Reads can happen concurrently on sealed
files while a new one is being written. This maps well to a log-structured design.

**RocksDB:** Also append-only at the SST level (each ingested SST is immutable), but the
surrounding RocksDB machinery (MANIFEST updates, potential compaction) adds complexity.
However, RocksDB offers built-in merge semantics, range deletions, and TTL — features
that would need to be built on top of packfiles if needed.

### Tiered Storage Architecture

A tiered architecture keeps recent data on fast local storage and archives older data to
cheaper, slower storage:

```
Hot tier (NVMe)  →  Warm tier (EBS gp3)  →  Cold tier (S3/GCS)
  recent chunks       older chunks            archive
  sub-ms reads        single-digit-ms reads   100ms+ reads
  ephemeral           durable                 durable
```

**How it works:**

Each ~10K-ledger chunk produces one eventstore packfile + one bitmap index (two immutable
files). Once sealed, a chunk never changes. This immutability makes tiering straightforward:

1. **Ingest** to NVMe: build packfile + bitmap on local NVMe (~0.69s per chunk at c=8).
   Keep the most recent N chunks on NVMe for instant access.
2. **Promote to EBS**: after a chunk ages past the hot window, copy it to EBS gp3. The copy
   is a simple file transfer (~466MB total per chunk, ~2s at EBS write throughput). NVMe copy
   can be deleted after EBS confirms.
3. **Archive to S3/GCS**: after a chunk ages past the warm window, upload to object storage
   (single PUT per file). EBS copy can be deleted after upload confirms.
4. **Read path**: check NVMe first, then EBS, then S3/GCS. The lookup is trivial — chunk
   boundaries are deterministic (ledger ranges), so the tier is known from the ledger number
   without any index.

**Sizing the tiers:**

| Tier | Purpose | Retention | Disk size | Cost/month |
|------|---------|-----------|-----------|------------|
| NVMe (i4i.xlarge) | Hot: last 24h of chunks | ~144 chunks (1 chunk/10min) | ~65 GB | included in $250 instance |
| EBS gp3 | Warm: last 30 days | ~4,320 chunks | ~2 TB | ~$160 (storage) |
| S3 Standard | Cold: full history | ~6,139 chunks | ~2.7 TB | ~$62 (storage) |

NVMe sizing assumes Stellar produces ~1 chunk per 10 minutes (10K ledgers × ~6s/ledger).
Actual rate depends on ledger close time. The 937GB NVMe on i4i.xlarge can hold ~2,000
chunks (~14 days), far more than needed for a 24h hot window.

**Latency by tier:**

| Operation | NVMe (hot) | EBS gp3 (warm) | S3 Standard (cold) | S3 Express (cold) |
|-----------|-----------|----------------|--------------------|--------------------|
| 15 bitmap lookups | 0.13 ms | ~5 ms | ~100 ms | ~10 ms |
| 1,000 scattered reads | 5.2 ms | 332 ms | ~800 ms | ~80 ms |
| Point read | 0.012 ms | ~0.3 ms | ~100 ms | ~10 ms |

**Why this works well with packfile:**

- **Immutable files**: no coordination needed between tiers. A chunk is either fully present
  on a tier or not. No partial states, no locks, no distributed transactions.
- **Deterministic placement**: ledger number → chunk number → tier. No metadata service
  needed to locate data.
- **Single-file format**: each tier transition is a file copy (NVMe→EBS) or PUT (EBS→S3).
  RocksDB's multi-file directories would require tar/zip packaging or per-file uploads.
- **Byte-range reads on S3**: cold tier reads don't require downloading the entire packfile.
  The offset array (loaded once, cached) enables surgical Range GETs for specific events.
- **No replication for hot tier**: NVMe is ephemeral, but chunks can be rebuilt from source
  data or re-downloaded from S3. The warm/cold tiers provide durability.

**Cost comparison: tiered vs single-tier:**

| Architecture | Monthly cost | Worst-case read latency | Notes |
|-------------|-------------|------------------------|-------|
| NVMe only (i4i.xlarge) | $250 | 5.2 ms | All data on NVMe; ephemeral, must fit on disk |
| EBS gp3 only (50GB) | $34 | 332 ms | IOPS-limited; cheap but slow |
| EBS gp3 + 16K IOPS (2TB) | $225 | ~62 ms | Durable, mid-latency |
| S3 Standard only | $62 | ~800 ms | Cheapest for full history; high latency |
| **Tiered (NVMe + EBS + S3)** | **$472** | **5.2 ms (hot), 332 ms (warm), 800 ms (cold)** | **Best of all worlds** |
| **Tiered (NVMe + S3 Express)** | **$362** | **5.2 ms (hot), 80 ms (cold)** | **Skip EBS; S3 Express for warm+cold** |

The tiered approach costs more than any single tier ($472 vs $250 for NVMe-only) but
provides what no single tier can: sub-ms latency for recent data AND durable storage for
full history. The NVMe + S3 Express variant ($362) is particularly attractive — it
eliminates the EBS warm tier entirely since S3 Express at ~80ms may be fast enough for
non-recent queries, saving operational complexity (one fewer tier to manage).

**When tiering is worth it:**

- **Yes** if the workload has strong temporal locality (recent data queried much more
  frequently than old data). This is typical for blockchain analytics — recent ledgers
  are queried for monitoring, alerting, and real-time dashboards; historical data is
  accessed infrequently for backfills or audits.
- **Yes** if full history must be retained but the hot working set fits on NVMe (~65GB
  for 24h is easily within i4i.xlarge's 937GB).
- **No** if access is uniformly random across all history (every tier gets hit equally,
  negating the latency benefit of the hot tier).
- **No** if the total dataset fits on NVMe (< 937GB) — just keep everything local.

**Implementation complexity:**

The tiering logic is simple because chunks are immutable and placement is deterministic:

```go
func tierFor(ledgerSeq uint32) Tier {
    chunkAge := currentLedger - chunkEndLedger(ledgerSeq)
    switch {
    case chunkAge < hotWindow:   return NVMe
    case chunkAge < warmWindow:  return EBS    // optional, skip for NVMe+S3 Express
    default:                     return S3
    }
}
```

A background goroutine periodically promotes chunks down the tiers. The read path wraps
the `StoreReader` / `IndexReader` interface — the caller doesn't know which tier served
the data. For S3, the reader uses HTTP Range GETs backed by the same offset array.

Total additional code: ~200-300 lines for the tier manager + ~100 lines for a remote
(S3/GCS) `StoreReader` implementation. The `IndexReader` remote implementation is similar
in size.

### Summary: When to Use Each

| Use Case | Recommended |
|----------|-------------|
| Maximum read throughput (local disk) | Packfile |
| Maximum write throughput | Packfile |
| Remote storage (S3/GCS) | Packfile |
| Tiered storage (NVMe → EBS → S3) | Packfile (immutable single files, deterministic placement) |
| Content-addressable archives | Either (logical hash workaround is format-neutral) |
| Minimum implementation complexity | RocksDB |
| Key-based lookups (non-positional) | RocksDB |
| Mutable data (updates, deletes) | RocksDB |
| Range queries on arbitrary keys | RocksDB |
| Rapid prototyping / iteration | RocksDB |
| Cross-language access (C, C++, Java, Python) | RocksDB |

## 5. Overall Assessment

The packfile format is the clear winner for this project's specific workload: immutable,
append-only event storage with positional access patterns on local or remote storage. It
delivers 1.2-5.9x better read throughput, 3.0x better write throughput (NVMe), 1.09x better
write throughput (EBS), and smaller files, with a simpler deployment story (no RocksDB
build dependency). RocksDB uses less peak write memory (29 vs 55 MB).

The cost is ~3x more implementation code and a custom binary format that requires careful
testing. This cost has already been paid — the format is implemented, well-tested, and
stable.

RocksDB's value in this project is primarily as a **comparison baseline** — a well-known,
general-purpose format that validates the packfile's design decisions. It also serves as a
**fallback option** for workloads that don't fit packfile's strengths (key-based lookups,
mutable data). For the bitmap index specifically, RocksDB is a reasonable alternative when
the MPHF's build-time cost (BBHash construction) or the two-file format (hash + data) is
undesirable.

For future architectures involving remote or tiered storage, the packfile format has a
significant structural advantage. Its immutable single-file format with byte-addressable
blocks maps directly to HTTP Range requests (remote) and trivial file-copy promotion
between tiers. A tiered NVMe → S3 Express architecture ($362/month) provides sub-ms
latency for recent data and ~80ms for historical queries — a compelling combination for
blockchain analytics workloads with strong temporal locality. RocksDB would require either
full file download or a complex VFS layer for remote access, and its multi-file directory
structure complicates tier transitions. For content-addressable storage, the logical content
hash workaround works equally well for both formats.
