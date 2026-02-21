# Event Analysis - Development Guide

## Project Overview

Benchmarking and analysis tool for event storage formats: custom packfile/eventstore vs RocksDB. Uses real Stellar ledger data (~8.7M events from 1.7GB source files).

## Prerequisites

### Go
- Go 1.26+ (specified in go.mod)

### RocksDB (required for benchmarks)

The `grocksdb v1.10.7` Go binding requires RocksDB headers and shared library at build time. The system `librocksdb-dev` package (Ubuntu 24.04) provides RocksDB 8.9.1 which is **incompatible** — the API changed between 8.x versions.

**Build RocksDB 10.9.1 from source:**

```bash
cd /tmp
git clone --depth 1 --branch v10.9.1 https://github.com/facebook/rocksdb.git
cd rocksdb
USE_ZSTD=1 USE_LZ4=1 USE_SNAPPY=1 USE_BZ2=1 PREFIX=$HOME/.local make install-shared -j$(nproc)
```

**Required system compression dev libraries (install via apt):**
```bash
sudo apt-get install -y libsnappy-dev liblz4-dev libzstd-dev libbz2-dev
```

**CGO flags for every `go test` / `go build` command:**
```bash
export CGO_CFLAGS="-I$HOME/.local/include"
export CGO_LDFLAGS="-L$HOME/.local/lib"
export LD_LIBRARY_PATH="$HOME/.local/lib:$LD_LIBRARY_PATH"
```

## Running Tests

Subpackage tests (fast, no RocksDB needed):
```bash
go test ./eventstore/... ./packfile/... ./recordcodec/...
```

Root package tests (need RocksDB + CGO flags, ~15 min total):
```bash
go test -v -count=1 -timeout 30m github.com/tamir/events-analysis
```

## Running Benchmarks

**Run each benchmark in a separate process** to avoid two sources of contamination:

1. **Page cache warming**: Go's test framework runs benchmarks alphabetically within a single process. Earlier benchmarks (e.g., `BenchmarkPackfile*`) warm the page cache for later ones (e.g., `BenchmarkRocksDB*`), inflating the later numbers.
2. **GOGC=1 contamination**: Memory benchmarks (`*WriteMemory*`) set `GOGC=1` to measure working set. This penalizes Go-side allocations ~10x while leaving C-side allocations (RocksDB) unaffected. Running throughput and memory benchmarks in the same process invalidates throughput comparisons.

```bash
# Correct: each benchmark in its own process
go test -bench='^BenchmarkPackfileWrite$' -benchmem -run='^$' -count=5 -timeout 30m
go test -bench='^BenchmarkRocksDBWrite$' -benchmem -run='^$' -count=5 -timeout 30m

# Wrong: all benchmarks in one process
go test -bench='.*' -benchmem -run='^$' -timeout 30m
```

Use `benchstat` for statistical comparison across runs.

### Write throughput (fast feedback)

`TestWriteThroughput` runs all write configurations in a single test without `GOGC=1` overhead. Use `WRITE_DIR` to target a specific device:

```bash
WRITE_DIR=/mnt/nvme go test -run=TestWriteThroughput -timeout 30m
WRITE_DIR=/tmp go test -run=TestWriteThroughput -timeout 30m   # EBS
```

### Measuring Peak Memory

Write benchmarks report `peak-delta-MB` — the peak anonymous RSS delta during the write operation, measured via `RssAnon` from `/proc/self/status` (excludes page cache, captures both Go heap and C allocations).

For accurate readings, run with `GODEBUG=madvdontneed=1` and **each benchmark in a separate process** to avoid cross-contamination:

```bash
# Compile once
go test -c -o /tmp/bench.test

# Run each separately
GODEBUG=madvdontneed=1 LD_LIBRARY_PATH="$HOME/.local/lib" \
  /tmp/bench.test -test.bench='BenchmarkPackfileWrite$' -test.benchmem -test.run='^$' -test.timeout 30m

GODEBUG=madvdontneed=1 LD_LIBRARY_PATH="$HOME/.local/lib" \
  /tmp/bench.test -test.bench='BenchmarkPackfileWriteParallel8$' -test.benchmem -test.run='^$' -test.timeout 30m
```

**Why `GODEBUG=madvdontneed=1`:** Go's default `MADV_FREE` lets the kernel lazily reclaim freed pages, which inflates RSS readings. `madvdontneed=1` forces immediate reclaim so the baseline is clean.

**Why separate processes:** Go's runtime never shrinks its virtual address space, and RocksDB's allocators have their own retention. Separate processes prevent one benchmark's memory footprint from leaking into the next.

**Why `RssAnon` instead of RSS:** RSS includes file-backed page cache from reading source data (~1.7GB) and writing output files (~425MB). `RssAnon` only counts anonymous pages (heap, stack, mmap anonymous) — the actual program working memory.

### Fixture Caching

Benchmark fixtures (eventstore and RocksDB files ~1.3GB total) are cached in `testdata/fixtures/`. First run generates them (~3-4 min); subsequent runs load from cache (~3s).

- Fixtures are invalidated automatically when source data files (`006016.data`, `006016.index`) change (mtime comparison).
- Force regeneration: `FORCE_REGEN=1 go test -bench=...`
- Fixture metadata is in `testdata/fixtures/meta.json`.

## Storage Layout (AWS)

This machine has three drives:
- `/` (root, 50GB EBS gp3) — OS + working directory (~149 MB/s write)
- `/mnt/xvdf` (100GB EBS) — secondary storage
- `/mnt/nvme` (1.7TB NVMe instance store) — fast local SSD

Benchmark fixtures are symlinked to NVMe for faster I/O:
```
testdata/fixtures/ -> /mnt/nvme/fixtures/
```

Note: NVMe instance storage is **ephemeral** — data is lost on instance stop/terminate. Fixtures can be regenerated with `FORCE_REGEN=1`.

If the NVMe drive is not mounted after a reboot:
```bash
sudo mkfs.ext4 /dev/nvme2n1  # only if not already formatted
sudo mkdir -p /mnt/nvme
sudo mount /dev/nvme2n1 /mnt/nvme
sudo chown tamir:staff /mnt/nvme
mkdir -p /mnt/nvme/fixtures
```

## Source Data

- `006016.index` + `006016.data` (~1.7GB) — raw Stellar ledger data with ~10K ledgers / ~8.7M events
- Not checked into git (listed in `.gitignore`)
