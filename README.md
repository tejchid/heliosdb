# HeliosDB

A crash-consistent LSM-tree key-value storage engine written in C++20.

Built from scratch to understand how production storage engines like LevelDB and RocksDB work under the hood — specifically around durability, compaction, and read performance at scale.

## Architecture

```
Write Path:  client → MemTable (SkipList) → WAL → SSTable flush → compaction
Read Path:   MemTable → Bloom filter check → SSTable (descriptor cache) → disk
```

**Write-Ahead Log (WAL)** — every write is checksummed and appended to the WAL before being applied to the MemTable. On crash, the WAL is replayed to reconstruct in-memory state.

**MemTable** — implemented as a SkipList using `std::atomic` for concurrent reads during flush. Once the MemTable hits the size threshold it is flushed to an immutable SSTable on disk.

**SSTables** — sorted, immutable files written atomically via `rename`. Each SSTable has a Bloom filter to skip unnecessary disk reads on negative lookups.

**Descriptor Cache** — open file descriptors are cached per SSTable rather than reopened on every read. This was the single biggest read performance lever — eliminating per-lookup `open`/`close` overhead.

**Background Compaction** — a background thread merges SSTables to bound read amplification and reclaim space from deleted keys.

**Manifest** — tracks the current set of live SSTables. Written atomically so recovery always starts from a consistent view of the database.

## Performance

Benchmarked on MacBook Pro (x86_64, Apple Clang 14):

```
BM_WriteThroughput    78K writes/sec
BM_ReadThroughput     21K reads/sec
```

Before descriptor caching, read throughput was **207 ops/sec** — caching file descriptors produced a **100x improvement**.

Recovery correctness validated across 15K+ operations using AddressSanitizer and UBSan with zero errors detected.

## Build

**Requirements:** macOS with Xcode Command Line Tools, CMake 3.20+, Google Benchmark

```bash
git clone https://github.com/tejchid/heliosdb
cd heliosdb
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(sysctl -n hw.logicalcpu)
```

## Run Benchmarks

```bash
cd build
./heliosdb_bench
```

Expected output is in `BENCHMARKS_ELITE.txt` at the repo root.

## Run Tests

```bash
cd build
./heliosdb_tests
```

To run with AddressSanitizer:

```bash
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined"
make -j$(sysctl -n hw.logicalcpu)
./heliosdb_tests
```

## Project Structure

```
heliosdb/
├── include/        # Public headers
├── src/            # Engine implementation
├── tests/          # Correctness tests
├── benchmarks/     # Google Benchmark harness
├── BENCHMARKS.txt          # Baseline benchmark results
└── BENCHMARKS_ELITE.txt    # Results after descriptor cache optimization
```

## Key Design Decisions

**Why SkipList for MemTable?** Lock-free concurrent reads during flush without blocking writers. Simpler than a red-black tree and cache-friendlier for sequential scans.

**Why `rename` for atomic SSTable writes?** `rename` is atomic on POSIX filesystems — a reader either sees the complete file or not at all. No partial writes visible to concurrent readers.

**Why Bloom filters?** Point lookups on missing keys would otherwise require scanning every SSTable on disk. Bloom filters reduce this to a single in-memory probabilistic check per SSTable.
