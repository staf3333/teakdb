# TeakDB 🪵

A write-optimized key-value store built from scratch in Go, powered by an LSM tree — memtable, SSTables, compaction, and bloom filters.

**DDIA Chapter 3** — the write-optimized counterpart to the [simple OLAP DB](../simple-olap-db).

## Key Concepts

- **Write-ahead log (WAL)** — durability before data hits the memtable
- **Memtable** — in-memory sorted structure (red-black tree, skip list, or sorted array)
- **SSTables** — immutable, sorted on-disk files flushed from the memtable
- **Compaction** — merging SSTables to reclaim space and keep reads fast
- **Bloom filters** — probabilistic check to avoid unnecessary disk reads

## Project Roadmap

1. **Memtable** — sorted in-memory writes with a size threshold
2. **SSTable flush** — serialize the memtable to a sorted on-disk file
3. **Read path** — check memtable → SSTables (newest first)
4. **WAL** — crash recovery by replaying the log
5. **Compaction** — merge SSTables (start with size-tiered)
6. **Bloom filters** — skip SSTables that definitely don't contain a key
7. **Benchmarks** — compare random writes, sequential reads, point lookups

## Progress

### Phase 1: Red-Black Tree (Memtable internals)
- [x] Define node and tree structs
- [x] Implement left-rotate and right-rotate
- [x] Implement insert (BST insert + color red)
- [x] Implement insert-fixup (recoloring/rotation cases)
- [x] Implement search
- [x] Implement in-order traversal
- [x] Tests for all operations

### Phase 2: Memtable
- [x] Wrap red-black tree with memtable API (Put, Get)
- [x] Size tracking and flush threshold

### Phase 3: SSTable Flush
- [x] Define SSTable on-disk format (length-prefixed entries + embedded index)
- [x] Serialize memtable to sorted on-disk file
- [x] Read SSTable: load index, binary search, read value from disk
- [x] Tests for write → read roundtrip

### Phase 4: Read Path
- [x] Check memtable → SSTables (newest first)
- [x] DB engine with Put/Get API
- [x] Load existing SSTables on startup

### Phase 5: WAL
- [x] Write-ahead log for crash recovery
- [x] Replay log on startup
- [x] Reset WAL on memtable flush

### Phase 6: Compaction
- [x] Merge SSTables (size-tiered)
- [x] K-way merge with min heap
- [x] Deduplicate keys (newest wins)

### Phase 7: Bloom Filters
- [x] Probabilistic check to skip SSTables
- [x] ~1% false positive rate with double hashing

### Phase 8: Benchmarks
- [x] Random writes, sequential reads, point lookups
- [x] Bloom filter impact comparison
- [x] Mixed read/write workload

## Getting Started

```bash
go test ./...                          # run all tests
go test -bench=. -benchmem -run=^$ .   # run benchmarks
```

## Benchmark Results (Apple M1 Pro)

| Benchmark | ops/sec | Latency | Notes |
|---|---|---|---|
| Random Writes | ~10,600/s | ~94μs | WAL `O_SYNC` is the bottleneck |
| Sequential Writes | ~10,800/s | ~92μs | Similar — write speed is I/O-bound |
| Memtable Lookup | ~10,000,000/s | ~154ns | In-memory, no disk I/O |
| SSTable Lookup | ~68,000/s | ~14.5μs | Binary search + disk read |
| Miss (Bloom Filter) | ~9,700,000/s | ~103ns | Bloom filter rejects instantly |
| Miss (No Bloom) | ~18,000,000/s | ~56ns | Still fast, but does binary search |
| Bloom vs No Bloom | **2.8x faster** | 20ns vs 56ns | Zero allocations with bloom |

## How TeakDB Compares to Production Databases

TeakDB uses the **same architecture** as production LSM-tree databases like RocksDB, LevelDB, and Cassandra's storage engine. The core pipeline is identical: WAL → Memtable → SSTable → Compaction, with bloom filters to accelerate reads.

The performance gap (~50-80x on writes, ~3-5x on reads) comes from engineering optimizations, not architectural differences:

### Where Production DBs Are Faster

| Area | TeakDB | Production (RocksDB, etc.) | Why It Matters |
|---|---|---|---|
| **WAL writes** | `O_SYNC` on every write | Group commit — batch multiple writes into one `fsync` | Amortizes the expensive disk sync across many operations |
| **SSTable format** | Simple length-prefixed entries | Block-based format with compression (Snappy, LZ4, Zstd) | Smaller files = less I/O, more data fits in cache |
| **File I/O** | `os.Open` + `Seek` per read | Memory-mapped files (`mmap`) + block cache | Avoids syscall overhead, OS manages caching |
| **Memtable** | Red-black tree | Skip list (lock-free) | Supports concurrent reads/writes without mutexes |
| **Compaction** | Simple "merge all" size-tiered | Leveled compaction with background threads | Controls file sizes, better read amplification |
| **Index** | Full index in memory | Sparse index + block index with prefix compression | Uses far less memory for large datasets |
| **Bloom filter** | Rebuilt on load from index | Persisted in SSTable file | No rebuild cost on startup |
| **Concurrency** | Single-threaded | Lock-free memtable, concurrent compaction, readers/writers | Scales across CPU cores |

### What I Learned

- **Why LSM trees are write-optimized**: sequential disk writes (WAL append + SSTable flush) are orders of magnitude faster than random writes. The memtable absorbs writes in memory and batches them into sorted disk flushes.
- **The read-write tradeoff**: fast writes come at the cost of read amplification — a key might live in the memtable, any SSTable, or nowhere. Bloom filters and sorted indexes mitigate this.
- **Durability vs performance**: `O_SYNC` on every WAL write guarantees no data loss but is the #1 bottleneck. Production databases batch writes (group commit) to get both durability and speed.
- **Why compaction matters**: without it, reads degrade linearly as SSTables accumulate. Compaction merges files, removes stale keys, and keeps the read path fast.
- **Bloom filters are a cheat code**: a few bytes of memory per key can eliminate 99% of unnecessary disk reads. The 2.8x speedup on misses would be even more dramatic with slower storage.
- **Red-black trees from scratch**: implementing insert-fixup with rotations and recoloring taught me more about balanced BSTs than any textbook. The sentinel node trick eliminates nil checks everywhere.

## Language

Go 🐹
