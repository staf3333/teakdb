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
- [ ] Random writes, sequential reads, point lookups

## Getting Started

```bash
go run cmd/lsm/main.go
```

## Language

Go 🐹
