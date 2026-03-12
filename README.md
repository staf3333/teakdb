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
- [ ] Define node and tree structs
- [ ] Implement left-rotate and right-rotate
- [ ] Implement insert (BST insert + color red)
- [ ] Implement insert-fixup (recoloring/rotation cases)
- [ ] Implement search
- [ ] Implement in-order traversal
- [ ] Tests for all operations

### Phase 2: Memtable
- [ ] Wrap red-black tree with memtable API (Put, Get, Delete)
- [ ] Size tracking and flush threshold

### Phase 3: SSTable Flush
- [ ] Define SSTable on-disk format
- [ ] Serialize memtable to sorted on-disk file

### Phase 4: Read Path
- [ ] Check memtable → SSTables (newest first)

### Phase 5: WAL
- [ ] Write-ahead log for crash recovery
- [ ] Replay log on startup

### Phase 6: Compaction
- [ ] Merge SSTables (size-tiered)

### Phase 7: Bloom Filters
- [ ] Probabilistic check to skip SSTables

### Phase 8: Benchmarks
- [ ] Random writes, sequential reads, point lookups

## Getting Started

```bash
go run cmd/lsm/main.go
```

## Language

Go 🐹
