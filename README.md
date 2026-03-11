# LSM-Tree Key-Value Store

A write-optimized storage engine built from scratch in Go — memtable, SSTables, compaction, and bloom filters.

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

## Getting Started

```bash
go run cmd/lsm/main.go
```

## Language

Go 🐹
