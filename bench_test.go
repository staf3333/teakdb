package lsmtree

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/staf3333/teakdb/memtable"
	"github.com/staf3333/teakdb/sstable"
)

// BenchmarkRandomWrites measures throughput of random key writes
func BenchmarkRandomWrites(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(1000000))
		val := fmt.Sprintf("value-%010d", i)
		err := db.Put(key, val)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkSequentialWrites measures throughput of sequential key writes
func BenchmarkSequentialWrites(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := fmt.Sprintf("value-%010d", i)
		err := db.Put(key, val)
		if err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// BenchmarkPointLookupMemtable measures reads that hit the memtable (best case)
func BenchmarkPointLookupMemtable(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}

	// write enough keys to stay in memtable
	for i := 0; i < 1000; i++ {
		db.Put(fmt.Sprintf("key-%04d", i), fmt.Sprintf("val-%04d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%04d", rand.Intn(1000))
		db.Get(key)
	}
}

// BenchmarkPointLookupSSTable measures reads that must check SSTables
func BenchmarkPointLookupSSTable(b *testing.B) {
	dir := b.TempDir()

	// write data directly to an SSTable
	var pairs []memtable.KeyValuePair
	for i := 0; i < 10000; i++ {
		pairs = append(pairs, memtable.KeyValuePair{
			Key:   fmt.Sprintf("key-%06d", i),
			Value: fmt.Sprintf("val-%06d", i),
		})
	}
	sstPath := filepath.Join(dir, "bench.sst")
	sstable.WriteSSTable(pairs, sstPath)

	sst, err := sstable.OpenSSTable(sstPath)
	if err != nil {
		b.Fatalf("OpenSSTable failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%06d", rand.Intn(10000))
		sst.Search(key)
	}
}

// BenchmarkPointLookupMiss measures read performance for keys that don't exist
func BenchmarkPointLookupMiss(b *testing.B) {
	dir := b.TempDir()

	var pairs []memtable.KeyValuePair
	for i := 0; i < 10000; i++ {
		pairs = append(pairs, memtable.KeyValuePair{
			Key:   fmt.Sprintf("key-%06d", i),
			Value: fmt.Sprintf("val-%06d", i),
		})
	}
	sstPath := filepath.Join(dir, "bench.sst")
	sstable.WriteSSTable(pairs, sstPath)

	sst, err := sstable.OpenSSTable(sstPath)
	if err != nil {
		b.Fatalf("OpenSSTable failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// these keys don't exist — bloom filter should short-circuit
		key := fmt.Sprintf("miss-%06d", rand.Intn(10000))
		sst.Search(key)
	}
}

// BenchmarkBloomFilterImpact compares search with and without bloom filter on missing keys
func BenchmarkBloomFilterImpact(b *testing.B) {
	dir := b.TempDir()

	var pairs []memtable.KeyValuePair
	for i := 0; i < 10000; i++ {
		pairs = append(pairs, memtable.KeyValuePair{
			Key:   fmt.Sprintf("key-%06d", i),
			Value: fmt.Sprintf("val-%06d", i),
		})
	}
	sstPath := filepath.Join(dir, "bench.sst")
	sstable.WriteSSTable(pairs, sstPath)

	sst, err := sstable.OpenSSTable(sstPath)
	if err != nil {
		b.Fatalf("OpenSSTable failed: %v", err)
	}

	// pre-generate missing keys
	missingKeys := make([]string, 10000)
	for i := range missingKeys {
		missingKeys[i] = fmt.Sprintf("miss-%06d", i)
	}

	b.Run("WithBloomFilter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sst.Search(missingKeys[i%len(missingKeys)])
		}
	})

	b.Run("WithoutBloomFilter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sst.SearchWithoutBloom(missingKeys[i%len(missingKeys)])
		}
	})
}

// BenchmarkFullDBWorkload simulates a mixed read/write workload
func BenchmarkFullDBWorkload(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		b.Fatalf("NewDB failed: %v", err)
	}

	// pre-populate
	for i := 0; i < 5000; i++ {
		db.Put(fmt.Sprintf("key-%06d", i), fmt.Sprintf("val-%06d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			// 33% writes
			db.Put(fmt.Sprintf("key-%06d", rand.Intn(10000)), fmt.Sprintf("val-%06d", i))
		} else {
			// 67% reads
			db.Get(fmt.Sprintf("key-%06d", rand.Intn(10000)))
		}
	}

	// print final stats
	b.StopTimer()
	entries, _ := os.ReadDir(dir)
	sstCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			sstCount++
		}
	}
	b.Logf("Final state: %d SSTable files", sstCount)
}
