package compaction

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/staf3333/teakdb/memtable"
	"github.com/staf3333/teakdb/sstable"
)

func writeTestSSTable(t *testing.T, dir string, name string, pairs []memtable.KeyValuePair) sstable.SSTable {
	t.Helper()
	path := filepath.Join(dir, name)
	err := sstable.WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}
	sst, err := sstable.OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}
	return *sst
}

func TestCompactMergesTwoSSTables(t *testing.T) {
	dir := t.TempDir()

	sst1 := writeTestSSTable(t, dir, "sst1.sst", []memtable.KeyValuePair{
		{Key: "apple", Value: "red"},
		{Key: "cherry", Value: "dark red"},
	})
	sst2 := writeTestSSTable(t, dir, "sst2.sst", []memtable.KeyValuePair{
		{Key: "banana", Value: "yellow"},
		{Key: "date", Value: "brown"},
	})

	outputPath := filepath.Join(dir, "compacted.sst")
	err := Compact([]sstable.SSTable{sst1, sst2}, outputPath)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	result, err := sstable.OpenSSTable(outputPath)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	// all 4 keys should be present in sorted order
	expected := []struct{ key, val string }{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
		{"date", "brown"},
	}
	for _, e := range expected {
		val, found, err := result.Search(e.key)
		if err != nil || !found || val != e.val {
			t.Errorf("Search(%q) = (%q, %v, %v), want (%q, true, nil)", e.key, val, found, err, e.val)
		}
	}
}

func TestCompactDeduplicatesKeys(t *testing.T) {
	dir := t.TempDir()

	// sst1 is "newer" (index 0), sst2 is "older" (index 1)
	sst1 := writeTestSSTable(t, dir, "sst1.sst", []memtable.KeyValuePair{
		{Key: "color", Value: "green"},
		{Key: "name", Value: "teakdb"},
	})
	sst2 := writeTestSSTable(t, dir, "sst2.sst", []memtable.KeyValuePair{
		{Key: "color", Value: "blue"},
		{Key: "name", Value: "oldname"},
	})

	outputPath := filepath.Join(dir, "compacted.sst")
	err := Compact([]sstable.SSTable{sst1, sst2}, outputPath)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	result, err := sstable.OpenSSTable(outputPath)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	// newer values should win
	val, found, err := result.Search("color")
	if err != nil || !found || val != "green" {
		t.Errorf("Search(color) = (%q, %v, %v), want (\"green\", true, nil)", val, found, err)
	}

	val, found, err = result.Search("name")
	if err != nil || !found || val != "teakdb" {
		t.Errorf("Search(name) = (%q, %v, %v), want (\"teakdb\", true, nil)", val, found, err)
	}
}

func TestCompactManyTables(t *testing.T) {
	dir := t.TempDir()

	// create 5 SSTables with some overlapping keys
	var tables []sstable.SSTable
	for i := 0; i < 5; i++ {
		pairs := []memtable.KeyValuePair{
			{Key: fmt.Sprintf("key-%03d", i*10), Value: fmt.Sprintf("val-%d-from-sst%d", i*10, i)},
			{Key: fmt.Sprintf("key-%03d", i*10+1), Value: fmt.Sprintf("val-%d-from-sst%d", i*10+1, i)},
			{Key: "shared-key", Value: fmt.Sprintf("value-from-sst%d", i)},
		}
		sst := writeTestSSTable(t, dir, fmt.Sprintf("sst%d.sst", i), pairs)
		tables = append(tables, sst)
	}

	outputPath := filepath.Join(dir, "compacted.sst")
	err := Compact(tables, outputPath)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	result, err := sstable.OpenSSTable(outputPath)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	// shared-key should have value from sst0 (newest = index 0)
	val, found, err := result.Search("shared-key")
	if err != nil || !found || val != "value-from-sst0" {
		t.Errorf("Search(shared-key) = (%q, %v, %v), want (\"value-from-sst0\", true, nil)", val, found, err)
	}

	// unique keys should all be present
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%03d", i*10)
		val, found, err := result.Search(key)
		if err != nil || !found {
			t.Errorf("Search(%q) = (%q, %v, %v), expected found", key, val, found, err)
		}
	}
}

func TestCompactPreservesSortOrder(t *testing.T) {
	dir := t.TempDir()

	sst1 := writeTestSSTable(t, dir, "sst1.sst", []memtable.KeyValuePair{
		{Key: "delta", Value: "4"},
		{Key: "foxtrot", Value: "6"},
	})
	sst2 := writeTestSSTable(t, dir, "sst2.sst", []memtable.KeyValuePair{
		{Key: "alpha", Value: "1"},
		{Key: "echo", Value: "5"},
	})
	sst3 := writeTestSSTable(t, dir, "sst3.sst", []memtable.KeyValuePair{
		{Key: "bravo", Value: "2"},
		{Key: "charlie", Value: "3"},
	})

	outputPath := filepath.Join(dir, "compacted.sst")
	err := Compact([]sstable.SSTable{sst1, sst2, sst3}, outputPath)
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	result, err := sstable.OpenSSTable(outputPath)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	pairs, err := result.GetPairs()
	if err != nil {
		t.Fatalf("GetPairs failed: %v", err)
	}

	expectedOrder := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}
	if len(pairs) != len(expectedOrder) {
		t.Fatalf("got %d pairs, want %d", len(pairs), len(expectedOrder))
	}
	for i, key := range expectedOrder {
		if pairs[i].Key != key {
			t.Errorf("pairs[%d].Key = %q, want %q", i, pairs[i].Key, key)
		}
	}
}
