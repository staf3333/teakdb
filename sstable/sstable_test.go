package sstable

import (
	"os"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/staf3333/teakdb/memtable"
)

func tempFilePath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestWriteAndReadBasic(t *testing.T) {
	path := tempFilePath(t, "basic.sst")

	pairs := []memtable.KeyValuePair{
		{Key: "apple", Value: "red"},
		{Key: "banana", Value: "yellow"},
		{Key: "cherry", Value: "dark red"},
	}

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	for _, pair := range pairs {
		val, found, err := sst.Search(pair.Key)
		if err != nil {
			t.Fatalf("Search(%q) error: %v", pair.Key, err)
		}
		if !found {
			t.Errorf("Search(%q) not found, expected %q", pair.Key, pair.Value)
		}
		if val != pair.Value {
			t.Errorf("Search(%q) = %q, want %q", pair.Key, val, pair.Value)
		}
	}
}

func TestSearchKeyNotFound(t *testing.T) {
	path := tempFilePath(t, "notfound.sst")

	pairs := []memtable.KeyValuePair{
		{Key: "alpha", Value: "1"},
		{Key: "beta", Value: "2"},
	}

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	val, found, err := sst.Search("gamma")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if found {
		t.Errorf("Search(gamma) found with value %q, expected not found", val)
	}
}

func TestSingleEntry(t *testing.T) {
	path := tempFilePath(t, "single.sst")

	pairs := []memtable.KeyValuePair{
		{Key: "only", Value: "one"},
	}

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	val, found, err := sst.Search("only")
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}
	if !found || val != "one" {
		t.Errorf("Search(only) = (%q, %v), want (\"one\", true)", val, found)
	}
}

func TestManyEntries(t *testing.T) {
	path := tempFilePath(t, "many.sst")

	var pairs []memtable.KeyValuePair
	for i := 0; i < 500; i++ {
		pairs = append(pairs, memtable.KeyValuePair{
			Key:   fmt.Sprintf("key-%04d", i),
			Value: fmt.Sprintf("val-%04d", i),
		})
	}

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	// search for first, last, and middle entries
	checks := []int{0, 1, 249, 250, 498, 499}
	for _, i := range checks {
		key := fmt.Sprintf("key-%04d", i)
		expectedVal := fmt.Sprintf("val-%04d", i)
		val, found, err := sst.Search(key)
		if err != nil {
			t.Fatalf("Search(%q) error: %v", key, err)
		}
		if !found || val != expectedVal {
			t.Errorf("Search(%q) = (%q, %v), want (%q, true)", key, val, found, expectedVal)
		}
	}
}

func TestFileCreatedOnDisk(t *testing.T) {
	path := tempFilePath(t, "exists.sst")

	pairs := []memtable.KeyValuePair{
		{Key: "a", Value: "1"},
	}

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("SSTable file not found: %v", err)
	}
	if info.Size() == 0 {
		t.Error("SSTable file is empty")
	}
}

func TestMemtableToSSTableRoundtrip(t *testing.T) {
	path := tempFilePath(t, "roundtrip.sst")

	// build a memtable and flush it
	m := memtable.NewMemtable(1024)
	m.Put("zebra", "stripes")
	m.Put("aardvark", "snout")
	m.Put("mango", "tropical")

	// get sorted pairs from memtable
	pairs := m.InOrder()

	err := WriteSSTable(pairs, path)
	if err != nil {
		t.Fatalf("WriteSSTable failed: %v", err)
	}

	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable failed: %v", err)
	}

	// verify all keys survived the full trip: memtable → SSTable → read back
	val, found, err := sst.Search("aardvark")
	if err != nil || !found || val != "snout" {
		t.Errorf("Search(aardvark) = (%q, %v, %v), want (\"snout\", true, nil)", val, found, err)
	}

	val, found, err = sst.Search("mango")
	if err != nil || !found || val != "tropical" {
		t.Errorf("Search(mango) = (%q, %v, %v), want (\"tropical\", true, nil)", val, found, err)
	}

	val, found, err = sst.Search("zebra")
	if err != nil || !found || val != "stripes" {
		t.Errorf("Search(zebra) = (%q, %v, %v), want (\"stripes\", true, nil)", val, found, err)
	}
}
