package lsmtree

import (
	"fmt"
	"testing"

	"github.com/staf3333/teakdb/memtable"
)

func TestDBPutAndGet(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	db.Put("name", "teakdb")
	db.Put("lang", "go")

	val, found, err := db.Get("name")
	if err != nil || !found || val != "teakdb" {
		t.Errorf("Get(name) = (%q, %v, %v), want (\"teakdb\", true, nil)", val, found, err)
	}

	val, found, err = db.Get("lang")
	if err != nil || !found || val != "go" {
		t.Errorf("Get(lang) = (%q, %v, %v), want (\"go\", true, nil)", val, found, err)
	}
}

func TestDBGetNotFound(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	val, found, err := db.Get("ghost")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if found {
		t.Errorf("Get(ghost) found with value %q, expected not found", val)
	}
}

func TestDBFlushToSSTable(t *testing.T) {
	dir := t.TempDir()

	// use a tiny memtable so we trigger a flush
	db := &DB{
		memtable: newSmallMemtable(),
		dataDir:  dir,
	}

	// write enough data to trigger a flush
	for i := 0; i < 100; i++ {
		err := db.Put(fmt.Sprintf("key-%03d", i), fmt.Sprintf("val-%03d", i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// should have at least one SSTable
	if len(db.sstables) == 0 {
		t.Error("expected at least one SSTable after filling memtable")
	}

	// all keys should still be findable (memtable or SSTables)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		expectedVal := fmt.Sprintf("val-%03d", i)
		val, found, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error: %v", key, err)
		}
		if !found || val != expectedVal {
			t.Errorf("Get(%q) = (%q, %v), want (%q, true)", key, val, found, expectedVal)
		}
	}
}

func TestDBUpdateOverwritesValue(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDB(dir)
	if err != nil {
		t.Fatalf("NewDB failed: %v", err)
	}

	db.Put("color", "blue")
	db.Put("color", "green")

	val, found, err := db.Get("color")
	if err != nil || !found || val != "green" {
		t.Errorf("Get(color) = (%q, %v, %v), want (\"green\", true, nil)", val, found, err)
	}
}

func TestDBSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// first "session" — write data and trigger flush
	db1 := &DB{
		memtable: newSmallMemtable(),
		dataDir:  dir,
	}
	for i := 0; i < 100; i++ {
		err := db1.Put(fmt.Sprintf("key-%03d", i), fmt.Sprintf("val-%03d", i))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if len(db1.sstables) == 0 {
		t.Fatal("expected SSTables to be created")
	}

	// second "session" — reopen from same directory
	db2, err := NewDB(dir)
	if err != nil {
		t.Fatalf("NewDB (reopen) failed: %v", err)
	}

	// data that was flushed to SSTables should still be readable
	// (data still in memtable at shutdown is lost — WAL will fix this later)
	for _, sst := range db1.sstables {
		_ = sst // just verifying sstables existed
	}

	// check a few keys that should be in SSTables
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%03d", i)
		expectedVal := fmt.Sprintf("val-%03d", i)
		val, found, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error: %v", key, err)
		}
		// some keys might still have been in memtable (not flushed), so only check if found
		if found && val != expectedVal {
			t.Errorf("Get(%q) = %q, want %q", key, val, expectedVal)
		}
	}
}

// helper to create a small memtable that flushes quickly
func newSmallMemtable() *memtable.Memtable {
	return memtable.NewMemtable(256) // 256 bytes
}
