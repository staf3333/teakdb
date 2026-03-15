package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog failed: %v", err)
	}

	w.Write("apple", "red")
	w.Write("banana", "yellow")
	w.Write("cherry", "dark red")

	pairs, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(pairs) != 3 {
		t.Fatalf("Replay returned %d entries, want 3", len(pairs))
	}

	expected := []struct{ key, val string }{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "dark red"},
	}
	for i, e := range expected {
		if pairs[i].Key != e.key || pairs[i].Value != e.val {
			t.Errorf("entry[%d] = {%q, %q}, want {%q, %q}", i, pairs[i].Key, pairs[i].Value, e.key, e.val)
		}
	}
}

func TestReplayEmptyWAL(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog failed: %v", err)
	}

	pairs, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(pairs) != 0 {
		t.Errorf("Replay on empty WAL returned %d entries, want 0", len(pairs))
	}
}

func TestReset(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog failed: %v", err)
	}

	w.Write("key1", "val1")
	w.Write("key2", "val2")

	err = w.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// after reset, replay should return nothing
	pairs, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay after reset failed: %v", err)
	}
	if len(pairs) != 0 {
		t.Errorf("Replay after reset returned %d entries, want 0", len(pairs))
	}
}

func TestWALSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	// first session — write some data
	w1, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog failed: %v", err)
	}
	w1.Write("name", "teakdb")
	w1.Write("lang", "go")
	w1.Close()

	// second session — reopen and replay
	w2, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog (reopen) failed: %v", err)
	}

	pairs, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(pairs) != 2 {
		t.Fatalf("Replay returned %d entries, want 2", len(pairs))
	}
	if pairs[0].Key != "name" || pairs[0].Value != "teakdb" {
		t.Errorf("entry[0] = {%q, %q}, want {\"name\", \"teakdb\"}", pairs[0].Key, pairs[0].Value)
	}
}

func TestResetDeletesFile(t *testing.T) {
	dir := t.TempDir()

	w, err := NewWriteAheadLog(dir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog failed: %v", err)
	}

	w.Write("key", "val")

	walPath := filepath.Join(dir, "wal.log")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Fatal("WAL file should exist after write")
	}

	w.Reset()

	// file should exist again (fresh one created by Reset)
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal("WAL file should exist after reset")
	}
	if info.Size() != 0 {
		t.Errorf("WAL file should be empty after reset, got %d bytes", info.Size())
	}
}
