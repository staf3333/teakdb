package memtable

import "testing"

func TestMemtablePutAndGet(t *testing.T) {
	m := NewMemtable(1024)

	m.Put("name", "teakdb")
	m.Put("lang", "go")

	val, found := m.Get("name")
	if !found || val != "teakdb" {
		t.Errorf("Get(name) = (%q, %v), want (\"teakdb\", true)", val, found)
	}

	val, found = m.Get("lang")
	if !found || val != "go" {
		t.Errorf("Get(lang) = (%q, %v), want (\"go\", true)", val, found)
	}
}

func TestMemtableGetNotFound(t *testing.T) {
	m := NewMemtable(1024)

	val, found := m.Get("missing")
	if found || val != "" {
		t.Errorf("Get(missing) = (%q, %v), want (\"\", false)", val, found)
	}
}

func TestMemtableDuplicateKeyUpdatesValue(t *testing.T) {
	m := NewMemtable(1024)

	m.Put("color", "blue")
	m.Put("color", "green")

	val, found := m.Get("color")
	if !found || val != "green" {
		t.Errorf("Get(color) = (%q, %v), want (\"green\", true)", val, found)
	}
}

func TestMemtableIsFull(t *testing.T) {
	// maxSize of 20 bytes
	m := NewMemtable(20)

	if m.IsFull() {
		t.Error("empty memtable should not be full")
	}

	// "key1" (4) + "val1" (4) = 8 bytes
	m.Put("key1", "val1")
	if m.IsFull() {
		t.Errorf("memtable should not be full at 8 bytes")
	}

	// "key2" (4) + "val2" (4) = 8 more → 16 bytes
	m.Put("key2", "val2")
	if m.IsFull() {
		t.Errorf("memtable should not be full at 16 bytes")
	}

	// "key3" (4) + "val3" (4) = 8 more → 24 bytes → full!
	m.Put("key3", "val3")
	if !m.IsFull() {
		t.Errorf("memtable should be full at 24 bytes (threshold 20)")
	}
}

func TestMemtableSizeAccountsForUpdates(t *testing.T) {
	m := NewMemtable(100)

	// "k" (1) + "longvalue" (9) = 10 bytes
	m.Put("k", "longvalue")

	// update: "k" already counted, old val (9) replaced by "short" (5)
	// new size = 10 - 9 + 5 = 6
	m.Put("k", "short")

	// add another: "x" (1) + "y" (1) = 2 more → 8 total
	m.Put("x", "y")

	// should NOT be full at 8 bytes with 100 byte threshold
	if m.IsFull() {
		t.Error("memtable should not be full after value update reduced size")
	}
}

func TestMemtableInOrder(t *testing.T) {
	m := NewMemtable(1024)

	m.Put("cherry", "3")
	m.Put("apple", "1")
	m.Put("banana", "2")

	result := m.InOrder()
	expected := []KeyValuePair{
		{"apple", "1"},
		{"banana", "2"},
		{"cherry", "3"},
	}

	if len(result) != len(expected) {
		t.Fatalf("InOrder() length = %d, want %d", len(result), len(expected))
	}
	for i, kv := range result {
		if kv.Key != expected[i].Key || kv.Value != expected[i].Value {
			t.Errorf("InOrder()[%d] = {%q, %q}, want {%q, %q}", i, kv.Key, kv.Value, expected[i].Key, expected[i].Value)
		}
	}
}
