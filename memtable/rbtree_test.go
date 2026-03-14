package memtable

import (
	"fmt"
	"testing"
)

func TestInsertAndSearchBasic(t *testing.T) {
	tree := NewRBTree()

	tree.Insert("banana", "yellow")
	tree.Insert("apple", "red")
	tree.Insert("cherry", "dark red")

	tests := []struct {
		key      string
		wantVal  string
		wantFind bool
	}{
		{"banana", "yellow", true},
		{"apple", "red", true},
		{"cherry", "dark red", true},
	}

	for _, tt := range tests {
		val, found := tree.Search(tt.key)
		if found != tt.wantFind || val != tt.wantVal {
			t.Errorf("Search(%q) = (%q, %v), want (%q, %v)", tt.key, val, found, tt.wantVal, tt.wantFind)
		}
	}
}

func TestSearchKeyNotFound(t *testing.T) {
	tree := NewRBTree()

	tree.Insert("apple", "red")

	val, found := tree.Search("mango")
	if found || val != "" {
		t.Errorf("Search(mango) = (%q, %v), want (\"\", false)", val, found)
	}
}

func TestSearchEmptyTree(t *testing.T) {
	tree := NewRBTree()

	val, found := tree.Search("anything")
	if found || val != "" {
		t.Errorf("Search on empty tree = (%q, %v), want (\"\", false)", val, found)
	}
}

func TestInsertDuplicateKeyUpdatesValue(t *testing.T) {
	tree := NewRBTree()

	tree.Insert("color", "blue")
	tree.Insert("color", "green")

	val, found := tree.Search("color")
	if !found || val != "green" {
		t.Errorf("Search(color) = (%q, %v), want (\"green\", true)", val, found)
	}
}

func TestInOrderTraversalSorted(t *testing.T) {
	tree := NewRBTree()

	// insert in random order
	keys := []string{"delta", "bravo", "echo", "alpha", "charlie"}
	for _, k := range keys {
		tree.Insert(k, k+"-val")
	}

	result := tree.InOrderTraversal()
	expected := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	if len(result) != len(expected) {
		t.Fatalf("InOrderTraversal length = %d, want %d", len(result), len(expected))
	}
	for i, kv := range result {
		if kv.Key != expected[i] {
			t.Errorf("InOrderTraversal[%d].Key = %q, want %q", i, kv.Key, expected[i])
		}
	}
}

func TestRedBlackProperties(t *testing.T) {
	tree := NewRBTree()

	// insert enough nodes to trigger multiple rotations and recolors
	for i := 0; i < 100; i++ {
		tree.Insert(fmt.Sprintf("key-%03d", i), fmt.Sprintf("val-%03d", i))
	}

	// Rule 2: root is black
	if tree.root.color != black {
		t.Error("Rule 2 violated: root is not black")
	}

	// Rule 3: no red node has a red child
	// Rule 5: all paths from a node to leaves have the same black-height
	violations := []string{}
	checkNode(tree, tree.root, &violations)
	for _, v := range violations {
		t.Error(v)
	}

	// Check black-height consistency
	_, ok := checkBlackHeight(tree, tree.root)
	if !ok {
		t.Error("Rule 5 violated: unequal black-heights detected")
	}
}

// checkNode recursively checks rule 3 (no red-red parent-child)
func checkNode(tree *RBTree, n *node, violations *[]string) {
	if n == tree.sentinel {
		return
	}
	if n.color == red {
		if n.left.color == red {
			*violations = append(*violations, fmt.Sprintf("Rule 3 violated: red node %q has red left child %q", n.key, n.left.key))
		}
		if n.right.color == red {
			*violations = append(*violations, fmt.Sprintf("Rule 3 violated: red node %q has red right child %q", n.key, n.right.key))
		}
	}
	checkNode(tree, n.left, violations)
	checkNode(tree, n.right, violations)
}

// checkBlackHeight returns the black-height of a subtree and whether it's consistent
func checkBlackHeight(tree *RBTree, n *node) (int, bool) {
	if n == tree.sentinel {
		return 1, true // sentinel nodes are black
	}

	leftHeight, leftOk := checkBlackHeight(tree, n.left)
	rightHeight, rightOk := checkBlackHeight(tree, n.right)

	if !leftOk || !rightOk || leftHeight != rightHeight {
		return 0, false
	}

	height := leftHeight
	if n.color == black {
		height++
	}
	return height, true
}

func TestManyInsertsAndSearches(t *testing.T) {
	tree := NewRBTree()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%04d", i)
		tree.Insert(key, val)
	}

	// verify all keys are findable
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		expectedVal := fmt.Sprintf("val-%04d", i)
		val, found := tree.Search(key)
		if !found || val != expectedVal {
			t.Errorf("Search(%q) = (%q, %v), want (%q, true)", key, val, found, expectedVal)
		}
	}
}
