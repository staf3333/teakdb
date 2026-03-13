package memtable

type node struct {
	key    string
	value  string
	color  color
	left   *node
	right  *node
	parent *node
}

type RBTree struct {
	root     *node
	sentinel *node
}

type color int

const (
	red = iota
	black
)

func NewRBTree() *RBTree {
	sentinel := &node{color: black}
	return &RBTree{
		root:     sentinel,
		sentinel: sentinel,
	}
}

func (t *RBTree) leftRotate(node *node) {
	pivot := node.right
	parent := node.parent
	leftSubChild := pivot.left

	node.right = leftSubChild
	leftSubChild.parent = node

	pivot.parent = parent
	pivot.left = node
	if t.root == node {
		t.root = pivot
	} else if parent.left == node {
		parent.left = pivot
	} else {
		parent.right = pivot
	}

	node.parent = pivot
}

func (t *RBTree) rightRotate(node *node) {
	pivot := node.left
	parent := node.parent
	rightSubChild := pivot.right

	node.left = rightSubChild
	rightSubChild.parent = node

	pivot.parent = parent
	pivot.right = node
	if t.root == node {
		t.root = pivot
	} else if parent.left == node {
		parent.left = pivot
	} else {
		parent.right = pivot
	}

	node.parent = pivot
}

func (t *RBTree) Insert(key, value string) {
	newNode := &node{
		key:   key,
		value: value,
		color: red,
		left:  t.sentinel,
		right: t.sentinel,
	}

	// handle case where this is the first node in the tree
	if t.root == t.sentinel {
		t.root = newNode
		newNode.parent = t.sentinel
		// root is always black
		newNode.color = black
		return
	}

	// iterate through tree to find the proper location
	curr := t.root
	parent := t.root.parent

	for curr != t.sentinel {
		if newNode.key == curr.key {
			curr.value = newNode.value
			return
		}

		parent = curr
		if newNode.key < curr.key {
			curr = curr.left
		} else {
			curr = curr.right
		}
	}

	if newNode.key < parent.key {
		parent.left = newNode
	} else {
		parent.right = newNode
	}
	newNode.parent = parent
	t.insertFixup(newNode)
}

func (t *RBTree) insertFixup(newNode *node) {
	for newNode.parent.color == red {
		parent := newNode.parent
		grandparent := newNode.parent.parent
		if parent == grandparent.left {
			uncle := grandparent.right
			if uncle.color == red {
				parent.color = black
				uncle.color = black
				grandparent.color = red
				newNode = grandparent
			} else {
				if newNode == parent.right {
					t.leftRotate(parent)
					newNode = parent
				}
				t.rightRotate(grandparent)
				grandparent.color = red
				newNode.parent.color = black
			}
		} else {
			uncle := grandparent.left
			if uncle.color == red {
				parent.color = black
				uncle.color = black
				grandparent.color = red
				newNode = grandparent
			} else {
				if newNode == parent.left {
					t.rightRotate(parent)
					newNode = parent
				}
				t.leftRotate(grandparent)
				grandparent.color = red
				newNode.parent.color = black
			}
		}
	}
	t.root.color = black
}

func (t *RBTree) Search(key string) (string, bool) {
	if t.root == t.sentinel {
		return "", false
	}

	curr := t.root
	for curr != t.sentinel {
		if curr.key == key {
			return curr.value, true
		}

		if key < curr.key {
			curr = curr.left
		} else {
			curr = curr.right
		}
	}

	return "", false
}

type KeyValuePair struct {
	Key, Value string
}

func (t *RBTree) InOrderTraversal() []KeyValuePair {
	return t.dfs(t.root)
}

func (t *RBTree) dfs(node *node) []KeyValuePair {
	if node == t.sentinel {
		return []KeyValuePair{}
	}
	result := append(t.dfs(node.left), KeyValuePair{Key: node.key, Value: node.value})
	result = append(result, t.dfs(node.right)...)
	return result
}
