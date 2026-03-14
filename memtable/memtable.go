package memtable

type Memtable struct {
	maxSize int
	currentSize int
	rbTree *RBTree
}

func NewMemtable(maxSize int) *Memtable {
	return &Memtable {
		maxSize: maxSize,
		currentSize: 0,
		rbTree: NewRBTree(),
	}
}

func (m *Memtable) Put(key, val string) {
	prevValue, isKeyInTree := m.rbTree.Search(key)
	m.rbTree.Insert(key, val)
	m.currentSize += len(val)
	if isKeyInTree {
		m.currentSize -= len(prevValue)
	} else {
		m.currentSize += len(key)
	}
}

func (m *Memtable) Get(key string) (string, bool) {
	return m.rbTree.Search(key)
}

func (m *Memtable) IsFull() bool {
	return m.currentSize >= m.maxSize
}

func (m *Memtable) InOrder() []KeyValuePair {
	return m.rbTree.InOrderTraversal()
}

