package compaction

// heapEntry represents a single key-value pair in the merge heap,
// tagged with which SSTable it came from and its position within that SSTable.
type heapEntry struct {
	Key           string
	Value         string
	SSTableIndex  int // lower = newer SSTable
}

// mergeHeap implements container/heap.Interface for min-heap of heapEntry.
// Ordering: by Key first, then by SSTableIndex (lower = newer = higher priority).
type mergeHeap []heapEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	if h[i].Key == h[j].Key {
		return h[i].SSTableIndex < h[j].SSTableIndex // newer SSTable wins
	}
	return h[i].Key < h[j].Key
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(heapEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	*h = old[:n-1]
	return entry
}
