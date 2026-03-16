package compaction

import (
	"container/heap"

	"github.com/staf3333/teakdb/memtable"
	"github.com/staf3333/teakdb/sstable"
)

func Compact(tables []sstable.SSTable, outputPath string) error {
	// read all the sstable pairs into heap structure
	// but for memory efficiency, only push one level at a time
	var tableHeap mergeHeap
	heap.Init(&tableHeap)
	for i, table := range tables {

		pairs, err := table.GetPairs()
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			heap.Push(&tableHeap, heapEntry{
				Key: pair.Key,
				Value: pair.Value,
				SSTableIndex: i,
			})
		}
	}

	var compactedPairs []memtable.KeyValuePair
	for len(tableHeap) > 0 {
		// pop the smallest key from heap
		heapEntry := heap.Pop(&tableHeap).(heapEntry)

		// if the next heap entries have the same key, ignore them
		for len(tableHeap) > 0 && heapEntry.Key == tableHeap[0].Key {
			heap.Pop(&tableHeap)
		}

		compactedPairs = append(compactedPairs, memtable.KeyValuePair{Key: heapEntry.Key, Value: heapEntry.Value})

	}

	return sstable.WriteSSTable(compactedPairs, outputPath)
}
