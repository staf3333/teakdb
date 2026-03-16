package sstable

import (
	"fmt"
	"hash/fnv"
)

type BloomFilter struct {
	bits []bool
	numHashes int
}

func NewBloomFilter(numKeys int) *BloomFilter {
	return &BloomFilter{
		bits: make([]bool, numKeys * 10),
		numHashes: 3,
	} 
}

func (b *BloomFilter) Add(key string) {
	for i := range b.numHashes {
		hashVal := getFNVHash(fmt.Sprintf("%s%d", key, i))
		b.bits[hashVal % uint32(len(b.bits))] = true
	}
}

func (b *BloomFilter) MightContain(key string) bool {
	for i := range b.numHashes {
		hashVal := getFNVHash(fmt.Sprintf("%s%d", key, i))
		existForThisHash := b.bits[hashVal % uint32(len(b.bits))]
		if !existForThisHash {
			return false
		}
	}

	return true
}

func getFNVHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() // Get the 32-bit hash value
}
