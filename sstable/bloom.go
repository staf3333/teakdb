package sstable

import (
	"hash/fnv"
)

type BloomFilter struct {
	bits      []bool
	numHashes int
}

func NewBloomFilter(numKeys int) *BloomFilter {
	return &BloomFilter{
		bits:      make([]bool, numKeys*10),
		numHashes: 3,
	}
}

func (b *BloomFilter) Add(key string) {
	h1, h2 := getDoubleHash(key)
	size := uint32(len(b.bits))
	for i := 0; i < b.numHashes; i++ {
		idx := (h1 + uint32(i)*h2) % size
		b.bits[idx] = true
	}
}

func (b *BloomFilter) MightContain(key string) bool {
	h1, h2 := getDoubleHash(key)
	size := uint32(len(b.bits))
	for i := 0; i < b.numHashes; i++ {
		idx := (h1 + uint32(i)*h2) % size
		if !b.bits[idx] {
			return false
		}
	}
	return true
}

// getDoubleHash returns two independent hashes using a seeded FNV approach
func getDoubleHash(s string) (uint32, uint32) {
	data := []byte(s)

	h1 := fnv.New32a()
	h1.Write(data)

	// seed the second hash by prepending a constant byte
	h2 := fnv.New32a()
	h2.Write([]byte{0x9e})
	h2.Write(data)

	// ensure h2 is odd so it's coprime with the bit array size, giving better spread
	hash2 := h2.Sum32() | 1

	return h1.Sum32(), hash2
}
