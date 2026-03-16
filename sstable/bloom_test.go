package sstable

import (
	"fmt"
	"testing"
)

func TestBloomFilterAddAndCheck(t *testing.T) {
	bf := NewBloomFilter(100)

	bf.Add("apple")
	bf.Add("banana")
	bf.Add("cherry")

	if !bf.MightContain("apple") {
		t.Error("MightContain(apple) = false, expected true")
	}
	if !bf.MightContain("banana") {
		t.Error("MightContain(banana) = false, expected true")
	}
	if !bf.MightContain("cherry") {
		t.Error("MightContain(cherry) = false, expected true")
	}
}

func TestBloomFilterDefinitelyNot(t *testing.T) {
	bf := NewBloomFilter(100)

	bf.Add("apple")
	bf.Add("banana")

	// these should almost certainly return false
	falsePositives := 0
	missingKeys := []string{"mango", "grape", "kiwi", "peach", "plum", "fig", "pear", "lime"}
	for _, key := range missingKeys {
		if bf.MightContain(key) {
			falsePositives++
		}
	}

	// with 100 keys capacity and only 2 added, false positives should be extremely rare
	if falsePositives > 2 {
		t.Errorf("too many false positives: %d out of %d checks", falsePositives, len(missingKeys))
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	numKeys := 1000
	bf := NewBloomFilter(numKeys)

	// add 1000 keys
	for i := 0; i < numKeys; i++ {
		bf.Add(fmt.Sprintf("key-%04d", i))
	}

	// all added keys must be found
	for i := 0; i < numKeys; i++ {
		if !bf.MightContain(fmt.Sprintf("key-%04d", i)) {
			t.Errorf("MightContain(key-%04d) = false, must be true for added key", i)
		}
	}

	// check 10000 keys that were NOT added — count false positives
	falsePositives := 0
	numChecks := 10000
	for i := numKeys; i < numKeys+numChecks; i++ {
		if bf.MightContain(fmt.Sprintf("key-%04d", i)) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(numChecks) * 100
	t.Logf("False positive rate: %.2f%% (%d / %d)", rate, falsePositives, numChecks)

	// with 10 bits per key and 3 hashes, expect ~1% — allow up to 3%
	if rate > 3.0 {
		t.Errorf("false positive rate too high: %.2f%% (expected < 3%%)", rate)
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(10)

	if bf.MightContain("anything") {
		t.Error("empty bloom filter should not contain anything")
	}
}
