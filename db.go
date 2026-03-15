package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/staf3333/teakdb/memtable"
	"github.com/staf3333/teakdb/sstable"
)

const defaultMaxSize = 4 * 1024 * 1024 // 4MB

type DB struct {
	memtable *memtable.Memtable
	sstables []sstable.SSTable
	dataDir string
}

func NewDB(dataDir string) (*DB, error) {
	// ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// scan for existing SSTable files and load them (newest first)
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	var sstFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
			sstFiles = append(sstFiles, entry.Name())
		}
	}

	// sort alphabetically (timestamp in name = chronological), then reverse for newest first
	sort.Strings(sstFiles)

	var sstables []sstable.SSTable
	for i := len(sstFiles) - 1; i >= 0; i-- {
		sst, err := sstable.OpenSSTable(filepath.Join(dataDir, sstFiles[i]))
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, *sst)
	}

	return &DB{
		memtable: memtable.NewMemtable(defaultMaxSize),
		sstables: sstables,
		dataDir:  dataDir,
	}, nil
}

func (d *DB) Put(key, val string) error {
	// write to memtable, if full flush to sstable and start a new one
	d.memtable.Put(key, val)

	if d.memtable.IsFull() {
		memtableKVPairs := d.memtable.InOrder()
		fileName := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
		filePath := filepath.Join(d.dataDir, fileName)
		err := sstable.WriteSSTable(memtableKVPairs, filePath)
		if err != nil {
			return err
		}

		sst, err := sstable.OpenSSTable(filePath)
		if err != nil {
			return err
		}
		// newest first -> prepend
		d.sstables = append([]sstable.SSTable{*sst}, d.sstables...)

		d.memtable = memtable.NewMemtable(defaultMaxSize)
	}
	return nil
}

func (d *DB) Get(key string) (string, bool, error) {
	// first check memtable for the key
	// if not there, read from the SSTables sequentially (newest to oldest)
	val, found := d.memtable.Get(key)
	if !found {
		for _, sst := range d.sstables {
			val, found, err := sst.Search(key)
			if err != nil {
				return "", false, err
			}

			if found {
				return val, true, nil
			}
		}
	} 
	return val, found, nil
}



