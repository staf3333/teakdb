package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/staf3333/teakdb/compaction"
	"github.com/staf3333/teakdb/memtable"
	"github.com/staf3333/teakdb/sstable"
	"github.com/staf3333/teakdb/wal"
)

const defaultMaxSize = 4 * 1024 * 1024 // 4MB
const defaultSSTableLimit = 5

type DB struct {
	memtable *memtable.Memtable
	sstables []sstable.SSTable
	dataDir string
	writeAheadLog *wal.WriteAheadLog
}

func NewDB(dataDir string) (*DB, error) {
	// ensure data directory exists
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
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

	memtable := memtable.NewMemtable(defaultMaxSize)

	// initialize write ahead log
	writeAheadLog, err := wal.NewWriteAheadLog(dataDir)
	if err != nil {
		return nil, err
	}

	walKeyValPairs, err := writeAheadLog.Replay() 
	if err != nil {
		return nil, err
	}
	if len(walKeyValPairs) > 0 {
		for _, pair := range walKeyValPairs {
			memtable.Put(pair.Key, pair.Value)
		}
	}

	return &DB{
		memtable: memtable,
		sstables: sstables,
		dataDir:  dataDir,
		writeAheadLog: writeAheadLog,
	}, nil
}

func (d *DB) Put(key, val string) error {
	err := d.writeAheadLog.Write(key, val)
	if err != nil {
		return err
	}
	// write to memtable, if full flush to sstable and start a new one
	d.memtable.Put(key, val)
	err = d.flushMemtable()
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) flushMemtable() error {
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
		d.writeAheadLog.Reset()
	
		err = d.compact()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) compact() error {
	if len(d.sstables) > defaultSSTableLimit {
		fileName := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
		filePath := filepath.Join(d.dataDir, fileName)
		err := compaction.Compact(d.sstables, filePath)
		if err != nil {
			return err
		}

		// after compaction, delete the existing sstables
		for _, table := range d.sstables {
			os.Remove(table.Filepath)
		}

		// open the new sstable
		sst, err := sstable.OpenSSTable(filePath)
		if err != nil {
			return err
		}

		// create new sstables list
		d.sstables = []sstable.SSTable{*sst}
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



