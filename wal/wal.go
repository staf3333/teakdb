package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"github.com/staf3333/teakdb/memtable"
)

// open file
// writer
type WriteAheadLog struct {
	file *os.File
	filePath string
}

func NewWriteAheadLog(dataDir string) (*WriteAheadLog, error) {

	filePath := filepath.Join(dataDir, "wal.log")
	// add O_SYNC to have the file automatically call sync after every write instead of manually calling file.Sync()
	// this has stronger guarantees, but most production databases like PostgreSQL batch writes and do group commits with file.Sync()
	walFlags := os.O_WRONLY | os.O_CREATE | os.O_APPEND | os.O_SYNC
	file, err := os.OpenFile(filePath, walFlags, 0644)
	if err != nil {
		return nil, err
	}

	return &WriteAheadLog{
		file: file,
		filePath: filePath,
	}, nil
}

func (wal *WriteAheadLog) Write(key, val string) error {
	err := binary.Write(wal.file, binary.BigEndian, uint32(len(key)))
	if err != nil {
		return err
	}

	_, err = wal.file.Write([]byte(key))
	if err != nil {
		return err
	}

	err = binary.Write(wal.file, binary.BigEndian, uint32(len(val)))
	if err != nil {
		return err
	}

	_, err = wal.file.Write([]byte(val))
	if err != nil {
		return err
	}

	return nil
}

func (wal *WriteAheadLog) Replay() ([]memtable.KeyValuePair, error) {
	file, err := os.Open(wal.filePath)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	var kvPairs []memtable.KeyValuePair
	for {
		var keyLen uint32
		err = binary.Read(file, binary.BigEndian, &keyLen)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var key string
		keyBytes := make([]byte, keyLen)
		_, err := io.ReadFull(file, keyBytes)
		if err != nil {
			return nil, err
		}
		key = string(keyBytes)

		var valueLen uint32
		err = binary.Read(file, binary.BigEndian, &valueLen)
		if err != nil {
			return nil, err
		}

		var value string
		valueBytes := make([]byte, valueLen)
		_, err = io.ReadFull(file, valueBytes)
		if err != nil {
			return nil, err
		}
		value = string(valueBytes)
		kvPairs = append(kvPairs, memtable.KeyValuePair{Key: key, Value: value})
	}

	return kvPairs, nil
}

func (wal *WriteAheadLog) Reset() error {
	err := wal.file.Close()
	if err != nil {
		return err
	}
	err = os.Remove(wal.filePath)
	if err != nil {
		return err
	}

	walFlags := os.O_WRONLY | os.O_CREATE | os.O_APPEND | os.O_SYNC
	file, err := os.OpenFile(wal.filePath, walFlags, 0644)
	if err != nil {
		return err
	}
	wal.file = file
	return nil
}

func (wal *WriteAheadLog) Close() error {
	return wal.file.Close()
}
