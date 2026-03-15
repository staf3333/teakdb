package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/staf3333/teakdb/memtable"
)

type indexEntry struct {
	key string
	offset uint32
}

type SSTable struct {
	filepath string
	index []indexEntry
}

func OpenSSTable(filepath string) (*SSTable, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(-4, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// read index offset from file
	var indexOffset uint32
	err = binary.Read(file, binary.BigEndian, &indexOffset)
	if err != nil {
		return nil, err
	}


	// read the index into memory starting from the offset
	// keep track of the bytes to know when to stop reading (before the index offset 4 bytes)
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := uint32(fileInfo.Size())
	currentPosition := indexOffset
	_, err = file.Seek(int64(indexOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	var index []indexEntry

	for currentPosition < fileSize - 4 {
		// read key, and offset into memory
		var keyLen uint32
		err = binary.Read(file, binary.BigEndian, &keyLen)
		if err != nil {
			return nil, err
		}

		var key string
		keyBytes := make([]byte, keyLen)
		_, err := io.ReadFull(file, keyBytes)
		if err != nil {
			return nil, err
		}
		key = string(keyBytes)

		var offset uint32
		err = binary.Read(file, binary.BigEndian, &offset)
		if err != nil {
			return nil, err
		}
		index = append(index, indexEntry{key: key, offset: offset})
		currentPosition += 4 + keyLen + 4
	}

	return &SSTable{filepath: filepath, index: index}, nil
}


func WriteSSTable(sortedPairs []memtable.KeyValuePair, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	// use defer as a guard to make sure that the file is closed even if we have to return early
	defer file.Close()

	// Do a buffered write to efficiently write each pair to the file
	// Buffering groups small writes into a single, larger system call, significantly reducing I/O overhead and CPU usage. 
	// If we were to write each individual key value pair, we would be triggering a seperate system call
	// SSTables are designed for high-speed sequential writes. Buffering ensures the disk receives continuous streams of data rather than fragmented small writes
	// https://www.reddit.com/r/golang/comments/1qc58lp/when_to_use_bufiowriter_bufioreader_and_when_not/
	writer := bufio.NewWriter(file)

	var offset uint32;
	var index []indexEntry
	for _, pair := range sortedPairs {
		// write [key_len][key_bytes][val_len][val_bytes]
		err = binary.Write(writer, binary.BigEndian, uint32(len(pair.Key)))
		if err != nil {
			return err
		}

		_, err := writer.Write([]byte(pair.Key))
		if err != nil {
			return err
		}

		err = binary.Write(writer, binary.BigEndian, uint32(len(pair.Value)))
		if err != nil {
			return err
		}

		_, err = writer.Write([]byte(pair.Value))
		if err != nil {
			return err
		}

		// after writing  add to index add to the offset and
		index = append(index, indexEntry{pair.Key, offset})

		// 4+4 bytes for length of each key/val and the lengths themselves
		offset += 8 + uint32(len(pair.Key)) + uint32(len(pair.Value))

	}

	for _, entry := range index {
		// write [key_len][key_bytes][offset]
		err = binary.Write(writer, binary.BigEndian, uint32(len(entry.key)))
		if err != nil {
			return err
		}

		_, err := writer.Write([]byte(entry.key))
		if err != nil {
			return err
		}

		err = binary.Write(writer, binary.BigEndian, entry.offset)
		if err != nil {
			return err
		}
	}

	// at the end of the key/val writes, the offset will coincidide with the start of the index
	err = binary.Write(writer, binary.BigEndian, offset)
	if err != nil {
		return err
	}

	// 1. Flush Go's internal buffer to the OS kernel
	err = writer.Flush()
	if err != nil {
		return err
	}
	// 2. Sync the OS kernel's cache to physical hardware
	// This is the most likely place for a hardware-level write error.
	err = file.Sync()
	if err != nil {
		return err
	}

	// 3. Explicitly close to catch any final filesystem errors.
	// If successful, the deferred file.Close() will just return an error
	// indicating the file is already closed, which is harmless.
	return file.Close()
}

func (s *SSTable) Search(key string) (string, bool, error) {
	// do binary search on the index entries of the SSTable to find the byte offset of the key/val pair
	var found bool
	var offset uint32
	left := 0
	right := len(s.index) - 1
	for left <= right {
		mid := (left + right) / 2
		if s.index[mid].key == key {
			offset = s.index[mid].offset
			found = true
			break
		} else if s.index[mid].key > key {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	if !found {
		return "", false, nil
	}

	// once you have the byte offset, read those bytes from the file and return the value
	file, err := os.Open(s.filepath)
	if err != nil {
		return "", false, err
	}

	defer file.Close()

	// because we already know the key length, we can skip to where we want to read the value
	_, err = file.Seek(int64(offset) + 4 + int64(len(key)), io.SeekStart)
	if err != nil {
		return "", false, err
	}

	var valueLen uint32
	err = binary.Read(file, binary.BigEndian, &valueLen)
	if err != nil {
		return "", false, err
	}

	var value string
	valueBytes := make([]byte, valueLen)
	_, err = io.ReadFull(file, valueBytes)
	if err != nil {
		return "", false, err
	}
	value = string(valueBytes)

	return value, found, nil
}
