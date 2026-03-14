package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/staf3333/teakdb/memtable"
)

type indexEntry struct {
	key string
	offset uint32
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
