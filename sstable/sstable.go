package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/staf3333/teakdb/memtable"
)


func WriteSSTable(sortedPairs []memtable.KeyValuePair, fileName string) error {
	// open the file 
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
