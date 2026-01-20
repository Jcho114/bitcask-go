package store

import (
	"crypto/sha1"
	"encoding/binary"
	"io"
	"os"
	"time"
)

type storeEntry struct {
	crc      uint32
	tstamp   uint32
	ksz      uint32
	value_sz uint32
	key      []byte
	value    []byte
}

func (entry *storeEntry) toBytes() []byte {
	size := 16 + len(entry.key) + len(entry.value)
	bytes := make([]byte, size)

	binary.BigEndian.PutUint32(bytes[:4], entry.crc)
	binary.BigEndian.PutUint32(bytes[4:8], entry.tstamp)
	binary.BigEndian.PutUint32(bytes[8:12], entry.ksz)
	binary.BigEndian.PutUint32(bytes[12:16], entry.value_sz)
	copy(bytes[16:16+len(entry.key)], entry.key)
	copy(bytes[16+len(entry.key):16+len(entry.key)+len(entry.value)], entry.value)

	return bytes
}

func readFirstEntry(filePath string) (*storeEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes := make([]byte, 16)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	ksz := binary.BigEndian.Uint32(bytes[8:12])
	value_sz := binary.BigEndian.Uint32(bytes[12:16])
	totalSize := 16 + ksz + value_sz

	fullBytes := make([]byte, totalSize)
	copy(fullBytes[:16], bytes)
	_, err = file.Read(fullBytes[16:])
	if err != nil && err != io.EOF {
		return nil, err
	}

	entry := entryFromBytes(fullBytes)
	return entry, nil
}

func readEntriesFromFile(filePath string) ([]*storeEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := []*storeEntry{}
	for {
		headerBytes := make([]byte, 16)
		_, err := file.Read(headerBytes)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		ksz := binary.BigEndian.Uint32(headerBytes[8:12])
		value_sz := binary.BigEndian.Uint32(headerBytes[12:16])
		totalSize := 16 + ksz + value_sz

		entryBytes := make([]byte, totalSize)
		copy(entryBytes[:16], headerBytes)
		_, err = file.Read(entryBytes[16:])
		if err != nil && err != io.EOF {
			return nil, err
		}

		entry := entryFromBytes(entryBytes)
		entries = append(entries, entry)
	}

	return entries, nil
}

func entryFromBytes(bytes []byte) *storeEntry {
	entry := &storeEntry{}

	entry.crc = binary.BigEndian.Uint32(bytes[:4])
	entry.tstamp = binary.BigEndian.Uint32(bytes[4:8])
	entry.ksz = binary.BigEndian.Uint32(bytes[8:12])
	entry.value_sz = binary.BigEndian.Uint32(bytes[12:16])
	entry.key = make([]byte, entry.ksz)
	copy(entry.key, bytes[16:16+entry.ksz])
	entry.value = make([]byte, entry.value_sz)
	copy(entry.value, bytes[16+entry.ksz:16+entry.ksz+entry.value_sz])

	return entry
}

func createEntry(key string, value []byte) *storeEntry {
	hashBytes := sha1.Sum(value) // TODO - Do we checksum only the value?
	firstFourBytes := hashBytes[:4]
	checksum := binary.BigEndian.Uint32(firstFourBytes)
	now := time.Now()

	return &storeEntry{
		crc:      checksum,
		tstamp:   uint32(now.Unix()),
		ksz:      uint32(len(key)),
		value_sz: uint32(len(value)),
		key:      []byte(key),
		value:    []byte(value),
	}
}

func createTombstoneEntry(key string) *storeEntry {
	return &storeEntry{
		crc:      0,
		tstamp:   uint32(time.Now().Unix()),
		ksz:      uint32(len(key)),
		value_sz: 0,
		key:      []byte(key),
		value:    []byte{},
	}
}
