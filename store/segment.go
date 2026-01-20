package store

import (
	"io"
	"os"
)

// TODO - Add lock if necessary in the future
type storeSegment struct {
	path string
}

func (segment *storeSegment) getEntry(key string, info *storeKeyInfo) (*storeEntry, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(int64(info.value_pos), io.SeekStart)
	if err != nil {
		return nil, err
	}

	entrySize := 16 + len(key) + info.value_sz
	bytes := make([]byte, entrySize)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}

	entry := entryFromBytes(bytes)
	return entry, nil
}

func (segment *storeSegment) putEntry(key string, value []byte) (uint32, int, error) {
	file, err := os.OpenFile(segment.path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize := int(fileInfo.Size())

	entry := createEntry(key, value)

	_, err = file.Write(entry.toBytes())
	if err != nil {
		return 0, 0, err
	}

	return entry.tstamp, fileSize, nil
}

func (segment *storeSegment) deleteEntry(key string) error {
	file, err := os.OpenFile(segment.path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	entry := createTombstoneEntry(key)
	_, err = file.Write(entry.toBytes())
	if err != nil {
		return err
	}

	return nil
}
