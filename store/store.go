package store

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Store interface {
	RunReplit() error
}

type store struct {
	path         string
	keydir       map[string]storeKeyInfo
	segments     []storeSegment
	curr_segment uint64
}

type storeKeyInfo struct {
	file_id   uint64
	value_sz  int
	value_pos int
	tstamp    uint32
}

type storeSegment os.FileInfo

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

func OpenStore(path string) (Store, error) {
	directoryExists := false

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return nil, fmt.Errorf("open: %v", err)
		}
	} else if err == nil {
		directoryExists = true
	} else {
		return nil, fmt.Errorf("open: %v", err)
	}

	s := &store{
		path:         path,
		keydir:       make(map[string]storeKeyInfo),
		segments:     make([]storeSegment, 0),
		curr_segment: 0,
	}

	if directoryExists {
		// TODO - Should process the directory instead
		return s, nil
	}

	firstSegmentPath := filepath.Join(path, strconv.FormatUint(s.curr_segment, 10))
	file, err := os.Create(firstSegmentPath)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	file.Close()

	firstSegmentInfo, err := os.Stat(firstSegmentPath)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	s.segments = append(s.segments, firstSegmentInfo)
	return s, nil
}

func (s *store) RunReplit() error {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
		if err := s.handleLine(line); err != nil {
			return fmt.Errorf("replit: %v", err)
		}
		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("replit: %v", err)
	}

	return nil
}

func (s *store) handleLine(line string) error {
	split := strings.Split(line, " ")
	command := split[0]
	switch command {
	case "GET":
		if len(split) != 2 {
			fmt.Println("INVALID COMMAND: GET REQUIRES 1 ARGUMENT")
			return nil
		}
		key := split[1]
		value, err := s.get(key)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		if value == nil {
			fmt.Println("NULL")
		} else {
			fmt.Println(string(value))
		}
	case "PUT":
		if len(split) != 3 {
			fmt.Println("INVALID COMMAND: PUT REQUIRES 2 ARGUMENTS")
			return nil
		}
		key, value := split[1], []byte(split[2])
		err := s.put(key, value)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		fmt.Printf("PUT %s SUCCESSFULLY\n", split[1])
	case "DELETE":
		if len(split) != 2 {
			fmt.Println("INVALID COMMAND: DELETE REQUIRES 1 ARGUMENT")
			return nil
		}
		key := split[1]
		err := s.delete(key)
		if err != nil {
			return fmt.Errorf("handle: %v", err)
		}
		fmt.Printf("DELETE %s SUCCESSFULLY\n", split[1])
	case "KEYS":
		if len(split) != 1 {
			fmt.Println("INVALID COMMAND: KEYS REQUIRES NO ARGUMENTS")
			return nil
		}
		keys := s.keys()
		if len(keys) == 0 {
			fmt.Println("NO KEYS")
		} else {
			fmt.Println(strings.Join(keys, ", "))
		}
	default:
		fmt.Printf("INVALID COMMAND: %s IS NOT A COMMAND\n", command)
	}
	return nil
}

func (s *store) get(key string) ([]byte, error) {
	info, ok := s.keydir[key]
	if !ok {
		return nil, nil
	}

	segment := s.segments[info.file_id]
	segmentPath := filepath.Join(s.path, segment.Name())
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("get: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(int64(info.value_pos), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("get: %v", err)
	}

	entrySize := 16 + len(key) + info.value_sz
	bytes := make([]byte, entrySize)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, fmt.Errorf("get: %v", err)
	}

	entry := entryFromBytes(bytes)
	return entry.value, nil
}

func (s *store) createEntry(key string, value []byte) *storeEntry {
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

func (s *store) put(key string, value []byte) error {
	segmentPath := filepath.Join(s.path, strconv.FormatUint(s.curr_segment, 10))

	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}
	fileSize := int(fileInfo.Size())

	entry := s.createEntry(key, value)

	_, err = file.Write(entry.toBytes())
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	offset := fileSize
	s.keydir[key] = storeKeyInfo{
		file_id:   s.curr_segment,
		value_sz:  len(value),
		value_pos: offset,
		tstamp:    entry.tstamp,
	}

	return nil
}

func (s *store) createTombstoneEntry(key string) *storeEntry {
	return &storeEntry{
		crc:      0,
		tstamp:   uint32(time.Now().Unix()),
		ksz:      uint32(len(key)),
		value_sz: 0,
		key:      []byte(key),
		value:    []byte{},
	}
}

func (s *store) delete(key string) error {
	if _, ok := s.keydir[key]; !ok {
		return nil // TODO - Maybe add an error? Idk yet
	}

	segmentPath := filepath.Join(s.path, strconv.FormatUint(s.curr_segment, 10))
	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}
	defer file.Close()

	entry := s.createTombstoneEntry(key)
	_, err = file.Write(entry.toBytes())
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}
	delete(s.keydir, key)

	return nil
}

func (s *store) keys() []string {
	keys := []string{}
	for key := range s.keydir {
		keys = append(keys, key)
	}
	return keys
}
