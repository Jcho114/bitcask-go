package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/google/uuid"
)

// TODO - Make configurable
const MaximumSegmentSize = 512 // 512 bytes for testing

type Store interface {
	RunReplit() error
	get(key string) ([]byte, error)
	put(key string, value []byte) error
	delete(key string) error
	keys() []string
}

type store struct {
	path     string
	keydir   storeKeyDir
	segments []storeSegment
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
		path:     path,
		keydir:   make(map[string]storeKeyInfo),
		segments: make([]storeSegment, 0),
	}

	if directoryExists {
		err := s.initializeFromDirectory()
		if err != nil {
			return nil, fmt.Errorf("open: %v", err)
		}
		return s, nil
	}

	err := s.createNewSegment()
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	return s, nil
}

func (s *store) initializeFromDirectory() error {
	entries, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	timestamps := map[string]uint32{}
	for _, entry := range entries {
		segmentPath := filepath.Join(s.path, entry.Name())
		s.segments = append(s.segments, storeSegment{path: segmentPath})
		firstEntry, err := readFirstEntry(segmentPath)
		if err != nil {
			return err
		}
		timestamps[segmentPath] = firstEntry.tstamp
	}

	slices.SortFunc(s.segments, func(a, b storeSegment) int {
		atstamp := timestamps[a.path]
		btstamp := timestamps[b.path]
		return int(atstamp - btstamp)
	})

	for _, segment := range s.segments {
		entries, err := readEntriesFromFile(segment.path)
		if err != nil {
			return err
		}

		currSize := 0
		for _, entry := range entries {
			if entry.value_sz == 0 {
				s.keydir.removeInfo(string(entry.key))
			} else {
				s.keydir.putInfo(string(entry.key), storeKeyInfo{
					file_id:   uint64(len(s.segments) - 1),
					value_sz:  int(entry.value_sz),
					value_pos: int(currSize),
					tstamp:    entry.tstamp,
				})
			}
			currSize += 16 + len(entry.key) + len(entry.value)
		}
	}

	return nil
}

func (s *store) createNewSegment() error {
	filename := uuid.New().String()
	segmentPath := filepath.Join(s.path, filename)

	file, err := os.Create(segmentPath)
	if err != nil {
		return fmt.Errorf("open: %v", err)
	}
	file.Close()

	s.segments = append(s.segments, storeSegment{path: segmentPath})
	return nil
}

func (s *store) currSegment() *storeSegment {
	return &s.segments[len(s.segments)-1]
}

func (s *store) get(key string) ([]byte, error) {
	info, ok := s.keydir.getInfo(key)
	if !ok {
		return nil, nil
	}

	entry, err := s.segments[info.file_id].getEntry(key, &info)
	if err != nil {
		return nil, fmt.Errorf("get: %v", err)
	}
	return entry.value, nil
}

func (s *store) put(key string, value []byte) error {
	fileInfo, err := os.Stat(s.currSegment().path)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	if fileInfo.Size() > MaximumSegmentSize {
		err := s.createNewSegment()
		if err != nil {
			return fmt.Errorf("put: %v", err)
		}
	}

	tstamp, offset, err := s.currSegment().putEntry(key, value)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	s.keydir.putInfo(key, storeKeyInfo{
		file_id:   uint64(len(s.segments) - 1),
		value_sz:  len(value),
		value_pos: offset,
		tstamp:    tstamp,
	})

	return nil
}

func (s *store) delete(key string) error {
	if _, ok := s.keydir.getInfo(key); !ok {
		return nil // TODO - Maybe add an error? Idk yet
	}

	fileInfo, err := os.Stat(s.currSegment().path)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	if fileInfo.Size() > MaximumSegmentSize {
		err := s.createNewSegment()
		if err != nil {
			return fmt.Errorf("put: %v", err)
		}
	}

	err = s.currSegment().deleteEntry(key)
	if err != nil {
		return fmt.Errorf("delete: %v", err)
	}

	s.keydir.removeInfo(key)

	return nil
}

func (s *store) keys() []string {
	return s.keydir.getKeys()
}

func (s *store) merge() error {
	// TODO - Implement merge functionality
	// Run replay of first n-1 segments
	// Take live entries and write to new segment
	// Delete old segments, rename new segment
	return nil
}
