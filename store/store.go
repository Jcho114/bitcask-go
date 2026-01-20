package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type Store interface {
	RunReplit() error
}

type store struct {
	path         string
	keydir       storeKeyDir
	segments     []storeSegment
	curr_segment uint64
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

	err := s.createNewSegment()
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	return s, nil
}

func (s *store) createNewSegment() error {
	segmentPath := filepath.Join(s.path, strconv.FormatUint(s.curr_segment, 10))

	file, err := os.Create(segmentPath)
	if err != nil {
		return fmt.Errorf("open: %v", err)
	}
	file.Close()

	segmentFileInfo, err := os.Stat(segmentPath)
	if err != nil {
		return fmt.Errorf("open: %v", err)
	}

	s.segments = append(s.segments, storeSegment{path: s.path, fileInfo: segmentFileInfo})
	s.curr_segment = uint64(len(s.segments) - 1)
	return nil
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
	tstamp, offset, err := s.segments[s.curr_segment].putEntry(key, value)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	s.keydir.putInfo(key, storeKeyInfo{
		file_id:   s.curr_segment,
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

	err := s.segments[s.curr_segment].deleteEntry(key)
	if err != nil {
		return fmt.Errorf("delete: %v", err)
	}

	s.keydir.removeInfo(key)

	return nil
}

func (s *store) keys() []string {
	return s.keydir.getKeys()
}
