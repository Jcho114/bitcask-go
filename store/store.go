package store

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/google/uuid"
)

// TODO - Make configurable
const MaximumSegmentSize = 512 // 512 bytes for testing

type Store interface {
	RunReplit() error
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
	Keys() []string
	Merge() error
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

	err := s.initializeFromScratch()
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}
	return s, nil
}

func (s *store) initializeFromScratch() error {
	return s.createNewSegment()
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
			if err == io.EOF {
				timestamps[segmentPath] = math.MaxUint32
				continue
			}
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
					segment:   &segment,
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

func (s *store) Get(key string) ([]byte, error) {
	return s.get(key)
}

func (s *store) get(key string) ([]byte, error) {
	info, ok := s.keydir.getInfo(key)
	if !ok {
		return nil, nil
	}

	entry, err := info.segment.getEntry(key, &info)
	if err != nil {
		return nil, fmt.Errorf("get: %v", err)
	}
	return entry.value, nil
}

func (s *store) Put(key string, value []byte) error {
	return s.put(key, value, nil)
}

func (s *store) put(key string, value []byte, timestamp *uint32) error {
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

	tstamp, offset, err := s.currSegment().putEntry(key, value, timestamp)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}

	s.keydir.putInfo(key, storeKeyInfo{
		segment:   s.currSegment(),
		value_sz:  len(value),
		value_pos: offset,
		tstamp:    tstamp,
	})

	return nil
}

func (s *store) Delete(key string) error {
	return s.delete(key, nil)
}

func (s *store) delete(key string, timestamp *uint32) error {
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

	err = s.currSegment().deleteEntry(key, timestamp)
	if err != nil {
		return fmt.Errorf("delete: %v", err)
	}

	s.keydir.removeInfo(key)

	return nil
}

func (s *store) Keys() []string {
	return s.keys()
}

func (s *store) keys() []string {
	return s.keydir.getKeys()
}

func (s *store) Merge() error {
	return s.merge()
}

// TODO - Double check correctness, I am lowkey kinda tired rn
func (s *store) merge() error {
	type tempInfo struct {
		value  []byte
		tstamp uint32
	}
	tempKeyMap := make(map[string]tempInfo)

	for i := 0; i < len(s.segments)-1; i++ {
		segment := s.segments[i]
		entries, err := readEntriesFromFile(segment.path)
		if err != nil {
			return fmt.Errorf("merge: %v", err)
		}

		for _, entry := range entries {
			if entry.value_sz == 0 {
				delete(tempKeyMap, string(entry.key))
			} else {
				tempKeyMap[string(entry.key)] = tempInfo{
					value:  entry.value,
					tstamp: entry.tstamp,
				}
			}
		}
	}

	tempStore := &store{
		path:     s.path,
		keydir:   make(map[string]storeKeyInfo),
		segments: make([]storeSegment, 0),
	}
	tempStore.initializeFromScratch()
	for key, info := range tempKeyMap {
		tempStore.put(key, info.value, &info.tstamp)
	}

	for i := 0; i < len(s.segments)-1; i++ {
		os.Remove(s.segments[i].path)
	}

	currSegment := s.currSegment()
	s.segments = append(tempStore.segments, *currSegment)

	for key, info := range tempStore.keydir {
		currInfo, ok := s.keydir.getInfo(key)
		// entry has been added, deleted, or updated in the current segment logs
		if !ok || currInfo.tstamp > info.tstamp {
			continue
		}
		s.keydir.putInfo(key, info)
	}

	return nil
}
