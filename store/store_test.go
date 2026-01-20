package store

import (
	"os"
	"testing"
)

const TestStorePath = "./test_store"

func setupTestStore(t *testing.T) Store {
	s, err := OpenStore(TestStorePath)
	if err != nil {
		t.Fatalf("Failed to open test store: %v", err)
	}
	return s
}

func teardownTestStore(t *testing.T, s Store) {
	err := os.RemoveAll(TestStorePath)
	if err != nil {
		t.Fatalf("Failed to remove test store: %v", err)
	}
}

func TestNullGet(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	value, err := s.Get("nonexistent_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected NULL for nonexistent key, got: %v", value)
	}
}

func TestPutThenGet(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	err := s.Put("test_key", []byte("test_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	value, err := s.Get("test_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if string(value) != "test_value" {
		t.Fatalf("Expected 'test_value', got: %s", value)
	}
}

func TestOverridePut(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	err := s.Put("test_key", []byte("initial_value"))
	if err != nil {
		t.Fatalf("Initial PUT operation failed: %v", err)
	}

	err = s.Put("test_key", []byte("overridden_value"))
	if err != nil {
		t.Fatalf("Override PUT operation failed: %v", err)
	}

	value, err := s.Get("test_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if string(value) != "overridden_value" {
		t.Fatalf("Expected 'overridden_value', got: %s", value)
	}
}

func TestDeleteKey(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	err := s.Put("test_key", []byte("test_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.Delete("test_key")
	if err != nil {
		t.Fatalf("DELETE operation failed: %v", err)
	}

	value, err := s.Get("test_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected NULL after deletion, got: %v", value)
	}
}

func TestKeysRetrieval(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	keysToInsert := []string{"key1", "key2", "key3"}
	for _, key := range keysToInsert {
		err := s.Put(key, []byte("value"))
		if err != nil {
			t.Fatalf("PUT operation failed for key %s: %v", key, err)
		}
	}

	retrievedKeys := s.Keys()
	if len(retrievedKeys) != len(keysToInsert) {
		t.Fatalf("Expected %d keys, got %d", len(keysToInsert), len(retrievedKeys))
	}

	keyMap := make(map[string]bool)
	for _, key := range retrievedKeys {
		keyMap[key] = true
	}

	for _, key := range keysToInsert {
		if !keyMap[key] {
			t.Fatalf("Expected key %s not found in retrieved keys", key)
		}
	}
}

func TestPersistence(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	err := s.Put("persistent_key", []byte("persistent_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.Put("deleted_key", []byte("to_be_deleted"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.Delete("deleted_key")
	if err != nil {
		t.Fatalf("DELETE operation failed: %v", err)
	}

	s2 := setupTestStore(t)
	defer teardownTestStore(t, s2)

	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	value, err := s2.Get("persistent_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if string(value) != "persistent_value" {
		t.Fatalf("Expected 'persistent_value', got: %s", value)
	}
}

func TestPersistenceEmpty(t *testing.T) {
	s := setupTestStore(t)
	defer teardownTestStore(t, s)

	s2 := setupTestStore(t)
	defer teardownTestStore(t, s2)

	keys := s2.Keys()
	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys in reopened empty store, got: %d", len(keys))
	}
}

// TODO - Add Tests For Merge
