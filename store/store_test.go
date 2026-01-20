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

	value, err := s.get("nonexistent_key")
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

	err := s.put("test_key", []byte("test_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	value, err := s.get("test_key")
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

	err := s.put("test_key", []byte("initial_value"))
	if err != nil {
		t.Fatalf("Initial PUT operation failed: %v", err)
	}

	err = s.put("test_key", []byte("overridden_value"))
	if err != nil {
		t.Fatalf("Override PUT operation failed: %v", err)
	}

	value, err := s.get("test_key")
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

	err := s.put("test_key", []byte("test_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.delete("test_key")
	if err != nil {
		t.Fatalf("DELETE operation failed: %v", err)
	}

	value, err := s.get("test_key")
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
		err := s.put(key, []byte("value"))
		if err != nil {
			t.Fatalf("PUT operation failed for key %s: %v", key, err)
		}
	}

	retrievedKeys := s.keys()
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

	err := s.put("persistent_key", []byte("persistent_value"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.put("deleted_key", []byte("to_be_deleted"))
	if err != nil {
		t.Fatalf("PUT operation failed: %v", err)
	}

	err = s.delete("deleted_key")
	if err != nil {
		t.Fatalf("DELETE operation failed: %v", err)
	}

	s2 := setupTestStore(t)
	defer teardownTestStore(t, s2)

	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	value, err := s2.get("persistent_key")
	if err != nil {
		t.Fatalf("GET operation failed: %v", err)
	}
	if string(value) != "persistent_value" {
		t.Fatalf("Expected 'persistent_value', got: %s", value)
	}
}
