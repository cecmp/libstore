package libstore

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryOps is an in-memory implementation of the Ops interface.
type InMemoryOps struct {
	mu    sync.RWMutex
	store map[string][][]byte
}

// NewInMemoryOps creates a new InMemoryOps instance.
func NewInMemoryOps() *InMemoryOps {
	return &InMemoryOps{
		store: make(map[string][][]byte),
	}
}

// Create creates a new key in the store.
func (ops *InMemoryOps) Create(ctx context.Context, key string) error {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	if _, exists := ops.store[key]; exists {
		return KeyError(fmt.Sprintf("key %s already exists", key))
	}

	ops.store[key] = [][]byte{}
	return nil
}

// ReadWhole reads the entire content associated with the key.
func (ops *InMemoryOps) ReadAll(ctx context.Context, key string) ([][]byte, error) {
	ops.mu.RLock()
	defer ops.mu.RUnlock()

	data, exists := ops.store[key]
	if !exists {
		return nil, KeyNotFoundError(fmt.Sprintf("key %s not found", key))
	}

	return data, nil
}

// ReadLast reads the last entry associated with the key.
func (ops *InMemoryOps) Read(ctx context.Context, key string) ([]byte, error) {
	ops.mu.RLock()
	defer ops.mu.RUnlock()

	data, exists := ops.store[key]
	if !exists {
		return nil, KeyNotFoundError(fmt.Sprintf("key %s not found", key))
	}

	if len(data) == 0 {
		return nil, EntryError(fmt.Sprintf("no entries found for key %s", key))
	}

	return data[len(data)-1], nil
}

// Put replaces all entries associated with the key with a single entry.
func (ops *InMemoryOps) Put(ctx context.Context, key string, entry []byte) error {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	if _, exists := ops.store[key]; !exists {
		return KeyNotFoundError(fmt.Sprintf("key %s not found", key))
	}

	ops.store[key] = [][]byte{entry}
	return nil
}

// Delete deletes the key and all its associated entries.
func (ops *InMemoryOps) Delete(ctx context.Context, key string) error {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	if _, exists := ops.store[key]; !exists {
		return KeyNotFoundError(fmt.Sprintf("key %s not found", key))
	}

	delete(ops.store, key)
	return nil
}

// List lists all keys in the store.
func (ops *InMemoryOps) List(ctx context.Context) ([]string, error) {
	ops.mu.RLock()
	defer ops.mu.RUnlock()

	var keys []string
	for key := range ops.store {
		keys = append(keys, key)
	}

	return keys, nil
}
