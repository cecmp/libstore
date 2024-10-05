package libstore

import (
	"context"
)

// Ops defines the interface for data operations.
type Ops interface {
	// Create creates a new key.
	// It returns an error if the key already exists or if there is an issue creating the key.
	Create(ctx context.Context, key string) error
	// ReadAll reads the entire content of the given key.
	// It returns the content as a byte slice or an error if the content cannot be read.
	ReadAll(ctx context.Context, key string) ([][]byte, error)
	// Read reads the last entry of the given key.
	// It returns the last entry or an error if the file cannot be read.
	Read(ctx context.Context, key string) ([]byte, error)
	// Put replaces an entry to the file with the given key.
	// It returns an error if the file cannot be opened or written to.
	Put(ctx context.Context, key string, entry []byte) error
	// Delete deletes the given key and associated content.
	// It returns an error if the key or associated content cannot be deleted.
	Delete(ctx context.Context, key string) error
	// List lists all keys in the bucket-scope.
	// It returns a slice of key names or an error if the bucket-scope cannot be read.
	List(ctx context.Context) ([]string, error)
}

type (
	LocationError    string
	KeyError         string
	EntryError       string
	OpsInternalError string
	KeyNotFoundError string
)

func (e LocationError) Error() string {
	return "libstore: " + (string)(e)
}
func (e KeyError) Error() string {
	return "libstore: " + (string)(e)
}
func (e EntryError) Error() string {
	return "libstore: " + (string)(e)
}
func (e OpsInternalError) Error() string {
	return "libstore: " + (string)(e)
}
func (e KeyNotFoundError) Error() string {
	return "libstore: " + string(e)
}
