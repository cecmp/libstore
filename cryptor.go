package libstore

import (
	"context"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/cecmp/libcipher"
)

const tsFormat = "2006-01-02 15:04:05.999999999 -0700 MST"

type CryptStore struct {
	storeOps  Ops
	encryptor libcipher.Encryptor
	decryptor libcipher.Decryptor
}

type (
	ValidationError string
	DecryptionError string
	TimestampError  string
)

func (e ValidationError) Error() string {
	return fmt.Sprintf("libstore/ops: validation error: %s", string(e))
}
func (e DecryptionError) Error() string {
	return fmt.Sprintf("libstore/ops: decryption error: %s", string(e))
}

func (e TimestampError) Error() string {
	return fmt.Sprintf("libstore/ops: timestamp error: %s", string(e))
}

// NewCryptStoreCBC initializes a new CryptStore instance using CBC-HMAC encryption.
//
// Parameters:
//   - ops: An instance of Ops that defines the underlying storage operations.
//   - encryptionKey: A byte slice representing the encryption key used for the CBC encryption.
//   - integrityKey: A byte slice representing the key used for HMAC integrity checks.
//   - calculateMAC: A function returning a new hash.Hash used for generating the MAC.
//   - rand: An io.Reader used as a source of randomness, typically crypto/rand.Reader.
//
// Returns:
//   - An Ops instance that wraps the provided storage operations with CBC-HMAC encryption.
//   - An error if the encryption or decryption setup fails.
//
// The function sets up an encryptor and decryptor using the specified keys and MAC function.
// It then returns a CryptStore that applies these operations on the provided Ops.
func NewCryptStoreCBC(ops Ops, encyptionKey []byte, integrityKey []byte, calculateMAC func() hash.Hash, rand io.Reader) (Ops, error) {
	encryptor, err := libcipher.NewCBCHMACEncryptor(encyptionKey, integrityKey, calculateMAC, rand)
	if err != nil {
		return nil, err
	}
	decryptor, err := libcipher.NewCBCHMACDecryptor(encyptionKey, integrityKey, calculateMAC)
	if err != nil {
		return nil, err
	}
	return CryptStore{storeOps: ops, encryptor: encryptor, decryptor: decryptor}, nil
}

// NewCryptStoreGCM initializes a new CryptStore instance using GCM encryption.
//
// Parameters:
//   - ops: An instance of Ops that defines the underlying storage operations.
//   - encryptionKey: A byte slice representing the encryption key used for GCM encryption.
//   - rand: An io.Reader used as a source of randomness, typically crypto/rand.Reader.
//
// Returns:
//   - An Ops instance that wraps the provided storage operations with GCM encryption.
//   - An error if the encryption or decryption setup fails.
//
// The function sets up an encryptor and decryptor using the specified encryption key.
// It then returns a CryptStore that applies these operations on the provided Ops.
func NewCryptStoreGCM(ops Ops, encyptionKey []byte, rand io.Reader) (Ops, error) {
	encryptor, err := libcipher.NewGCMEncryptor(encyptionKey, rand)
	if err != nil {
		return nil, err
	}
	decryptor, err := libcipher.NewGCMDecryptor(encyptionKey)
	if err != nil {
		return nil, err
	}

	return CryptStore{storeOps: ops, encryptor: encryptor, decryptor: decryptor}, nil
}

// Put implements libstore.Ops.
func (m CryptStore) Put(ctx context.Context, key string, entry []byte) error {
	ts := []byte(time.Now().UTC().Format(tsFormat))
	vault, err := m.encryptor.Crypt(entry, ts)
	if err != nil {
		return fmt.Errorf("%w: %w", DecryptionError("failed to encrypt entry"), err)
	}
	err = m.storeOps.Put(ctx, key, vault)
	if err != nil {
		return err
	}

	return nil
}

// Create implements libstore.Ops.
func (m CryptStore) Create(ctx context.Context, key string) error {
	err := m.storeOps.Create(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// Delete implements libstore.Ops.
func (m CryptStore) Delete(ctx context.Context, key string) error {
	err := m.storeOps.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// List implements libstore.Ops.
func (m CryptStore) List(ctx context.Context) ([]string, error) {
	res, err := m.storeOps.List(ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Read implements libstore.Ops.
func (m CryptStore) Read(ctx context.Context, key string) ([]byte, error) {
	vault, err := m.storeOps.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	res, meta, err := m.decryptor.Crypt(vault)
	if err != nil {
		return nil, err
	}
	ts, err := time.Parse(tsFormat, string(meta))
	if err != nil {
		return nil, err
	}
	if ts.After(time.Now().UTC()) {
		return nil, ValidationError("failed to validate sealing")
	}
	return res, nil
}

// ReadAll implements libstore.Ops.
func (m CryptStore) ReadAll(ctx context.Context, key string) ([][]byte, error) {
	vaults, err := m.storeOps.ReadAll(ctx, key)
	if err != nil {
		return nil, err
	}
	res := make([][]byte, len(vaults))
	var meta []byte
	for i := range vaults {
		res[i], meta, err = m.decryptor.Crypt(vaults[i])
		if err != nil {
			return nil, err
		}
		ts, err := time.Parse(tsFormat, string(meta))
		if err != nil {
			return nil, err
		}
		if ts.After(time.Now().UTC()) {
			return nil, ValidationError("failed to validate sealing")
		}
	}
	return res, nil
}
