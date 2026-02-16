package kvimpl

import (
	"context"
	"errors"

	syncsdk "github.com/anyproto/any-sync-sdk"

	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
)

var ErrKeyNotFound = errors.New("key not found")

type kvImpl struct {
	inner keyvaluestorage.Storage
}

// NewKeyValue creates a new KeyValue implementation wrapping a keyvaluestorage.Storage.
func NewKeyValue(inner keyvaluestorage.Storage) syncsdk.KeyValue {
	return &kvImpl{inner: inner}
}

func (kv *kvImpl) Set(ctx context.Context, key string, value []byte) error {
	return kv.inner.Set(ctx, key, value)
}

func (kv *kvImpl) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	var found bool
	err := kv.inner.GetAll(ctx, key, func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		if len(values) == 0 {
			return nil
		}
		// Take the last value (latest by timestamp)
		latest := values[len(values)-1]
		decrypted, err := decryptor(latest)
		if err != nil {
			return err
		}
		result = decrypted
		found = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrKeyNotFound
	}
	return result, nil
}

func (kv *kvImpl) Iterate(ctx context.Context, fn func(key string, value []byte) bool) error {
	return kv.inner.Iterate(ctx, func(decryptor keyvaluestorage.Decryptor, key string, values []innerstorage.KeyValue) (bool, error) {
		if len(values) == 0 {
			return true, nil
		}
		// Decrypt the last value (latest)
		latest := values[len(values)-1]
		decrypted, err := decryptor(latest)
		if err != nil {
			return true, nil // skip entries that can't be decrypted
		}
		return fn(key, decrypted), nil
	})
}
