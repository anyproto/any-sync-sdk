package auth

import (
	"context"
	"errors"
	"slices"
)

// KeyConfig builds a Provider from keys the host already holds: an
// account key from DeriveAccount and a stored device key, both raw
// Ed25519.
type KeyConfig struct {
	AccountKey []byte
	DeviceKey  []byte
}

// NewKeyProvider returns a Provider serving cfg's keys as given.
func NewKeyProvider(cfg KeyConfig) (Provider, error) {
	if len(cfg.AccountKey) == 0 {
		return nil, errors.New("account key is required")
	}
	if len(cfg.DeviceKey) == 0 {
		return nil, errors.New("device key is required")
	}
	return keyProvider{account: slices.Clone(cfg.AccountKey), device: slices.Clone(cfg.DeviceKey)}, nil
}

type keyProvider struct{ account, device []byte }

func (p keyProvider) AccountKey(context.Context) ([]byte, error) { return slices.Clone(p.account), nil }
func (p keyProvider) DeviceKey(context.Context) ([]byte, error)  { return slices.Clone(p.device), nil }
