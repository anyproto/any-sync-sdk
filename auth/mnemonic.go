package auth

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/util/crypto"
)

// MnemonicConfig builds a Provider from a BIP-39 mnemonic using the
// SLIP-10 path m/44'/2046'/[Index]' for the account identity.
type MnemonicConfig struct {
	// Mnemonic is the BIP-39 phrase (typically 12 or 24 words).
	Mnemonic string
	// Index is the account index. Defaults to 0.
	Index uint32
	// DeviceKey is the persisted device private key in raw Ed25519
	// form (64 bytes). Callers own device-key lifecycle: generate on
	// first run with GenerateDeviceKey, store securely, pass back
	// every init.
	DeviceKey []byte
}

// NewMnemonicProvider validates the mnemonic and returns a Provider
// that derives the account key on demand.
func NewMnemonicProvider(cfg MnemonicConfig) (Provider, error) {
	m := crypto.Mnemonic(cfg.Mnemonic)
	if _, err := m.Bytes(); err != nil {
		return nil, fmt.Errorf("invalid mnemonic: %w", err)
	}
	if len(cfg.DeviceKey) == 0 {
		return nil, fmt.Errorf("device key is required")
	}
	return &mnemonicProvider{
		mnemonic:  m,
		index:     cfg.Index,
		deviceKey: cfg.DeviceKey,
	}, nil
}

// GenerateMnemonic returns a fresh 12-word BIP-39 mnemonic.
func GenerateMnemonic() (string, error) {
	m, err := crypto.NewMnemonicGenerator().WithWordCount(12)
	if err != nil {
		return "", err
	}
	return string(m), nil
}

// GenerateDeviceKey returns a fresh Ed25519 device-key in raw form
// (64 bytes). Callers store the result in their secure storage and
// pass it back via MnemonicConfig.DeviceKey on every init.
func GenerateDeviceKey(_ context.Context) ([]byte, error) {
	priv, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return nil, err
	}
	return priv.Raw()
}

type mnemonicProvider struct {
	mnemonic  crypto.Mnemonic
	index     uint32
	deviceKey []byte
}

func (p *mnemonicProvider) AccountKey(_ context.Context) ([]byte, error) {
	res, err := p.mnemonic.DeriveKeys(p.index)
	if err != nil {
		return nil, fmt.Errorf("derive account key: %w", err)
	}
	return res.Identity.Raw()
}

func (p *mnemonicProvider) DeviceKey(_ context.Context) ([]byte, error) {
	return p.deviceKey, nil
}
