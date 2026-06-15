package auth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/anyproto/any-sync/util/crypto"
)

// FileProviderConfig configures the default file-backed Provider.
type FileProviderConfig struct {
	// Path to the wallet file. Required.
	Path string

	// Passkey, when non-empty, encrypts the wallet at rest with
	// AES-256-GCM keyed by PBKDF2-HMAC-SHA256 (600k iterations,
	// 16-byte salt). Leave empty for a plain-text wallet.
	Passkey string

	// Mnemonic, when non-empty, seeds wallet creation with an existing
	// BIP-39 phrase instead of generating a fresh one — the restore /
	// second-device path (the device key is still freshly generated).
	// When the wallet file already exists the stored phrase must match,
	// otherwise NewFileProvider returns ErrMnemonicMismatch.
	Mnemonic string

	// Index is the account derivation index used together with
	// Mnemonic. Ignored when the wallet file already exists.
	Index uint32
}

// ErrInvalidMnemonic wraps BIP-39 validation failures of a supplied
// mnemonic phrase.
var ErrInvalidMnemonic = errors.New("invalid mnemonic")

// ErrMnemonicMismatch is returned when FileProviderConfig.Mnemonic is
// set but an existing wallet file stores a different phrase.
var ErrMnemonicMismatch = errors.New("wallet exists with a different mnemonic")

// ErrPasskeyRequired is returned when the wallet file is encrypted but
// no passkey was supplied.
var ErrPasskeyRequired = errors.New("wallet is encrypted but no passkey provided")

// ErrWrongPasskey is returned when the supplied passkey fails to
// decrypt the wallet (or the file is corrupted).
var ErrWrongPasskey = errors.New("decrypt wallet: wrong passkey or corrupted file")

// FileProvider is a Provider backed by a JSON wallet file on disk.
// Generated on first use, loaded on subsequent launches. Exposes
// Mnemonic so the caller can display it once on first generation for
// the user to back up.
type FileProvider struct {
	path    string
	passkey string
	w       *wallet
	created bool
}

// NewFileProvider returns the default provider. On first use it
// generates a fresh mnemonic (or adopts cfg.Mnemonic when set) plus a
// fresh device key, writes them to Path, and marks Created() true. On
// subsequent runs it loads the existing wallet; Created() returns
// false, and a set cfg.Mnemonic must match the stored phrase.
func NewFileProvider(cfg FileProviderConfig) (*FileProvider, error) {
	if cfg.Path == "" {
		return nil, errors.New("wallet path is required")
	}
	path := cfg.Path
	if cfg.Mnemonic != "" {
		if _, err := crypto.Mnemonic(cfg.Mnemonic).Bytes(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidMnemonic, err)
		}
	}

	w, err := loadWallet(path, cfg.Passkey)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		w, err = generateWallet(cfg.Mnemonic, cfg.Index)
		if err != nil {
			return nil, err
		}
		if err := saveWallet(path, cfg.Passkey, w); err != nil {
			return nil, fmt.Errorf("save wallet: %w", err)
		}
		created = true
	} else if err != nil {
		return nil, err
	} else if cfg.Mnemonic != "" && cfg.Mnemonic != w.Mnemonic {
		return nil, ErrMnemonicMismatch
	}

	return &FileProvider{path: path, passkey: cfg.Passkey, w: w, created: created}, nil
}

// Path returns the resolved wallet-file path.
func (p *FileProvider) Path() string { return p.path }

// Created reports whether the wallet was freshly generated on this
// call to NewFileProvider. Callers typically use it to display the
// mnemonic to the user once ("write this down") the first time the
// SDK is initialized.
func (p *FileProvider) Created() bool { return p.created }

// Mnemonic returns the BIP-39 phrase backing this wallet. Sensitive —
// only surface to the user on first launch.
func (p *FileProvider) Mnemonic() string { return p.w.Mnemonic }

// AccountKey derives and returns the account private key.
func (p *FileProvider) AccountKey(_ context.Context) ([]byte, error) {
	res, err := crypto.Mnemonic(p.w.Mnemonic).DeriveKeys(p.w.Index)
	if err != nil {
		return nil, fmt.Errorf("derive account key: %w", err)
	}
	return res.Identity.Raw()
}

// DeviceKey returns the stored device private key (raw Ed25519).
func (p *FileProvider) DeviceKey(_ context.Context) ([]byte, error) {
	return p.w.DeviceKey, nil
}

type wallet struct {
	Mnemonic  string
	DeviceKey []byte
	Index     uint32
}

// walletEnvelope is the on-disk JSON shape. When encrypted, Payload
// is nil and Crypt is populated; when plain, Crypt is nil and Payload
// carries the walletPayload directly.
type walletEnvelope struct {
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Crypt   *walletCrypt    `json:"crypt,omitempty"`
}

type walletCrypt struct {
	KDF        string `json:"kdf"` // "pbkdf2-sha256/600000"
	Salt       string `json:"salt"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type walletPayload struct {
	Mnemonic  string `json:"mnemonic"`
	DeviceKey string `json:"deviceKey"` // base64
	Index     uint32 `json:"index,omitempty"`
}

const (
	walletVersion    = 1
	pbkdf2Iterations = 600_000
	pbkdf2KeyLen     = 32
	saltLen          = 16
)

// generateWallet builds a fresh wallet. An empty mnemonic means
// "generate one"; a supplied mnemonic is adopted as-is (already
// validated by the caller). The device key is always fresh.
func generateWallet(mnemonic string, index uint32) (*wallet, error) {
	if mnemonic == "" {
		m, err := crypto.NewMnemonicGenerator().WithWordCount(12)
		if err != nil {
			return nil, err
		}
		mnemonic = string(m)
		index = 0
	}
	devPriv, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return nil, err
	}
	devBytes, err := devPriv.Raw()
	if err != nil {
		return nil, err
	}
	return &wallet{Mnemonic: mnemonic, DeviceKey: devBytes, Index: index}, nil
}

func loadWallet(path, passkey string) (*wallet, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env walletEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("parse wallet: %w", err)
	}
	if env.Version != walletVersion {
		return nil, fmt.Errorf("unsupported wallet version %d", env.Version)
	}

	var p walletPayload
	if env.Crypt != nil {
		if passkey == "" {
			return nil, ErrPasskeyRequired
		}
		plain, err := decryptPayload(env.Crypt, passkey)
		if err != nil {
			return nil, err
		}
		p = plain
	} else {
		if passkey != "" {
			return nil, errors.New("wallet is plain but a passkey was provided")
		}
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return nil, fmt.Errorf("parse payload: %w", err)
		}
	}

	devKey, err := base64.StdEncoding.DecodeString(p.DeviceKey)
	if err != nil {
		return nil, fmt.Errorf("decode device key: %w", err)
	}
	return &wallet{Mnemonic: p.Mnemonic, DeviceKey: devKey, Index: p.Index}, nil
}

func saveWallet(path, passkey string, w *wallet) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	p := walletPayload{
		Mnemonic:  w.Mnemonic,
		DeviceKey: base64.StdEncoding.EncodeToString(w.DeviceKey),
		Index:     w.Index,
	}

	env := walletEnvelope{Version: walletVersion}
	if passkey == "" {
		body, err := json.Marshal(p)
		if err != nil {
			return err
		}
		env.Payload = body
	} else {
		crypt, err := encryptPayload(p, passkey)
		if err != nil {
			return err
		}
		env.Crypt = crypt
	}

	body, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o600)
}

func encryptPayload(p walletPayload, passkey string) (*walletCrypt, error) {
	plaintext, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	key, err := pbkdf2.Key(sha256.New, passkey, salt, pbkdf2Iterations, pbkdf2KeyLen)
	if err != nil {
		return nil, err
	}

	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ct := gcm.Seal(nil, nonce, plaintext, nil)

	return &walletCrypt{
		KDF:        fmt.Sprintf("pbkdf2-sha256/%d", pbkdf2Iterations),
		Salt:       base64.StdEncoding.EncodeToString(salt),
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(ct),
	}, nil
}

func decryptPayload(c *walletCrypt, passkey string) (walletPayload, error) {
	salt, err := base64.StdEncoding.DecodeString(c.Salt)
	if err != nil {
		return walletPayload{}, err
	}
	nonce, err := base64.StdEncoding.DecodeString(c.Nonce)
	if err != nil {
		return walletPayload{}, err
	}
	ct, err := base64.StdEncoding.DecodeString(c.Ciphertext)
	if err != nil {
		return walletPayload{}, err
	}

	key, err := pbkdf2.Key(sha256.New, passkey, salt, pbkdf2Iterations, pbkdf2KeyLen)
	if err != nil {
		return walletPayload{}, err
	}

	gcm, err := newGCM(key)
	if err != nil {
		return walletPayload{}, err
	}
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return walletPayload{}, ErrWrongPasskey
	}

	var p walletPayload
	if err := json.Unmarshal(pt, &p); err != nil {
		return walletPayload{}, err
	}
	return p, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
