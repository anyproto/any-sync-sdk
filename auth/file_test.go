package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
)

func TestFileProvider_PlainRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallet.key")

	p1, err := NewFileProvider(FileProviderConfig{Path: path})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if !p1.Created() {
		t.Fatal("first open should have Created() = true")
	}
	mnemonic := p1.Mnemonic()
	acct1, err := p1.AccountKey(context.Background())
	if err != nil {
		t.Fatalf("account key: %v", err)
	}
	dev1, err := p1.DeviceKey(context.Background())
	if err != nil {
		t.Fatalf("device key: %v", err)
	}

	p2, err := NewFileProvider(FileProviderConfig{Path: path})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if p2.Created() {
		t.Fatal("second open should have Created() = false")
	}
	if p2.Mnemonic() != mnemonic {
		t.Fatal("mnemonic changed across loads")
	}
	acct2, _ := p2.AccountKey(context.Background())
	dev2, _ := p2.DeviceKey(context.Background())
	if string(acct1) != string(acct2) {
		t.Fatal("account key not stable across loads")
	}
	if string(dev1) != string(dev2) {
		t.Fatal("device key not stable across loads")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("wallet file should be 0600, got %o", info.Mode().Perm())
	}
}

func TestFileProvider_EncryptedRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallet.key")
	const passkey = "hunter2"

	p1, err := NewFileProvider(FileProviderConfig{Path: path, Passkey: passkey})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	mnemonic := p1.Mnemonic()

	// Reopen with correct passkey.
	p2, err := NewFileProvider(FileProviderConfig{Path: path, Passkey: passkey})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if p2.Mnemonic() != mnemonic {
		t.Fatal("mnemonic changed")
	}

	// Wrong passkey fails cleanly.
	if _, err := NewFileProvider(FileProviderConfig{Path: path, Passkey: "wrong"}); err == nil {
		t.Fatal("expected error with wrong passkey")
	}

	// No passkey on an encrypted file fails cleanly.
	if _, err := NewFileProvider(FileProviderConfig{Path: path}); err == nil {
		t.Fatal("expected error with no passkey")
	}
}

func TestFileProvider_SeedFromMnemonic(t *testing.T) {
	dir := t.TempDir()

	// Reference wallet: freshly generated.
	ref, err := NewFileProvider(FileProviderConfig{Path: filepath.Join(dir, "ref.key")})
	if err != nil {
		t.Fatalf("generate reference: %v", err)
	}
	refAcct, _ := ref.AccountKey(context.Background())
	refDev, _ := ref.DeviceKey(context.Background())

	// Restore from the same mnemonic into a new wallet file.
	path := filepath.Join(dir, "restored.key")
	p, err := NewFileProvider(FileProviderConfig{Path: path, Mnemonic: ref.Mnemonic()})
	if err != nil {
		t.Fatalf("seed from mnemonic: %v", err)
	}
	if !p.Created() {
		t.Fatal("seeded open should have Created() = true")
	}
	if p.Mnemonic() != ref.Mnemonic() {
		t.Fatal("seeded wallet stored a different mnemonic")
	}
	acct, _ := p.AccountKey(context.Background())
	dev, _ := p.DeviceKey(context.Background())
	if string(acct) != string(refAcct) {
		t.Fatal("same mnemonic must derive the same account key")
	}
	if string(dev) == string(refDev) {
		t.Fatal("seeded wallet must generate a fresh device key")
	}

	// Reopen with the same mnemonic: fine, not created.
	p2, err := NewFileProvider(FileProviderConfig{Path: path, Mnemonic: ref.Mnemonic()})
	if err != nil {
		t.Fatalf("reopen with matching mnemonic: %v", err)
	}
	if p2.Created() {
		t.Fatal("reopen should have Created() = false")
	}

	// Reopen with a different mnemonic: mismatch.
	other, err := GenerateMnemonic()
	if err != nil {
		t.Fatalf("generate other mnemonic: %v", err)
	}
	if _, err := NewFileProvider(FileProviderConfig{Path: path, Mnemonic: other}); !errors.Is(err, ErrMnemonicMismatch) {
		t.Fatalf("expected ErrMnemonicMismatch, got %v", err)
	}

	// Invalid phrase rejected before touching disk.
	badPath := filepath.Join(dir, "bad.key")
	if _, err := NewFileProvider(FileProviderConfig{Path: badPath, Mnemonic: "not a valid phrase"}); !errors.Is(err, ErrInvalidMnemonic) {
		t.Fatalf("expected ErrInvalidMnemonic, got %v", err)
	}
	if _, err := os.Stat(badPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid mnemonic must not create a wallet file")
	}
}

func TestAccountId(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFileProvider(FileProviderConfig{Path: filepath.Join(dir, "wallet.key")})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	id, err := AccountId(p.Mnemonic(), 0)
	if err != nil {
		t.Fatalf("AccountId: %v", err)
	}
	id2, err := AccountId(p.Mnemonic(), 0)
	if err != nil {
		t.Fatalf("AccountId again: %v", err)
	}
	if id == "" || id != id2 {
		t.Fatalf("AccountId not deterministic: %q vs %q", id, id2)
	}

	// Agrees with the provider-derived identity.
	raw, err := p.AccountKey(context.Background())
	if err != nil {
		t.Fatalf("account key: %v", err)
	}
	priv, err := crypto.UnmarshalEd25519PrivateKey(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if want := priv.GetPublic().Account(); id != want {
		t.Fatalf("AccountId = %q, provider-derived = %q", id, want)
	}

	// Different index, different identity.
	id1, err := AccountId(p.Mnemonic(), 1)
	if err != nil {
		t.Fatalf("AccountId index 1: %v", err)
	}
	if id1 == id {
		t.Fatal("index 1 should derive a different identity")
	}

	if _, err := AccountId("not a valid phrase", 0); !errors.Is(err, ErrInvalidMnemonic) {
		t.Fatalf("expected ErrInvalidMnemonic, got %v", err)
	}
}

// A legacy wallet file — index-0, written before the `index` key
// existed (omitempty drops it) — must decode to index 0 and stay on
// the index-0 account. Also pins the restore-with-wrong-index error.
func TestFileProvider_LegacyWalletWithoutIndexKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallet.key")
	ref, err := NewFileProvider(FileProviderConfig{Path: filepath.Join(dir, "ref.key")})
	if err != nil {
		t.Fatalf("generate reference: %v", err)
	}
	// Hand-write the legacy shape: payload without an `index` key.
	legacy := fmt.Sprintf(`{"version":1,"payload":{"mnemonic":%q,"deviceKey":%q}}`,
		ref.Mnemonic(), base64.StdEncoding.EncodeToString(ref.w.DeviceKey))
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	p, err := NewFileProvider(FileProviderConfig{Path: path})
	if err != nil {
		t.Fatalf("load legacy wallet: %v", err)
	}
	if got := p.AccountIndex(); got != 0 {
		t.Fatalf("legacy wallet AccountIndex = %d, want 0", got)
	}

	// Restoring over it with the same phrase at the new default index
	// must refuse rather than silently serve the index-0 account.
	if _, err := NewFileProvider(FileProviderConfig{Path: path, Mnemonic: ref.Mnemonic(), Index: DefaultAccountIndex}); !errors.Is(err, ErrMnemonicMismatch) {
		t.Fatalf("expected ErrMnemonicMismatch on index mismatch, got %v", err)
	}
	// Matching index (explicit 0) loads fine.
	if _, err := NewFileProvider(FileProviderConfig{Path: path, Mnemonic: ref.Mnemonic(), Index: 0}); err != nil {
		t.Fatalf("matching index restore: %v", err)
	}
}

// Fresh generation honors cfg.Index: the wallet pins it, reload derives
// the same non-zero-index account, and AccountIndex reports it.
func TestFileProvider_FreshGenerationHonorsIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallet.key")
	p, err := NewFileProvider(FileProviderConfig{Path: path, Index: DefaultAccountIndex})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if got := p.AccountIndex(); got != DefaultAccountIndex {
		t.Fatalf("AccountIndex = %d, want %d", got, DefaultAccountIndex)
	}

	wantId, err := AccountId(p.Mnemonic(), DefaultAccountIndex)
	if err != nil {
		t.Fatalf("AccountId: %v", err)
	}
	id0, err := AccountId(p.Mnemonic(), 0)
	if err != nil {
		t.Fatalf("AccountId index 0: %v", err)
	}
	if wantId == id0 {
		t.Fatal("index 1 must derive a different identity than index 0")
	}

	providerId := func(p *FileProvider) string {
		raw, err := p.AccountKey(context.Background())
		if err != nil {
			t.Fatalf("account key: %v", err)
		}
		priv, err := crypto.UnmarshalEd25519PrivateKey(raw)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return priv.GetPublic().Account()
	}
	if got := providerId(p); got != wantId {
		t.Fatalf("fresh wallet derives %q, want index-%d account %q", got, DefaultAccountIndex, wantId)
	}

	// Reload: the stored index wins; cfg.Index is ignored on existing files.
	p2, err := NewFileProvider(FileProviderConfig{Path: path})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := p2.AccountIndex(); got != DefaultAccountIndex {
		t.Fatalf("reloaded AccountIndex = %d, want %d", got, DefaultAccountIndex)
	}
	if got := providerId(p2); got != wantId {
		t.Fatalf("reloaded wallet derives %q, want %q", got, wantId)
	}
}

func TestFileProvider_RejectsPasskeyOnPlainFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wallet.key")

	if _, err := NewFileProvider(FileProviderConfig{Path: path}); err != nil {
		t.Fatalf("generate plain: %v", err)
	}
	if _, err := NewFileProvider(FileProviderConfig{Path: path, Passkey: "x"}); err == nil {
		t.Fatal("expected error when providing passkey for plain wallet")
	}
}
