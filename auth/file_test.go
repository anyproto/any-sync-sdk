package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
