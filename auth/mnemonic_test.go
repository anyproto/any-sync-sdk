package auth

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
)

// DeriveAccount yields the identity key any-sync's full DeriveKeys
// derives, and the id AccountId reports.
func TestDeriveAccount_MatchesDeriveKeys(t *testing.T) {
	m, err := GenerateMnemonic()
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []uint32{0, 1, 7} {
		id, got, err := DeriveAccount(m, index)
		if err != nil {
			t.Fatalf("index %d: %v", index, err)
		}
		wantId, err := AccountId(m, index)
		if err != nil {
			t.Fatal(err)
		}
		if id != wantId {
			t.Fatalf("index %d: id %q, AccountId %q", index, id, wantId)
		}
		res, err := crypto.Mnemonic(m).DeriveKeys(index)
		if err != nil {
			t.Fatal(err)
		}
		want, err := res.Identity.Raw()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("index %d: identity key differs from DeriveKeys", index)
		}
	}
	if _, _, err := DeriveAccount("not a valid phrase", 0); !errors.Is(err, ErrInvalidMnemonic) {
		t.Fatalf("expected ErrInvalidMnemonic, got %v", err)
	}
}

// The provider derives once; a caller scribbling over the returned key
// does not change what the next call returns.
func TestMnemonicProvider_AccountKeyIsACopy(t *testing.T) {
	m, err := GenerateMnemonic()
	if err != nil {
		t.Fatal(err)
	}
	dev, err := GenerateDeviceKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewMnemonicProvider(MnemonicConfig{Mnemonic: m, Index: DefaultAccountIndex, DeviceKey: dev})
	if err != nil {
		t.Fatal(err)
	}
	_, want, err := DeriveAccount(m, DefaultAccountIndex)
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.AccountKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clear(first)
	second, err := p.AccountKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, want) {
		t.Fatal("AccountKey changed after the caller cleared a returned key")
	}
}

// A key provider serves the keys it was built with and refuses a
// missing one.
func TestNewKeyProvider(t *testing.T) {
	m, err := GenerateMnemonic()
	if err != nil {
		t.Fatal(err)
	}
	_, acc, err := DeriveAccount(m, DefaultAccountIndex)
	if err != nil {
		t.Fatal(err)
	}
	dev, err := GenerateDeviceKey(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewKeyProvider(KeyConfig{AccountKey: acc, DeviceKey: dev})
	if err != nil {
		t.Fatal(err)
	}
	gotAcc, _ := p.AccountKey(context.Background())
	gotDev, _ := p.DeviceKey(context.Background())
	if !bytes.Equal(gotAcc, acc) || !bytes.Equal(gotDev, dev) {
		t.Fatal("key provider returned other keys than it was built with")
	}
	if _, err := NewKeyProvider(KeyConfig{DeviceKey: dev}); err == nil {
		t.Fatal("no account key: want an error")
	}
	if _, err := NewKeyProvider(KeyConfig{AccountKey: acc}); err == nil {
		t.Fatal("no device key: want an error")
	}
}
