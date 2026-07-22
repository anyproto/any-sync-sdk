package space

import (
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
)

// A legacy plaintext identityRepo profile (pushed before profile
// encryption, same kind) must resolve to (zero, false), never panic —
// "kaye\x00\x00" is the exact 6-byte blob from the SYN-94 field crash.
func TestDecryptProfileLegacyPlaintext(t *testing.T) {
	priv, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	key, err := DeriveAccountMetadataSymKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	short := EncodeAccountMetadata(AccountMetadata{Name: "kaye"})
	if len(short) != 6 {
		t.Fatalf("expected 6-byte legacy blob, got %d", len(short))
	}
	long := EncodeAccountMetadata(AccountMetadata{Name: "a-much-longer-legacy-name"})

	for _, data := range [][]byte{nil, short, long, make([]byte, minProfileCiphertext-1)} {
		if _, ok := DecryptProfile(data, key); ok {
			t.Fatalf("%d-byte non-ciphertext must not decrypt", len(data))
		}
	}

	// sanity: a real encrypted profile still round-trips
	meta := AccountMetadata{Name: "kaye"}
	enc, err := EncryptProfile(meta, key)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := DecryptProfile(enc, key)
	if !ok || got != meta {
		t.Fatalf("round-trip failed: ok=%v got=%+v", ok, got)
	}
}
