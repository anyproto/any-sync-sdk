package payloads

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeKeyProvider serves a fixed kid → key map. Unknown kids return
// ErrNoKey — the keyless-reader / post-rotation-missing-key view.
type fakeKeyProvider struct {
	current string
	keys    map[string]crypto.SymKey
}

func (f *fakeKeyProvider) CurrentKey(_ context.Context) (string, crypto.SymKey, error) {
	if key, ok := f.keys[f.current]; ok {
		return f.current, key, nil
	}
	return "", nil, ErrNoKey
}

func (f *fakeKeyProvider) KeyById(_ context.Context, kid string) (crypto.SymKey, error) {
	if key, ok := f.keys[kid]; ok {
		return key, nil
	}
	return nil, ErrNoKey
}

func newFakeProvider(t *testing.T, kids ...string) *fakeKeyProvider {
	t.Helper()
	f := &fakeKeyProvider{keys: make(map[string]crypto.SymKey, len(kids))}
	for _, kid := range kids {
		readKey, err := crypto.NewRandomAES()
		require.NoError(t, err)
		encKey, err := DeriveEncKey(readKey)
		require.NoError(t, err)
		f.keys[kid] = encKey
	}
	if len(kids) > 0 {
		f.current = kids[len(kids)-1]
	}
	return f
}

func testEncPayload() EncPayload {
	return EncPayload{
		Key:    []byte("wrapped-file-key-bytes-0123456789ab"),
		Name:   "image.png",
		SHA256: []byte("0123456789abcdef0123456789abcdef"),
		Mime:   "image/png",
	}
}

func TestSealUnsealRoundTrip(t *testing.T) {
	kp := newFakeProvider(t, "kid-1")
	kid, key, err := kp.CurrentKey(context.Background())
	require.NoError(t, err)
	require.Equal(t, "kid-1", kid)

	want := testEncPayload()
	ct, err := SealEnc(key, want)
	require.NoError(t, err)
	require.NotEmpty(t, ct)

	got, err := UnsealEnc(key, ct)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSealUnseal_Inline(t *testing.T) {
	kp := newFakeProvider(t, "kid-1")
	_, key, err := kp.CurrentKey(context.Background())
	require.NoError(t, err)

	want := EncPayload{Name: "tiny.txt", Inline: []byte("hello inline world")}
	ct, err := SealEnc(key, want)
	require.NoError(t, err)
	got, err := UnsealEnc(key, ct)
	require.NoError(t, err)
	assert.Equal(t, want.Inline, got.Inline)
	assert.Equal(t, want.Name, got.Name)
}

// TestSealUnseal_VariantTags pins the variant fields' round-trip (the
// sibling-row relationship lives only in the sealed meta).
func TestSealUnseal_VariantTags(t *testing.T) {
	kp := newFakeProvider(t, "kid-1")
	key, err := kp.KeyById(context.Background(), "kid-1")
	require.NoError(t, err)

	want := testEncPayload()
	want.Variant = "thumbnail"
	want.VariantOf = "origFileId"
	ct, err := SealEnc(key, want)
	require.NoError(t, err)
	got, err := UnsealEnc(key, ct)
	require.NoError(t, err)
	assert.Equal(t, "thumbnail", got.Variant)
	assert.Equal(t, "origFileId", got.VariantOf)

	// Absent on originals.
	ct, err = SealEnc(key, testEncPayload())
	require.NoError(t, err)
	got, err = UnsealEnc(key, ct)
	require.NoError(t, err)
	assert.Empty(t, got.Variant)
	assert.Empty(t, got.VariantOf)
}

// TestUnseal_WrongKeyFails pins that GCM authentication rejects a
// ciphertext under the wrong key — a corrupt row surfaces as an
// error, never as garbage plaintext.
func TestUnseal_WrongKeyFails(t *testing.T) {
	kp := newFakeProvider(t, "kid-1", "kid-2")
	key1, err := kp.KeyById(context.Background(), "kid-1")
	require.NoError(t, err)
	key2, err := kp.KeyById(context.Background(), "kid-2")
	require.NoError(t, err)

	ct, err := SealEnc(key1, testEncPayload())
	require.NoError(t, err)
	_, err = UnsealEnc(key2, ct)
	assert.Error(t, err)
}

// TestDeriveEncKey_DomainSeparated pins that the sealing key differs
// from the read key it derives from (never reuse the raw ACL key
// across encryption domains) and is deterministic.
func TestDeriveEncKey_DomainSeparated(t *testing.T) {
	readKey, err := crypto.NewRandomAES()
	require.NoError(t, err)
	a, err := DeriveEncKey(readKey)
	require.NoError(t, err)
	b, err := DeriveEncKey(readKey)
	require.NoError(t, err)

	rawRead, err := readKey.Raw()
	require.NoError(t, err)
	rawA, err := a.Raw()
	require.NoError(t, err)
	rawB, err := b.Raw()
	require.NoError(t, err)
	assert.NotEqual(t, rawRead, rawA, "enc key must not equal the read key")
	assert.Equal(t, rawA, rawB, "derivation must be deterministic")
}

// TestRowUnseal_Rotation covers the ACL-rotation read path: a row
// sealed under an old kid unseals via KeyById; a row under an unknown
// kid stays Sealed with no error (the keyless view).
func TestRowUnseal_Rotation(t *testing.T) {
	ctx := context.Background()
	kp := newFakeProvider(t, "kid-old", "kid-new")

	oldKey, err := kp.KeyById(ctx, "kid-old")
	require.NoError(t, err)
	ct, err := SealEnc(oldKey, testEncPayload())
	require.NoError(t, err)

	row := Row{Id: "f1", RootCid: "bafy1", EncKid: "kid-old", encCt: ct, Sealed: true}
	require.NoError(t, row.Unseal(ctx, kp))
	assert.False(t, row.Sealed)
	assert.Equal(t, "image.png", row.Enc.Name)

	// Unknown kid (e.g. this reader never had access) → stays sealed,
	// no error.
	sealed := Row{Id: "f2", RootCid: "bafy2", EncKid: "kid-gone", encCt: ct, Sealed: true}
	require.NoError(t, sealed.Unseal(ctx, kp))
	assert.True(t, sealed.Sealed)
}
