package payloads

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/util/crypto"
)

// encKeyDerivationPath is the SLIP-0021 path deriving the payloads
// enc-sealing key from the space read key. Domain-separated from the
// raw ACL read key on purpose: any-sync derives its own per-tree keys
// from the read key, and the SDK must not reuse the raw key across
// encryption domains.
const encKeyDerivationPath = "m/SLIP-0021/anysync-sdk/payloads/enc"

// ErrNoKey is the sentinel a KeyProvider returns when it cannot
// resolve a sealing key (keyless reader, unknown key-record id).
// Typed readers degrade to cleartext-only rows on it — it is the
// expected condition for a filenode-v2 broker, not a failure.
var ErrNoKey = errors.New("payloads: no key")

// KeyProvider resolves the enc-sealing keys (already domain-derived,
// see DeriveEncKey) by ACL key-record id. CurrentKey serves the seal
// path; KeyById serves unseal — including rows sealed under an older
// read key after an ACL rotation (any-sync retains historical read
// keys in AclState().Keys()). Both return ErrNoKey when the caller
// has no read access.
type KeyProvider interface {
	CurrentKey(ctx context.Context) (kid string, key crypto.SymKey, err error)
	KeyById(ctx context.Context, kid string) (crypto.SymKey, error)
}

// DeriveEncKey derives the payloads enc-sealing key from a space read
// key. Deterministic: every member derives the same sealing key from
// the same read key, so `kid` can stay the ACL key-record id of the
// read key itself. Implementations of KeyProvider should cache the
// result per kid.
func DeriveEncKey(readKey crypto.SymKey) (crypto.SymKey, error) {
	raw, err := readKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("payloads: read key raw: %w", err)
	}
	return crypto.DeriveSymmetricKey(raw, encKeyDerivationPath)
}

// EncPayload is the member-only plaintext grouped behind the sealed
// `enc` field. One sealed value on purpose: a single decrypt per row
// and better wire compression than per-field sealing. The wrapped
// per-file key stays inside (history is never re-sealed on ACL
// rotation, so a separate field would buy nothing).
//
// Wire shape (anyenc object inside the ciphertext):
// {key, name, sha256, mime, inline?} — unknown keys are ignored on
// read, so richer metadata (e.g. image dimensions) can be added
// without breaking older readers.
type EncPayload struct {
	// Key is the file's wrapped symmetric key (raw bytes; the byte
	// layer owns its format). Empty for inline rows if the caller
	// chose to seal the bytes directly.
	Key []byte
	// Name is the user-facing file name.
	Name string
	// SHA256 is the plaintext content hash — the whole-file dedup key.
	SHA256 []byte
	// Mime is the content type hint.
	Mime string
	// Inline holds the file bytes for the inline tier (< InlineMaxSize),
	// which has no rootCid / S3 / networkSign and rides the CRDT.
	Inline []byte
}

// Sealed enc-plaintext wire keys.
const (
	encKey    = "key"
	encName   = "name"
	encSHA256 = "sha256"
	encMime   = "mime"
	encInline = "inline"
)

// SealEnc serialises p and seals it with key (AES-256-GCM, nonce
// prepended by crypto.AESKey.Encrypt). The result is the `ct` value
// of the row's `enc` field.
func SealEnc(key crypto.SymKey, p EncPayload) ([]byte, error) {
	if key == nil {
		return nil, ErrNoKey
	}
	a := &anyenc.Arena{}
	obj := a.NewObject()
	if len(p.Key) > 0 {
		obj.Set(encKey, a.NewBinary(p.Key))
	}
	if p.Name != "" {
		obj.Set(encName, a.NewString(p.Name))
	}
	if len(p.SHA256) > 0 {
		obj.Set(encSHA256, a.NewBinary(p.SHA256))
	}
	if p.Mime != "" {
		obj.Set(encMime, a.NewString(p.Mime))
	}
	if len(p.Inline) > 0 {
		obj.Set(encInline, a.NewBinary(p.Inline))
	}
	return key.Encrypt(obj.MarshalTo(nil))
}

// UnsealEnc opens a sealed `ct` value and parses the EncPayload.
func UnsealEnc(key crypto.SymKey, ct []byte) (EncPayload, error) {
	if key == nil {
		return EncPayload{}, ErrNoKey
	}
	plain, err := key.Decrypt(ct)
	if err != nil {
		return EncPayload{}, fmt.Errorf("payloads: unseal: %w", err)
	}
	v, err := anyenc.Parse(plain)
	if err != nil {
		return EncPayload{}, fmt.Errorf("payloads: unseal parse: %w", err)
	}
	return EncPayload{
		Key:    append([]byte(nil), v.GetBytes(encKey)...),
		Name:   v.GetString(encName),
		SHA256: append([]byte(nil), v.GetBytes(encSHA256)...),
		Mime:   v.GetString(encMime),
		Inline: append([]byte(nil), v.GetBytes(encInline)...),
	}, nil
}
