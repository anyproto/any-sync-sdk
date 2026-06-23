package space

import "github.com/anyproto/any-sync/util/crypto"

// accountMetadataSymKeyPath is the SLIP-0021 derivation path for an
// account's metadata symmetric key — the key that encrypts the account's
// identityRepo profile. Deterministic from the account private key, so
// the same account always yields the same key and no key storage or
// rotation is needed; any contact who has received the key can decrypt
// current and future profile updates.
//
// The SDK's profile record uses its own format/kind (see
// IdentityProfileKind), so this path is intentionally distinct from other
// clients' metadata-key paths: cross-client decrypt would require
// explicit format negotiation and is out of scope. A distinct path makes
// the incompatibility explicit rather than producing a
// same-key-different-format collision.
const accountMetadataSymKeyPath = "m/SLIP-0021/anysync-sdk/account/metadata"

// DeriveAccountMetadataSymKey deterministically derives an account's
// metadata symmetric key from its private account (sign) key. The key is
// shared with contacts through already-encrypted channels (a shared
// space's ACL metadata, a 1-1 invite) so they can decrypt this account's
// identityRepo profile; see docs/13 and EncryptProfile.
func DeriveAccountMetadataSymKey(accountKey crypto.PrivKey) (crypto.SymKey, error) {
	raw, err := accountKey.Raw()
	if err != nil {
		return nil, err
	}
	return crypto.DeriveSymmetricKey(raw, accountMetadataSymKeyPath)
}

// MarshalSymKey encodes a symmetric key to its canonical string form for
// storage in the account-scoped identity→key cache. EncodeSymKey/
// DecodeSymKey round-trip through this representation.
func MarshalSymKey(k crypto.SymKey) (string, error) {
	return crypto.EncodeKeyToString(k)
}

// UnmarshalSymKey decodes a symmetric key produced by MarshalSymKey.
func UnmarshalSymKey(s string) (crypto.SymKey, error) {
	return crypto.DecodeKeyFromString(s, crypto.UnmarshallAESKey, nil)
}

// IdentityProfileKind is the identityRepo `Kind` string the SDK uses for
// the account profile record. The SDK's record is its own format — a
// NUL-separated layout (name\x00description\x00iconCID) encrypted with the
// account metadata symkey and signed by the account key — namespaced
// apart from other clients' profile kinds, whose encrypted-protobuf format
// it can't decode without explicit negotiation.
//
// The kind string MUST NOT contain a dot — the coordinator stores
// records as MongoDB sub-documents keyed by Kind (`data.<kind>`), so
// a dot is interpreted as a nested-path separator and silently
// re-shapes the record on disk.
const IdentityProfileKind = "anysync-sdk-profile"

// EncodeAccountMetadata serialises an AccountMetadata into the
// canonical bytes the SDK pushes to identityRepo. Same NUL-separated
// layout as the join-time metadata blob (see internal/spaceimpl
// encodeMetadata) so the same decoder works on both sides — keeps
// the parsing surface small.
//
// Returns nil for an empty input; caller is expected to refuse-to-push
// on nil to avoid clobbering an existing profile with empty values.
func EncodeAccountMetadata(m AccountMetadata) []byte {
	if m.Name == "" && m.Description == "" && m.IconCID == "" {
		return nil
	}
	out := make([]byte, 0, len(m.Name)+len(m.Description)+len(m.IconCID)+2)
	out = append(out, m.Name...)
	out = append(out, 0)
	out = append(out, m.Description...)
	out = append(out, 0)
	out = append(out, m.IconCID...)
	return out
}

// DecodeAccountMetadata is the inverse — used by the per-space
// fetcher to interpret the bytes pulled from identityRepo. Returns
// the zero value on a malformed input rather than an error: receivers
// fall through to the join-time metadata snapshot in that case.
func DecodeAccountMetadata(b []byte) AccountMetadata {
	if len(b) == 0 {
		return AccountMetadata{}
	}
	name, rest, ok := splitNul(b)
	if !ok {
		return AccountMetadata{}
	}
	desc, icon, ok := splitNul(rest)
	if !ok {
		return AccountMetadata{Name: name}
	}
	return AccountMetadata{
		Name:        name,
		Description: desc,
		IconCID:     string(icon),
	}
}

// EncryptProfile serialises an AccountMetadata and encrypts it with the
// account's metadata symkey for upload to identityRepo. Returns nil for
// empty input (caller should refuse-to-push on nil rather than clobber an
// existing profile). The plaintext layout is the same NUL-separated blob
// as the (now-deprecated plaintext) identityRepo format — only the
// transport is encrypted, so DecodeAccountMetadata still parses the
// decrypted bytes.
func EncryptProfile(meta AccountMetadata, key crypto.SymKey) ([]byte, error) {
	payload := EncodeAccountMetadata(meta)
	if len(payload) == 0 {
		return nil, nil
	}
	return key.Encrypt(payload)
}

// DecryptProfile decrypts an identityRepo profile blob with key and
// decodes it. ok is false when key is nil or decryption fails — callers
// must NOT fall back to parsing the raw bytes as plaintext (ciphertext
// parsed as a NUL blob yields a garbage name). A reader without the
// contact's symkey simply can't resolve the profile yet; it surfaces from
// identity alone until the key arrives (see docs/13, the identityMetaKeys
// cache).
func DecryptProfile(data []byte, key crypto.SymKey) (AccountMetadata, bool) {
	if key == nil || len(data) == 0 {
		return AccountMetadata{}, false
	}
	plain, err := key.Decrypt(data)
	if err != nil {
		return AccountMetadata{}, false
	}
	return DecodeAccountMetadata(plain), true
}

// splitNul splits on the first 0x00 byte. Returns (head, tail, true)
// on success; (whole, nil, false) if no separator.
func splitNul(b []byte) (string, []byte, bool) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return string(b), nil, false
}
