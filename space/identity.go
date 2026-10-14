package space

import (
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"github.com/anyproto/any-sync/util/crypto"
)

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
// identityRepo profile; see docs/one-to-one-spaces.md and EncryptProfile.
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
// NUL-separated layout (see EncodeProfile) encrypted with the account
// metadata symkey and signed by the account key — namespaced apart from
// other clients' profile kinds, whose encrypted-protobuf format it can't
// decode without explicit negotiation.
//
// The kind string MUST NOT contain a dot — the coordinator stores
// records as MongoDB sub-documents keyed by Kind (`data.<kind>`), so
// a dot is interpreted as a nested-path separator and silently
// re-shapes the record on disk.
const IdentityProfileKind = "anysync-sdk-profile"

// Profile is the account's identityRepo record: the display metadata
// plus the avatar reference. IconCID is the avatar's content address
// (the rootCid of a durable files-v2 object); IconSpaceId names the
// space holding it (the owner's tech space) and IconKey unlocks it.
// Readers fetch the ciphertext by (IconSpaceId, IconCID) through the
// public read path and decrypt it with IconKey — they never join or
// sync the owner's space. The whole record is sealed under the metadata
// symkey, so the key reaches exactly the contacts who can read the name.
type Profile struct {
	AccountMetadata
	// IconSpaceId is the space holding the avatar file.
	IconSpaceId string
	// IconKey is the avatar's AES-256 file key, standard base64.
	IconKey string
}

// HasIcon reports whether the profile references a fetchable avatar.
func (p Profile) HasIcon() bool {
	return p.IconCID != "" && p.IconSpaceId != "" && p.IconKey != ""
}

// IsEmpty reports whether the profile has nothing worth pushing.
func (p Profile) IsEmpty() bool {
	return p.Name == "" && p.Description == "" && p.IconCID == ""
}

// IconKeyBytes decodes IconKey; ok is false when it is absent or not a
// 32-byte key.
func (p Profile) IconKeyBytes() (key []byte, ok bool) {
	if p.IconKey == "" {
		return nil, false
	}
	key, err := base64.StdEncoding.DecodeString(p.IconKey)
	if err != nil || len(key) != iconKeySize {
		return nil, false
	}
	return key, true
}

// EncodeIconKey is the inverse of Profile.IconKeyBytes.
func EncodeIconKey(key []byte) string { return base64.StdEncoding.EncodeToString(key) }

// iconKeySize is the files byte-layer AES-256 key length.
const iconKeySize = 32

// MaxIconSize caps the plaintext an avatar may carry. Avatars are
// fetched by every contact that renders the account, so the cap keeps
// one profile from costing every reader a large download.
const MaxIconSize = 1 << 20

var (
	// ErrIconTooLarge — SetIcon read more than MaxIconSize bytes, or a
	// contact's profile names an object over the cap, which
	// Identities.OpenIcon refuses unread.
	ErrIconTooLarge = errors.New("space: profile icon exceeds the size cap")
	// ErrProfileIconReadOnly — UpdateMetadata was handed an IconCID.
	// The icon is a file: SetIcon stores the bytes and derives the cid.
	ErrProfileIconReadOnly = errors.New("space: the profile icon is set through SetIcon")
	// ErrProfileFieldInvalid — a name or description carries a NUL
	// byte, the profile record's field separator.
	ErrProfileFieldInvalid = errors.New("space: profile fields must not contain NUL")
)

// IconOpts is the caller metadata of an avatar upload; both fields are
// member-only (they ride the sealed part of the tech-space file row).
type IconOpts struct {
	Name string
	Mime string
}

// profileFieldSep separates the fields of the encoded profile.
const profileFieldSep = "\x00"

// EncodeProfile serialises a Profile into the canonical bytes the SDK
// pushes to identityRepo: NUL-separated name, description, iconCID, and
// — only for a profile with an avatar — iconSpaceId and iconKey. A
// profile without an avatar therefore encodes exactly as it did before
// the icon fields existed. An empty profile encodes to its separators:
// a push that clears every field must still reach the repo.
func EncodeProfile(p Profile) []byte {
	fields := []string{p.Name, p.Description, p.IconCID}
	if p.IconSpaceId != "" || p.IconKey != "" {
		fields = append(fields, p.IconSpaceId, p.IconKey)
	}
	return []byte(strings.Join(fields, profileFieldSep))
}

// DecodeProfile is the inverse — used by the fetchers to interpret the
// bytes pulled from identityRepo. Fields past the third are optional
// (a record written before avatars existed carries three). A malformed
// input decodes to the zero value rather than an error: receivers fall
// through to the join-time metadata snapshot in that case.
func DecodeProfile(b []byte) Profile {
	if len(b) == 0 {
		return Profile{}
	}
	fields := strings.Split(string(b), profileFieldSep)
	switch len(fields) {
	case 1:
		return Profile{}
	case 2:
		return Profile{AccountMetadata: AccountMetadata{Name: fields[0]}}
	}
	p := Profile{AccountMetadata: AccountMetadata{
		Name:        fields[0],
		Description: fields[1],
		IconCID:     fields[2],
	}}
	if len(fields) >= 5 {
		p.IconSpaceId, p.IconKey = fields[3], fields[4]
	}
	return p
}

// EncryptProfile serialises a Profile and encrypts it with the account's
// metadata symkey for upload to identityRepo. Only the transport is
// encrypted, so DecodeProfile parses the decrypted bytes.
func EncryptProfile(p Profile, key crypto.SymKey) ([]byte, error) {
	return key.Encrypt(EncodeProfile(p))
}

// minProfileCiphertext is the smallest well-formed AES-GCM profile blob:
// 12-byte nonce + 16-byte auth tag. Anything shorter is not ciphertext —
// notably legacy plaintext records pushed before profile encryption,
// which live under the same identityRepo kind and can be arbitrarily
// short (a 4-char name encodes to 6 bytes).
const minProfileCiphertext = crypto.NonceBytes + 16

// DecryptProfile decrypts an identityRepo profile blob with key and
// decodes it. ok is false when key is nil, data is too short to be
// ciphertext, or decryption fails — callers must NOT fall back to
// parsing the raw bytes as plaintext (ciphertext parsed as a NUL blob
// yields a garbage name; a legacy plaintext record stays unresolved
// until a re-push overwrites it with ciphertext). A reader without the
// contact's symkey simply can't resolve the profile yet; it surfaces from
// identity alone until the key arrives (see docs/one-to-one-spaces.md, the identityMetaKeys
// cache).
func DecryptProfile(data []byte, key crypto.SymKey) (Profile, bool) {
	if key == nil || len(data) < minProfileCiphertext {
		return Profile{}, false
	}
	plain, err := key.Decrypt(data)
	if err != nil {
		return Profile{}, false
	}
	return DecodeProfile(plain), true
}

// LimitIcon bounds an avatar upload: the returned reader yields at most
// MaxIconSize bytes and fails with ErrIconTooLarge when r has more.
func LimitIcon(r io.Reader) io.Reader {
	return &iconLimiter{r: io.LimitReader(r, MaxIconSize+1)}
}

type iconLimiter struct {
	r io.Reader
	n int64
}

func (l *iconLimiter) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	l.n += int64(n)
	if l.n > MaxIconSize {
		over := int(l.n - MaxIconSize)
		if over > n {
			over = n
		}
		return n - over, ErrIconTooLarge
	}
	return n, err
}
