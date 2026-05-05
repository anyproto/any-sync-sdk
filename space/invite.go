package space

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/mr-tron/base58"

	"github.com/anyproto/any-sync/util/crypto"
)

// Invite is the share-friendly handle a space owner produces and gives
// to someone they want to let in. Opaque to callers; encoded as a
// single base58 token so it survives copy/paste through chats and
// links without escaping.
//
// Internally an invite carries (spaceId, invitePrivKey). The invite
// private key is the one any-sync's BuildInvite minted on the owner
// side; the joiner uses it as proof against the matching public-key
// invite record on the ACL.
//
// SDK-defined format — independent of anytype-heart's encrypted-blob
// invite. There is no compatibility shim.
type Invite struct {
	SpaceId   string
	InviteKey crypto.PrivKey
}

// inviteFormatVersion is the magic byte stamped at the start of every
// encoded invite. Bump if the encoding ever changes; DecodeInvite
// rejects unknown versions outright.
const inviteFormatVersion byte = 1

var (
	// ErrInvalidInvite is returned by DecodeInvite when the input is
	// not a base58-encoded invite produced by EncodeInvite.
	ErrInvalidInvite = errors.New("anysyncsdk: invalid invite")
)

// EncodeInvite packs i into the share-friendly base58 string. The
// inverse is DecodeInvite.
//
// Layout (pre-base58):
//
//	[1] version=1
//	[varint] len(spaceId)
//	[..]    spaceId bytes
//	[..]    proto-marshalled invite private key (rest of buffer)
func EncodeInvite(i Invite) (string, error) {
	if i.SpaceId == "" {
		return "", errors.New("anysyncsdk: EncodeInvite: SpaceId required")
	}
	if i.InviteKey == nil {
		return "", errors.New("anysyncsdk: EncodeInvite: InviteKey required")
	}
	keyBytes, err := i.InviteKey.Marshall()
	if err != nil {
		return "", fmt.Errorf("anysyncsdk: marshal invite key: %w", err)
	}
	spaceId := []byte(i.SpaceId)

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(spaceId)+len(keyBytes))
	buf = append(buf, inviteFormatVersion)
	buf = binary.AppendUvarint(buf, uint64(len(spaceId)))
	buf = append(buf, spaceId...)
	buf = append(buf, keyBytes...)
	return base58.Encode(buf), nil
}

// DecodeInvite parses a base58 token produced by EncodeInvite back into
// its components. Returns ErrInvalidInvite (wrapped with detail) for
// any malformed input — callers can use errors.Is.
func DecodeInvite(s string) (Invite, error) {
	if s == "" {
		return Invite{}, fmt.Errorf("%w: empty", ErrInvalidInvite)
	}
	raw, err := base58.Decode(s)
	if err != nil {
		return Invite{}, fmt.Errorf("%w: base58: %w", ErrInvalidInvite, err)
	}
	if len(raw) < 1 {
		return Invite{}, fmt.Errorf("%w: too short", ErrInvalidInvite)
	}
	if raw[0] != inviteFormatVersion {
		return Invite{}, fmt.Errorf("%w: unknown version %d", ErrInvalidInvite, raw[0])
	}
	rest := raw[1:]
	spaceIdLen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < spaceIdLen {
		return Invite{}, fmt.Errorf("%w: truncated spaceId length", ErrInvalidInvite)
	}
	rest = rest[n:]
	spaceId := string(rest[:spaceIdLen])
	keyBytes := rest[spaceIdLen:]
	if len(keyBytes) == 0 {
		return Invite{}, fmt.Errorf("%w: missing key", ErrInvalidInvite)
	}
	priv, err := crypto.UnmarshalEd25519PrivateKeyProto(keyBytes)
	if err != nil {
		return Invite{}, fmt.Errorf("%w: invite key: %w", ErrInvalidInvite, err)
	}
	return Invite{SpaceId: spaceId, InviteKey: priv}, nil
}
