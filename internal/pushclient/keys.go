// Package pushclient is the client of the anytype-push-server (SYN-47):
// per-space key derivation, the signed/encrypted request builders, the
// thin DRPC transport, and the account-level service behind
// SDK.Push() / space.PushAPI.
//
// The push node is NOT part of the nodeconf — it is a direct
// out-of-band peer (config.Push) dialed through the regular
// secure-channel pool, so every RPC carries this account's identity in
// the handshake (the server authorizes by peer.CtxPubKey).
//
// Trust model (mirrors anytype-heart's core/pushnotification): the
// server never sees space content or space keys. Each space gets a
// dedicated ed25519 "push space key" derived from the ACL's first
// metadata key — topics are signed with it so only members can
// subscribe/publish, and the server verifies against the raw public
// key it was handed at CreateSpace time. Notification payloads are
// encrypted with a symmetric key derived from the space's CURRENT read
// key, so the server relays opaque ciphertext; receivers pick the
// decryption key by its sha256 id.
package pushclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/anyproto/any-sync/util/crypto"
	slip10 "github.com/anyproto/go-slip10"
)

// Derivation paths — shared with anytype-heart (aclobjectmanager/
// pushnotificationkeys.go). MUST NOT change: both ends of a space
// derive independently, and the server holds the public halves forever
// (CreateSpace). The keys_test cross-check vectors pin these strings.
const (
	// spaceKeyDerivationPath is the SLIP-10 path from the ACL's first
	// metadata key to the per-space push signing key.
	spaceKeyDerivationPath = "m/99999'/1'"
	// encKeyDerivationPath is the SLIP-21 path from the current ACL
	// read key to the payload encryption key.
	encKeyDerivationPath = "m/SLIP-0021/anytype/space/key"
)

// DeriveSpaceKey derives the per-space push signing key from the ACL's
// FIRST metadata key (aclState.FirstMetadataKey — fixed at space
// creation, never rotates, held by every member). Its raw public key
// is the space's identity on the push server: Topic.SpaceKey and
// CreateSpace/RemoveSpace.SpaceKey; the private half signs topics and
// the space-registration account signature.
func DeriveSpaceKey(firstMetadataKey crypto.PrivKey) (crypto.PrivKey, error) {
	if firstMetadataKey == nil {
		return nil, errors.New("pushclient: nil first metadata key")
	}
	raw, err := firstMetadataKey.Raw()
	if err != nil {
		return nil, err
	}
	node, err := slip10.DeriveForPath(spaceKeyDerivationPath, raw)
	if err != nil {
		return nil, err
	}
	key, _, err := crypto.GenerateEd25519Key(bytes.NewReader(node.RawSeed()))
	return key, err
}

// DeriveEncKey derives the notification-payload encryption key from
// the space's CURRENT read key (aclState.CurrentReadKey). Unlike the
// space key it rotates with the read key — receivers resolve which key
// to decrypt with via the message's KeyId (EncKeyId), so a rotation
// mid-flight only costs a miss on messages encrypted under the old key.
func DeriveEncKey(readKey crypto.SymKey) (crypto.SymKey, error) {
	if readKey == nil {
		return nil, errors.New("pushclient: nil read key")
	}
	raw, err := readKey.Raw()
	if err != nil {
		return nil, err
	}
	return crypto.DeriveSymmetricKey(raw, encKeyDerivationPath)
}

// EncKeyId is the wire identifier of a payload encryption key:
// hex(sha256(rawKeyBytes)). Stamped into pushapi.Message.KeyId so a
// receiver holding several derived keys (read-key rotations) can pick
// the right one without trial decryption.
func EncKeyId(k crypto.SymKey) (string, error) {
	if k == nil {
		return "", errors.New("pushclient: nil enc key")
	}
	raw, err := k.Raw()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
