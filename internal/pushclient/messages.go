// Request builders — the pure functions that produce every signed /
// encrypted byte the push server verifies. Kept free of transport and
// key-provider concerns so the contract test (messages_test.go) can
// check them against a replica of the server's verification code; the
// DRPC shim in client.go then stays untested, like the files broker.
//
// Server-side checks these bytes must satisfy (anytype-push-server):
//
//   - Topic: UnmarshalEd25519PublicKey(SpaceKey).Verify([]byte(Topic),
//     Signature) — the topic STRING is the signed message.
//   - CreateSpace/RemoveSpace: UnmarshalEd25519PublicKey(SpaceKey).
//     Verify([]byte(identity), AccountSignature) where identity is the
//     dialing account's StrKey form (PubKey.Account()) read off the
//     secure-channel handshake.
//   - Notify message: accountPubKey.Verify(Payload, Signature) — the
//     signature is over the CIPHERTEXT, verified against the dialer.

package pushclient

import (
	"fmt"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/anyproto/anytype-push-server/pushclient/pushapi"
)

// buildTopics signs each topic string with the space push key and
// packs them with its raw public key. The server stores/matches topics
// by (SpaceKey, Topic); the signature proves space membership (only
// members can derive the push key from the ACL).
func buildTopics(spaceKey crypto.PrivKey, topics []string) (*pushapi.Topics, error) {
	rawPub, err := spaceKey.GetPublic().Raw()
	if err != nil {
		return nil, fmt.Errorf("pushclient: raw space pub key: %w", err)
	}
	out := &pushapi.Topics{Topics: make([]*pushapi.Topic, 0, len(topics))}
	for _, topic := range topics {
		sig, err := spaceKey.Sign([]byte(topic))
		if err != nil {
			return nil, fmt.Errorf("pushclient: sign topic %q: %w", topic, err)
		}
		out.Topics = append(out.Topics, &pushapi.Topic{
			SpaceKey:  rawPub,
			Topic:     topic,
			Signature: sig,
		})
	}
	return out, nil
}

// buildSpaceRequest builds the CreateSpace / RemoveSpace body (both
// share the shape): the raw space push public key plus the space key's
// signature over the caller's account identity string. The signature
// binds the space key to THIS account — the server checks the identity
// it sees on the secure channel is the one the space key vouched for.
func buildSpaceRequest(spaceKey crypto.PrivKey, accountIdentity string) (spaceKeyRaw, accountSignature []byte, err error) {
	spaceKeyRaw, err = spaceKey.GetPublic().Raw()
	if err != nil {
		return nil, nil, fmt.Errorf("pushclient: raw space pub key: %w", err)
	}
	accountSignature, err = spaceKey.Sign([]byte(accountIdentity))
	if err != nil {
		return nil, nil, fmt.Errorf("pushclient: sign identity: %w", err)
	}
	return spaceKeyRaw, accountSignature, nil
}

// buildMessage encrypts a cleartext notification payload and signs the
// ciphertext: Payload = encKey.Encrypt(cleartext), KeyId = EncKeyId
// (hex sha256 of the raw enc key — the receiver's key selector),
// Signature = accountKey.Sign(ciphertext). The server verifies the
// signature against the dialing account and relays the opaque bytes.
func buildMessage(accountKey crypto.PrivKey, encKey crypto.SymKey, cleartext []byte) (*pushapi.Message, error) {
	ciphertext, err := encKey.Encrypt(cleartext)
	if err != nil {
		return nil, fmt.Errorf("pushclient: encrypt payload: %w", err)
	}
	keyId, err := EncKeyId(encKey)
	if err != nil {
		return nil, err
	}
	sig, err := accountKey.Sign(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("pushclient: sign payload: %w", err)
	}
	return &pushapi.Message{
		KeyId:     keyId,
		Payload:   ciphertext,
		Signature: sig,
	}, nil
}
