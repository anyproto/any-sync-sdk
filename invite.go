package syncsdk

import (
	"encoding/base64"
	"encoding/json"

	"github.com/anyproto/any-sync/util/crypto"
)

// invitePayload is the JSON structure encoded in an invite string.
type invitePayload struct {
	SpaceID string `json:"s"`
	Key     string `json:"k"`
	Type    int    `json:"t"`
}

// EncodeInvite creates a base64-encoded invite string from the given parameters.
func EncodeInvite(spaceID string, inviteKey crypto.PrivKey, approvalRequired bool) (string, error) {
	keyBytes, err := inviteKey.Marshall()
	if err != nil {
		return "", err
	}
	t := 1 // AnyoneCanJoin
	if approvalRequired {
		t = 0 // RequestToJoin
	}
	payload := invitePayload{
		SpaceID: spaceID,
		Key:     base64.StdEncoding.EncodeToString(keyBytes),
		Type:    t,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// DecodeInvite parses a base64-encoded invite string and returns its components.
func DecodeInvite(invite string) (spaceID string, inviteKey crypto.PrivKey, approvalRequired bool, err error) {
	data, err := base64.StdEncoding.DecodeString(invite)
	if err != nil {
		err = ErrInvalidInvite
		return
	}
	var payload invitePayload
	if err = json.Unmarshal(data, &payload); err != nil {
		err = ErrInvalidInvite
		return
	}
	if payload.SpaceID == "" || payload.Key == "" {
		err = ErrInvalidInvite
		return
	}
	keyBytes, decErr := base64.StdEncoding.DecodeString(payload.Key)
	if decErr != nil {
		err = ErrInvalidInvite
		return
	}
	inviteKey, err = crypto.UnmarshalEd25519PrivateKeyProto(keyBytes)
	if err != nil {
		err = ErrInvalidInvite
		return
	}
	spaceID = payload.SpaceID
	approvalRequired = payload.Type == 0
	return
}

// ParseInvite extracts just the space ID from an invite string without
// decoding the private key. Useful for lightweight validation.
func ParseInvite(invite string) (spaceID string, err error) {
	data, err := base64.StdEncoding.DecodeString(invite)
	if err != nil {
		return "", ErrInvalidInvite
	}
	var payload invitePayload
	if err = json.Unmarshal(data, &payload); err != nil {
		return "", ErrInvalidInvite
	}
	if payload.SpaceID == "" {
		return "", ErrInvalidInvite
	}
	return payload.SpaceID, nil
}
