// Direct-add invite payload — the body of an InboxPayloadRegularInvite
// coordinator-inbox message (SYN-46). Sent after ACL AddAccounts as a
// notification only: the receiver is already a full ACL member, and the
// body just tells their devices which space to surface as pending.

package spaceimpl

import (
	"encoding/json"
	"errors"
	"fmt"
)

// regularInviteVersion is the current regularInviteBody format version.
// Bump on incompatible shape changes; decode rejects unknown versions.
const regularInviteVersion = 1

// errInvalidRegularInvite flags an undecodable / malformed invite body.
// Non-retryable: the notifier logs and advances past the message.
var errInvalidRegularInvite = errors.New("spaceimpl: invalid regular-invite body")

// regularInviteBody is the JSON body of a direct-add invite. The whole
// message is encrypted to the receiver and signed on send by any-sync's
// InboxAddMessage; the sender is the coordinator-verified
// SenderIdentity, never anything self-declared here.
//
// SymKey is the sender's own metadata symkey (encodeSelfSymKeyMetadata
// value) so the receiver can resolve the sender's profile. Name and
// SpaceType are UNAUTHENTICATED display hints — shown on the pending
// row, replaced by the synced truth after accept. SpaceId is the one
// load-bearing field; its real validation is membership: a bogus id
// simply never loads.
type regularInviteBody struct {
	Version   int    `json:"v"`
	SpaceId   string `json:"spaceId"`
	SymKey    string `json:"symKey,omitempty"`
	Name      string `json:"name,omitempty"`
	SpaceType string `json:"spaceType,omitempty"`
}

// encodeRegularInviteBody packs b into wire bytes, stamping the current
// version. SpaceId is required.
func encodeRegularInviteBody(b regularInviteBody) ([]byte, error) {
	if b.SpaceId == "" {
		return nil, fmt.Errorf("%w: empty spaceId", errInvalidRegularInvite)
	}
	b.Version = regularInviteVersion
	return json.Marshal(b)
}

// decodeRegularInviteBody parses wire bytes strictly: bad JSON, an
// unknown version, or a missing spaceId all return
// errInvalidRegularInvite (errors.Is-able).
func decodeRegularInviteBody(raw []byte) (regularInviteBody, error) {
	var b regularInviteBody
	if err := json.Unmarshal(raw, &b); err != nil {
		return regularInviteBody{}, fmt.Errorf("%w: %v", errInvalidRegularInvite, err)
	}
	if b.Version != regularInviteVersion {
		return regularInviteBody{}, fmt.Errorf("%w: unsupported version %d", errInvalidRegularInvite, b.Version)
	}
	if b.SpaceId == "" {
		return regularInviteBody{}, fmt.Errorf("%w: empty spaceId", errInvalidRegularInvite)
	}
	return b, nil
}
