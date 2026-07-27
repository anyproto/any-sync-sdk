package spaceimpl

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/util/crypto"
)

// Issued-key custody: an ACL invite record carries only the invite
// PUBLIC key, so the private half a mint returns is persisted on the
// issuing account's tech-space row (techspace.FieldIssuedInviteKeys,
// synced) — every device of that account can re-show or revoke the
// same invite. Persistence is kind-agnostic; matching custody against
// live ACL state stays kind-specific (guest = ACL account identity,
// member = invite-record key), so that logic lives at the call sites.

// persistIssuedKey stores key as the custody entry for kind.
func (s *spaceImpl) persistIssuedKey(ctx context.Context, kind string, key crypto.PrivKey) error {
	encoded, err := crypto.EncodeKeyToString(key)
	if err != nil {
		return fmt.Errorf("acl: encode issued %s key: %w", kind, err)
	}
	if _, err := s.tsp.SetIssuedInviteKey(ctx, s.id, kind, encoded); err != nil {
		return fmt.Errorf("acl: persist issued %s key: %w", kind, err)
	}
	return nil
}

// loadIssuedKey returns the custody entry for kind; false when absent
// or undecodable (stale garbage reads as no custody).
func (s *spaceImpl) loadIssuedKey(ctx context.Context, kind string) (crypto.PrivKey, bool) {
	rec, ok := s.tsp.Get(ctx, s.id)
	if !ok {
		return nil, false
	}
	encoded := rec.IssuedInviteKey(kind)
	if encoded == "" {
		return nil, false
	}
	key, err := crypto.DecodeKeyFromString(encoded, crypto.UnmarshalEd25519PrivateKey, nil)
	if err != nil {
		return nil, false
	}
	return key, true
}

// clearIssuedKey removes the custody entry for kind. No-op when absent.
func (s *spaceImpl) clearIssuedKey(ctx context.Context, kind string) error {
	rec, ok := s.tsp.Get(ctx, s.id)
	if !ok || rec.IssuedInviteKey(kind) == "" {
		return nil
	}
	if _, err := s.tsp.SetIssuedInviteKey(ctx, s.id, kind, ""); err != nil {
		return fmt.Errorf("acl: clear issued %s key: %w", kind, err)
	}
	return nil
}
