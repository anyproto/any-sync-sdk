package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/space"
)

// aclAPI implements space.ACL on top of any-sync's per-space
// AclSpaceClient. The space is loaded through the App cache on each
// call — short-lived; the cache TTL keeps things idle when not in use.
type aclAPI struct {
	s *spaceImpl
}

func newACLAPI(s *spaceImpl) *aclAPI { return &aclAPI{s: s} }

// client loads the underlying any-sync space and pulls its ACL client.
// Each call rebinds — the cache returns the same loaded space until
// it TTL-evicts.
func (a *aclAPI) client(ctx context.Context) (aclclient.AclSpaceClient, error) {
	handle, err := a.s.app.GetSpace(ctx, a.s.id)
	if err != nil {
		return nil, fmt.Errorf("acl: load space: %w", err)
	}
	return handle.Inner().AclClient(), nil
}

// CreateInvite mints a fresh RequestToJoin invite, replacing any
// prior invite. Output is a share-friendly base58 token.
//
// The space must be registered with the coordinator before any ACL
// record can be published — otherwise the node returns
// "space not exists". CreateInvite calls SpaceMakeShareable
// transparently. The first call after Create may race with the
// periodic headsync that pushes the space header to the coordinator;
// we retry with a short backoff so callers don't have to.
func (a *aclAPI) CreateInvite(ctx context.Context) (space.Invite, error) {
	if err := ensureShareable(ctx, a.s); err != nil {
		return space.Invite{}, err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return space.Invite{}, err
	}
	res, err := cl.ReplaceInvite(ctx, aclclient.InvitePayload{
		InviteType: aclrecordproto.AclInviteType_RequestToJoin,
	})
	if err != nil {
		return space.Invite{}, fmt.Errorf("acl: replace invite: %w", err)
	}
	if err := cl.AddRecord(ctx, res.InviteRec); err != nil {
		return space.Invite{}, fmt.Errorf("acl: publish invite: %w", err)
	}
	return space.Invite{SpaceId: a.s.id, InviteKey: res.InviteKey}, nil
}

// ensureShareable calls coordinator.SpaceMakeShareable, retrying on
// "space not exists" to give the per-space headsync time to push the
// space header to the network. SpaceService.CreateSpace only writes
// locally; the headsync periodic loop pushes via SpacePush on the
// next tick (configured at 30s by anysyncx.GetSpace().SyncPeriod —
// the first tick fires immediately, but peer connections may not be
// established yet, so the first attempt is best-effort).
//
// Idempotent on the coordinator — calling it many times is cheap.
//
// Total retry window ≈ 35s — long enough to clear the worst case of
// peer-connect-then-syncperiod-tick.
func ensureShareable(ctx context.Context, s *spaceImpl) error {
	const (
		maxAttempts = 35
		backoff     = time.Second
	)
	coord := s.app.Coordinator()
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := coord.SpaceMakeShareable(ctx, s.id); err != nil {
			lastErr = err
			if !isSpaceNotPushedYet(err) {
				return fmt.Errorf("acl: make shareable: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("acl: make shareable (after %d attempts): %w", maxAttempts, lastErr)
}

// isSpaceNotPushedYet matches the coordinator's "space not exists"
// rejection. The error is wrapped through the DRPC layer; substring
// match is the pragmatic route since there's no exported sentinel.
func isSpaceNotPushedYet(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "space not exists") || strings.Contains(msg, "space does not exist")
}

func (a *aclAPI) RevokeInvite(ctx context.Context, inviteRecordId string) error {
	if inviteRecordId == "" {
		return errors.New("acl: RevokeInvite: inviteRecordId required")
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.RevokeInvite(ctx, inviteRecordId)
}

func (a *aclAPI) RevokeAllInvites(ctx context.Context) error {
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.RevokeAllInvites(ctx)
}

func (a *aclAPI) AcceptRequest(ctx context.Context, requestRecordId string, perm space.Permission) error {
	if requestRecordId == "" {
		return errors.New("acl: AcceptRequest: requestRecordId required")
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.AcceptRequest(ctx, list.RequestAcceptPayload{
		RequestRecordId: requestRecordId,
		Permissions:     toAclPermissions(perm),
	})
}

func (a *aclAPI) DeclineRequest(ctx context.Context, identity string) error {
	pk, err := decodeIdentity(identity)
	if err != nil {
		return err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.DeclineRequest(ctx, pk)
}

func (a *aclAPI) ChangePermissions(ctx context.Context, changes []space.PermissionChange) error {
	if len(changes) == 0 {
		return errors.New("acl: ChangePermissions: empty changes")
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	out := make([]list.PermissionChangePayload, 0, len(changes))
	for i, ch := range changes {
		pk, err := decodeIdentity(ch.Identity)
		if err != nil {
			return fmt.Errorf("acl: change %d: %w", i, err)
		}
		out = append(out, list.PermissionChangePayload{
			Identity:    pk,
			Permissions: toAclPermissions(ch.Permission),
		})
	}
	return cl.ChangePermissions(ctx, list.PermissionChangesPayload{Changes: out})
}

func (a *aclAPI) RemoveAccounts(ctx context.Context, identities []string) error {
	if len(identities) == 0 {
		return errors.New("acl: RemoveAccounts: empty identities")
	}
	pks := make([]crypto.PubKey, 0, len(identities))
	for i, id := range identities {
		pk, err := decodeIdentity(id)
		if err != nil {
			return fmt.Errorf("acl: identity %d: %w", i, err)
		}
		pks = append(pks, pk)
	}
	change, err := newReadKeyChange()
	if err != nil {
		return err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.RemoveAccounts(ctx, list.AccountRemovePayload{
		Identities: pks,
		Change:     change,
	})
}

func (a *aclAPI) AddAccounts(ctx context.Context, accounts []space.MemberAdd) error {
	if len(accounts) == 0 {
		return errors.New("acl: AddAccounts: empty accounts")
	}
	out := make([]list.AccountAdd, 0, len(accounts))
	for i, m := range accounts {
		pk, err := decodeIdentity(m.Identity)
		if err != nil {
			return fmt.Errorf("acl: account %d: %w", i, err)
		}
		out = append(out, list.AccountAdd{
			Identity:    pk,
			Permissions: toAclPermissions(m.Permission),
			Metadata:    encodeMetadata(m.Metadata),
		})
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.AddAccounts(ctx, list.AccountsAddPayload{Additions: out})
}

func (a *aclAPI) OwnershipChange(ctx context.Context, newOwner string, oldOwnerPerm space.Permission) error {
	pk, err := decodeIdentity(newOwner)
	if err != nil {
		return err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.OwnershipChange(ctx, pk, toAclPermissions(oldOwnerPerm))
}

func (a *aclAPI) RequestSelfRemove(ctx context.Context) error {
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.RequestSelfRemove(ctx)
}

func (a *aclAPI) CancelJoinRequest(ctx context.Context) error {
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.CancelRequest(ctx)
}

func (a *aclAPI) StopSharing(ctx context.Context) error {
	change, err := newReadKeyChange()
	if err != nil {
		return err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	return cl.StopSharing(ctx, change)
}

// newReadKeyChange mints a fresh metadata privkey + read key. The
// any-sync layer wraps these into the ACL record so removed members
// can no longer decrypt new content.
func newReadKeyChange() (list.ReadKeyChangePayload, error) {
	mdKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return list.ReadKeyChangePayload{}, fmt.Errorf("acl: metadata key: %w", err)
	}
	readKey, err := crypto.NewRandomAES()
	if err != nil {
		return list.ReadKeyChangePayload{}, fmt.Errorf("acl: read key: %w", err)
	}
	return list.ReadKeyChangePayload{MetadataKey: mdKey, ReadKey: readKey}, nil
}

// decodeIdentity parses the SDK-facing identity string (libp2p PeerId
// form, matching Account.Id()) into a crypto.PubKey.
func decodeIdentity(s string) (crypto.PubKey, error) {
	if s == "" {
		return nil, errors.New("acl: identity empty")
	}
	pk, err := crypto.DecodePeerId(s)
	if err != nil {
		return nil, fmt.Errorf("acl: decode identity %q: %w", s, err)
	}
	return pk, nil
}

// toAclPermissions maps the SDK Permission enum to any-sync's value.
// Defined as a 1:1 lookup (rather than int conversion) so a future
// reorder of either enum surfaces here as a compile error.
func toAclPermissions(p space.Permission) list.AclPermissions {
	switch p {
	case space.PermissionNone:
		return list.AclPermissionsNone
	case space.PermissionReader:
		return list.AclPermissionsReader
	case space.PermissionGuest:
		return list.AclPermissionsGuest
	case space.PermissionWriter:
		return list.AclPermissionsWriter
	case space.PermissionAdmin:
		return list.AclPermissionsAdmin
	case space.PermissionOwner:
		return list.AclPermissionsOwner
	default:
		return list.AclPermissionsNone
	}
}

// fromAclPermissions is the reverse mapping — used by the read-side
// MembersAPI when decoding AclState into Member entries.
func fromAclPermissions(p list.AclPermissions) space.Permission {
	switch p {
	case list.AclPermissionsNone:
		return space.PermissionNone
	case list.AclPermissionsReader:
		return space.PermissionReader
	case list.AclPermissionsGuest:
		return space.PermissionGuest
	case list.AclPermissionsWriter:
		return space.PermissionWriter
	case list.AclPermissionsAdmin:
		return space.PermissionAdmin
	case list.AclPermissionsOwner:
		return space.PermissionOwner
	default:
		return space.PermissionNone
	}
}

// encodeMetadata flattens an AccountMetadata into the wire bytes that
// land on the ACL record. Same NUL-separated layout as
// space.EncodeAccountMetadata — kept as a thin alias so internal call
// sites stay short.
func encodeMetadata(m space.AccountMetadata) []byte { return space.EncodeAccountMetadata(m) }

// decodeMetadata is the inverse — public alias so internal callers
// don't have to import the same helper from two places.
func decodeMetadata(b []byte) space.AccountMetadata { return space.DecodeAccountMetadata(b) }
