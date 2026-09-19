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
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
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

// CreateInvite returns the active request-to-join invite to any member.
// Only owners/admins can mint one; approval and revocation stay ACL-gated.
// Keys are shared through the encrypted spaceIndex, with the issuer's
// private tech-space custody retained as a recovery/migration source.
func (a *aclAPI) CreateInvite(ctx context.Context) (space.Invite, error) {
	invites, err := a.s.members.Invites(ctx)
	if err != nil {
		return space.Invite{}, err
	}
	for _, inv := range invites {
		if inv.Key != nil {
			if a.s.canWrite(ctx) {
				if err := a.s.publishInviteKey(ctx, inv.Key); err != nil {
					return space.Invite{}, err
				}
			}
			return space.Invite{SpaceId: a.s.id, InviteKey: inv.Key}, nil
		}
	}
	me, err := a.s.members.Me(ctx)
	if err != nil {
		return space.Invite{}, err
	}
	if me.Permission != space.PermissionOwner && me.Permission != space.PermissionAdmin {
		return space.Invite{}, space.ErrInsufficientPermissions
	}
	if me.Permission == space.PermissionOwner {
		if err := ensureShareable(ctx, a.s); err != nil {
			return space.Invite{}, err
		}
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
	if err := addRecordWaitingForLog(ctx, cl, res.InviteRec); err != nil {
		return space.Invite{}, fmt.Errorf("acl: publish invite: %w", err)
	}
	if err := a.s.persistIssuedKey(ctx, techspace.IssuedKeyMember, res.InviteKey); err != nil {
		return space.Invite{}, err
	}
	if err := a.s.publishInviteKey(ctx, res.InviteKey); err != nil {
		return space.Invite{}, err
	}
	return space.Invite{SpaceId: a.s.id, InviteKey: res.InviteKey}, nil
}

// addRecordWaitingForLog publishes an ACL record, retrying while the
// space's consensus log is still being created. ensureShareable only
// confirms the coordinator has the space header; the consensus ACL log
// is created separately and lazily by a tree node when it first loads
// the pushed space (nodeSpace.Init -> consensusclient.AddLog). Until
// that lands, the node/coordinator reject the record with "log not
// found". This is a transient startup race on a freshly-created space,
// so we retry with a short backoff instead of surfacing it to callers.
func addRecordWaitingForLog(ctx context.Context, cl aclclient.AclSpaceClient, rec *consensusproto.RawRecord) error {
	return callWaitingForLog(ctx, func() error { return cl.AddRecord(ctx, rec) })
}

// callWaitingForLog runs an ACL publish call with the log-not-ready
// retry described on addRecordWaitingForLog. call is re-invoked whole on
// each attempt — for client methods without a build/publish split (e.g.
// AddAccounts) the record is rebuilt per attempt, which is fine: prior
// attempts were rejected, so no duplicate can land.
func callWaitingForLog(ctx context.Context, call func() error) error {
	const (
		maxAttempts = 35
		backoff     = time.Second
	)
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if err := call(); err != nil {
			lastErr = err
			if !isLogNotReady(err) {
				return err
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
	return fmt.Errorf("consensus log not ready after %d attempts: %w", maxAttempts, lastErr)
}

// isLogNotReady matches the "log not found" rejection returned while a
// freshly-created space's consensus ACL log is still being established.
// The error crosses the DRPC boundary, so substring match is the
// pragmatic route — there's no exported sentinel to errors.Is against.
func isLogNotReady(err error) bool {
	return err != nil && strings.Contains(err.Error(), "log not found")
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
	if err := cl.RevokeInvite(ctx, inviteRecordId); err != nil {
		return err
	}
	a.clearStaleMemberCustody(ctx)
	return nil
}

func (a *aclAPI) RevokeAllInvites(ctx context.Context) error {
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	if err := cl.RevokeAllInvites(ctx); err != nil {
		return err
	}
	a.clearStaleMemberCustody(ctx)
	return nil
}

// clearStaleMemberCustody drops this account's member-invite custody
// once it no longer matches an active invite record — called after a
// successful revoke. Best-effort: the revoke already landed and the
// read path hides stale custody, so failures are swallowed.
func (a *aclAPI) clearStaleMemberCustody(ctx context.Context) {
	key, ok := a.s.loadIssuedKey(ctx, techspace.IssuedKeyMember)
	if !ok {
		return
	}
	handle, err := a.s.app.GetSpace(ctx, a.s.id)
	if err != nil {
		return
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return
	}
	acl.RLock()
	_, matchErr := acl.AclState().GetInviteIdByPrivKey(key)
	acl.RUnlock()
	if matchErr == nil {
		// Custody still matches an active invite — keep it.
		return
	}
	_ = a.s.clearIssuedKey(ctx, techspace.IssuedKeyMember)
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
	// Same precondition as CreateInvite: the ACL record is rejected until
	// the coordinator knows the space ("space not exists" / log not
	// found), so register it shareable first. Idempotent + retried.
	if err := ensureShareable(ctx, a.s); err != nil {
		return err
	}
	out := make([]list.AccountAdd, 0, len(accounts))
	for i, m := range accounts {
		pk, err := decodeIdentity(m.Identity)
		if err != nil {
			return fmt.Errorf("acl: account %d: %w", i, err)
		}
		// Symkey-only metadata: the ACL carries a member's own metadata
		// symkey (m.Metadata name/icon is no longer written). An owner
		// adding accounts directly can't produce another account's key,
		// but if it has already learned that member's symkey (from a prior
		// 1-1 or shared space), it forwards it here so the other members
		// can resolve the added member's profile. Absent a cached key the
		// record carries no metadata, and resolution waits until the key
		// reaches this account another way.
		add := list.AccountAdd{
			Identity:    pk,
			Permissions: toAclPermissions(m.Permission),
		}
		if enc, ok := a.s.tsp.GetIdentityMetaKey(ctx, m.Identity); ok {
			add.Metadata = []byte(enc)
		}
		out = append(out, add)
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	if err := callWaitingForLog(ctx, func() error {
		return cl.AddAccounts(ctx, list.AccountsAddPayload{Additions: out})
	}); err != nil {
		return err
	}
	// Membership is effective; notify the added accounts via the
	// coordinator inbox (durable, retried) so the space surfaces on their
	// devices as invite-pending. Queue failures are logged, never fail
	// the add.
	identities := make([]string, 0, len(accounts))
	for _, m := range accounts {
		identities = append(identities, m.Identity)
	}
	a.s.parent.markRegularInvitesToSend(ctx, a.s.id, identities)
	return nil
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

// CreateGuestKey mints (or returns) the space's shared read-only guest
// identity — see space.ACL.CreateGuestKey. Idempotent via owner-side
// custody: the private key is persisted on the owner's tech-space row
// (it is not recoverable from the ACL), so repeated calls return the
// same invite while the guest account is still active.
//
// The custody field and the ACL are reconciled to hold exactly ONE
// guest identity: any ACL guest account the custody doesn't know —
// a concurrent mint on another device, a failed custody persist, a
// stored key whose account was revoked elsewhere — is removed (with a
// read-key rotation) before a fresh identity is minted. Without this,
// an orphaned guest identity would survive every future revocation:
// RemoveAccounts re-encrypts the rotated key for remaining members,
// guests included.
func (a *aclAPI) CreateGuestKey(ctx context.Context) (space.Invite, error) {
	if !a.s.localIdentityIsOwner(ctx) {
		return space.Invite{}, errors.New("acl: CreateGuestKey: owner only")
	}
	if guestKey, ok := a.s.loadIssuedKey(ctx, techspace.IssuedKeyGuest); ok {
		guests, gErr := a.activeGuestIdentities(ctx)
		if gErr != nil {
			return space.Invite{}, gErr
		}
		// Custody active and no orphans — the idempotent fast path.
		if len(guests) == 1 && guests[0].Equals(guestKey.GetPublic()) {
			return space.Invite{SpaceId: a.s.id, InviteKey: guestKey, Kind: space.InviteKindGuest}, nil
		}
	}
	// Same publish preconditions as AddAccounts: coordinator must know
	// the space, consensus log must exist. No metadata and no inbox
	// notification — the guest identity is synthetic, nobody's device
	// listens for it.
	if err := ensureShareable(ctx, a.s); err != nil {
		return space.Invite{}, err
	}
	if err := a.removeAllGuestIdentities(ctx); err != nil {
		return space.Invite{}, err
	}
	guestKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return space.Invite{}, fmt.Errorf("acl: guest key: %w", err)
	}
	cl, err := a.client(ctx)
	if err != nil {
		return space.Invite{}, err
	}
	if err := callWaitingForLog(ctx, func() error {
		return cl.AddAccounts(ctx, list.AccountsAddPayload{Additions: []list.AccountAdd{{
			Identity:    guestKey.GetPublic(),
			Permissions: list.AclPermissionsGuest,
		}}})
	}); err != nil {
		return space.Invite{}, fmt.Errorf("acl: add guest account: %w", err)
	}
	if err := a.s.persistIssuedKey(ctx, techspace.IssuedKeyGuest, guestKey); err != nil {
		return space.Invite{}, err
	}
	return space.Invite{SpaceId: a.s.id, InviteKey: guestKey, Kind: space.InviteKindGuest}, nil
}

// RevokeGuestKey removes EVERY guest identity from the ACL (custodied
// or orphaned — public access must end regardless of which device
// minted what), rotating the read key, and clears the owner-side
// custody — see space.ACL.RevokeGuestKey.
func (a *aclAPI) RevokeGuestKey(ctx context.Context) error {
	if !a.s.localIdentityIsOwner(ctx) {
		return errors.New("acl: RevokeGuestKey: owner only")
	}
	rec, _ := a.s.tsp.Get(ctx, a.s.id)
	stored := rec.IssuedInviteKey(techspace.IssuedKeyGuest)
	guests, err := a.activeGuestIdentities(ctx)
	if err != nil {
		return err
	}
	if len(guests) == 0 && stored == "" {
		return fmt.Errorf("acl: RevokeGuestKey: %w", space.ErrNoActiveGuestKey)
	}
	if err := a.removeAllGuestIdentities(ctx); err != nil {
		return err
	}
	return a.s.clearIssuedKey(ctx, techspace.IssuedKeyGuest)
}

// activeGuestIdentities lists every ACL account currently holding
// guest permission.
func (a *aclAPI) activeGuestIdentities(ctx context.Context) ([]crypto.PubKey, error) {
	handle, err := a.s.app.GetSpace(ctx, a.s.id)
	if err != nil {
		return nil, fmt.Errorf("acl: load space: %w", err)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return nil, errors.New("acl: space has no acl")
	}
	acl.RLock()
	defer acl.RUnlock()
	var out []crypto.PubKey
	for _, acc := range acl.AclState().CurrentAccounts() {
		if acc.Permissions.IsGuest() {
			out = append(out, acc.PubKey)
		}
	}
	return out, nil
}

// removeAllGuestIdentities drops every guest account from the ACL in
// one RemoveAccounts (read-key rotation included). No-op when none.
func (a *aclAPI) removeAllGuestIdentities(ctx context.Context) error {
	guests, err := a.activeGuestIdentities(ctx)
	if err != nil {
		return err
	}
	if len(guests) == 0 {
		return nil
	}
	change, err := newReadKeyChange()
	if err != nil {
		return err
	}
	cl, err := a.client(ctx)
	if err != nil {
		return err
	}
	if err := cl.RemoveAccounts(ctx, list.AccountRemovePayload{
		Identities: guests,
		Change:     change,
	}); err != nil {
		return fmt.Errorf("acl: remove guest accounts: %w", err)
	}
	return nil
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
	if err := cl.StopSharing(ctx, change); err != nil {
		return err
	}
	// StopSharing revokes every invite as a side effect.
	a.clearStaleMemberCustody(ctx)
	return nil
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

// decodeIdentity parses the SDK-facing identity string (StrKey account
// address form, matching Account.Id()) into a crypto.PubKey.
func decodeIdentity(s string) (crypto.PubKey, error) {
	if s == "" {
		return nil, fmt.Errorf("acl: identity empty: %w", space.ErrBadIdentity)
	}
	pk, err := crypto.DecodeAccountAddress(s)
	if err != nil {
		return nil, fmt.Errorf("acl: decode identity %q: %w: %w", s, space.ErrBadIdentity, err)
	}
	return pk, nil
}

// selfAclActive reports whether state places the account's own
// identity in StatusActive. Caller holds the ACL lock.
func selfAclActive(state *list.AclState) bool {
	me := state.Identity()
	if me == nil {
		return false
	}
	for _, acc := range state.CurrentAccounts() {
		if acc.PubKey.Equals(me) {
			return acc.Status == list.StatusActive
		}
	}
	return false
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

// ownRole resolves this account's current ACL permission in spaceId
// from the loaded space's ACL state — the ACL mirror's source for the
// row's ownRole field. Same access pattern as PushKeys (space via the
// app cache, ACL read under RLock). An account absent from the ACL
// (e.g. a still-pending joiner) resolves to PermissionNone without
// error — that IS its current permission.
func (s *Service) ownRole(ctx context.Context, spaceId string) (space.Permission, error) {
	handle, err := s.app.GetSpace(ctx, spaceId)
	if err != nil {
		return space.PermissionNone, fmt.Errorf("ownRole: load space %q: %w", spaceId, err)
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return space.PermissionNone, fmt.Errorf("ownRole: space %q has no acl", spaceId)
	}
	acl.RLock()
	state := acl.AclState()
	perms := state.Permissions(state.Identity())
	acl.RUnlock()
	return fromAclPermissions(perms), nil
}

// encodeSelfSymKeyMetadata derives this account's metadata symkey and
// returns its marshalled bytes for the ACL RequestMetadata / create
// payload. The ACL carries ONLY the symkey, never inline name/icon —
// co-members cache the key and resolve the profile from identityRepo
// (see docs/one-to-one-spaces.md). any-sync encrypts these bytes at rest with the space
// metadata key, so only members can read the symkey. Returns nil bytes
// on derive/marshal failure (the member still joins; their profile just
// stays unresolved until a later key arrival).
func encodeSelfSymKeyMetadata(signKey crypto.PrivKey) ([]byte, error) {
	k, err := space.DeriveAccountMetadataSymKey(signKey)
	if err != nil {
		return nil, err
	}
	s, err := space.MarshalSymKey(k)
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

// decodeSymKeyMetadata decrypts an active member's ACL RequestMetadata
// (encrypted at rest with the space metadata key) and returns the
// contact's metadata symkey string, "" when absent or the caller lacks
// the metadata key.
func decodeSymKeyMetadata(raw []byte, keys map[string]list.AclKeys, keyRecordId string) string {
	if len(raw) == 0 {
		return ""
	}
	k, ok := keys[keyRecordId]
	if !ok || k.MetadataPrivKey == nil {
		return ""
	}
	plain, err := k.MetadataPrivKey.Decrypt(raw)
	if err != nil {
		return ""
	}
	return string(plain)
}
