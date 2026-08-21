// The space-index handler — the crdt.Handler for the tech space's
// list-of-spaces dataset. See doc.go for the package role.

package techspace

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// SpaceIndexSchema declares the `spaces` dataset fields and their class.
// Synced metadata mirrors across the account's devices; localStatus is
// per-device (never synced); remoteStatus carries the account-wide
// delete. Used as the controller's enforced schema and surfaced to
// consumers via discovery.
func SpaceIndexSchema() schema.Dataset {
	str := func() *schema.Schema { return schema.Leaf(schema.KindString) }
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldType, Name: "Type", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldName, Name: "Name", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldDescription, Name: "Description", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldIcon, Name: "Icon", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldSpaceType, Name: "Space type", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldRemoteStatus, Name: "Remote status", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldLocalStatus, Name: "Local status", Schema: str(), Scope: schema.ScopeLocal},
		{Id: FieldAclHeadId, Name: "Acl head id", Schema: str(), Scope: schema.ScopeLocal},
		{Id: FieldOneToOneInviteState, Name: "One-to-one invite state", Schema: str(), Scope: schema.ScopeLocal},
		{Id: FieldInviteNotifyPending, Name: "Invite notify pending", Schema: &schema.Schema{Kind: schema.KindArray, Items: str()}, Scope: schema.ScopeLocal},
		{Id: FieldOneToOnePeer, Name: "One-to-one peer", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldDerived, Name: "Derived", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeSynced},
		{Id: FieldCreatedAt, Name: "Created at", Schema: schema.Leaf(schema.KindDatetime), Scope: schema.ScopeDerived},
		// KindObject with nil Properties = free-form shape: the schema
		// validator accepts any nested keys (schema.validateValue stops at
		// an untyped object) and the controller's field-class enforcement
		// only looks at the top-level head, so arbitrary client keys under
		// `settings` are permitted by declaration.
		{Id: FieldSettings, Name: "Settings", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced},
		{Id: FieldPushKeys, Name: "Push keys", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeLocal},
		{Id: FieldOwnRole, Name: "Own role", Schema: str(), Scope: schema.ScopeLocal},
		{Id: FieldGuestKey, Name: "Guest key", Schema: str(), Scope: schema.ScopeSynced},
		// Free-form object like `settings`: subkeys are the issued-key
		// kinds, validated by the setter, not the schema.
		{Id: FieldIssuedInviteKeys, Name: "Issued invite keys", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced},
	}}
}

// SpaceIndexDeriveSeed mints the same space-index object id on every
// device for a given account. Tech space has exactly one space-index
// object, always derived from this seed, always owned by the
// account's own (owner-only) ACL.
const SpaceIndexDeriveSeed = "builtin:spaceIndex"

// SpaceIndexDataset is the name of the dataset on the space-index
// object that holds one record per space. Record id is the spaceId;
// fields are the caller-visible space metadata (type, name, icon,
// localStatus, remoteStatus, …) per docs/02-tech-space.md § "Space
// Index".
const SpaceIndexDataset = "spaces"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits. Tech-space datasets are account-private
// (owner-only ACL), so the only writer is the SDK itself; bump the
// suffix when the schema changes in a way that must reject stale
// writers.
const HandlerVersion = "spaceIndexHandler-v1"

// Space-index record fields. The shape is hardcoded — tech space is
// account-private; no cross-version writers to negotiate with.
const (
	FieldType        = "type"
	FieldName        = "name"
	FieldDescription = "description"
	FieldIcon        = "icon"
	// FieldLocalStatus is a DEVICE-LOCAL field (schema.ScopeLocal):
	// per-device lifecycle (active/joining/offloaded) that must NOT sync
	// — a space offloaded on one device must stay loaded on another, and
	// the value is meaningless offline or on a different network. Written
	// only via Service.SetLocalStatus → Object.LocalSet; never enters the
	// DAG. Absence means active.
	FieldLocalStatus = "localStatus"
	// FieldRemoteStatus is synced (account-wide). It carries the
	// account-wide delete signal (StatusDeleted) so every device drops
	// the space; the handler keeps it terminal.
	FieldRemoteStatus = "remoteStatus"
	// FieldAclHeadId is a DEVICE-LOCAL field (schema.ScopeLocal): the ACL
	// head id returned by RequestJoin, recorded on the joining row so the
	// joiner-side post-acceptance waiter can detect a decline (the join
	// record being removed at-or-after this head). Per-device like
	// localStatus — a pending join is a local lifecycle concern, never
	// synced. Written via Service.SetAclHeadId → Object.LocalSet; cleared
	// implicitly once the row reaches active (no further reads). Absent on
	// rows that never went through Join.
	FieldAclHeadId = "aclHeadId"
	// FieldSpaceType mirrors the in-space spaceIndex.spaceType app tag.
	// Distinct from FieldType (the on-wire header type): not pinned, so
	// the watcher can mirror the converged value.
	FieldSpaceType = "spaceType"
	// FieldOneToOneInviteState is a DEVICE-LOCAL field (schema.ScopeLocal)
	// tracking whether this device still owes the peer an inbox
	// notification for a 1-1 it initiated. Value "toSend" means the
	// send-retry loop should (re)deliver the InboxPayloadOneToOneInvite;
	// cleared (absent/"") once the coordinator confirms the send. Local
	// because delivery is a per-device obligation — only the device that
	// initiated owes the notification, and the obligation is meaningless
	// after it's met. Written via Service.SetOneToOneInviteState.
	FieldOneToOneInviteState = "oneToOneInviteState"
	// FieldInviteNotifyPending is a DEVICE-LOCAL array (schema.ScopeLocal)
	// of account identities this device still owes a RegularInvite inbox
	// notification for, after a successful ACL AddAccounts on this space.
	// Entries are removed one-by-one on confirmed delivery (or when
	// permanently undeliverable). Local because delivery is a per-device
	// obligation — only the device that ran AddAccounts owes it. The
	// many-receiver generalization of FieldOneToOneInviteState. Written via
	// Service.AddInviteNotify / ClearInviteNotify.
	FieldInviteNotifyPending = "inviteNotifyPending"
	// FieldOneToOnePeer is the other participant's account identity on a
	// derived 1-1 row (synced, account-wide). It is the one piece of state
	// a spaceId does not encode invertibly, and it is required to
	// materialize the space's storage (DeriveOneToOneSpace needs the peer
	// pubkey). Stored on the pending row so AcceptOneToOne can build
	// storage, and on active rows so any of the account's devices can
	// re-derive. Empty on non-1-1 rows.
	FieldOneToOnePeer = "oneToOnePeer"
	// FieldSettings is the client-writable, free-form per-space settings
	// object (SYNCED): account-private via the tech-space's owner-only
	// ACL, replicated across the account's devices, invisible to other
	// space members. Keys are the client's own vocabulary — the SDK
	// stores them verbatim under per-key paths (settings.<key>) via
	// Service.SetSettings, so concurrent edits to DIFFERENT keys merge
	// instead of last-writer-wins on the whole object. SDK-owned fields
	// stay siblings of `settings`, never inside it, and the spaceIndex
	// mirror (SetSpaceMetadata) never touches it.
	FieldSettings = "settings"
	// FieldCreatedAt is the added-to-account time: a datetime instant,
	// stamped by BeforeCreate from the creating change's timestamp when
	// the row first lands locally (Create / Derive / OneToOne / Join all
	// create the row once). ScopeDerived — handler-only, no input op may
	// write it, so it's immutable for life.
	//
	// Caveats (accepted — the value is advisory ordering metadata):
	//   - stamping is per-device first-touch: a device whose store
	//     materialized the row under an older handler reads nothing until
	//     a re-index replays the row's create change (SpaceIndexLocalVersion
	//     drives exactly that), while a device replaying the same DAG with
	//     this handler stamps the real value;
	//   - two devices independently creating the same row (e.g. both
	//     Derive/Join before tech-space sync converges) each keep their
	//     own change's timestamp — typically seconds apart.
	// Callers treat an absent stamp as "unknown" (SpaceIndexRecord
	// reports it as 0).
	FieldCreatedAt = "createdAt"
)

// SpaceIndexLocalVersion is the spaces handler's LOCAL logic version
// (HandlerReg.Version) — bumped when already-materialized rows would
// come out different, so the SDK rebuilds them from the DAG
// (docs/08-versioning.md). v2: the derived createdAt stamp is a
// TypeDateTime instant, not an epoch number.
const SpaceIndexLocalVersion = 2

const (
	// FieldPushKeys is a DEVICE-LOCAL object (schema.ScopeLocal) holding
	// the space's push-notification key material, mirrored from ACL
	// state by spaceimpl's per-space ACL mirror watcher so clients can
	// read it off the row (and its subscribe stream), cache it, and
	// decrypt push payloads while the SDK process is down. Subfields:
	// PushKeySpaceKey / PushKeyEncKey / PushKeyEncKeyId. Local, not
	// synced, because every device derives the same values from the
	// same converged ACL — syncing would only add DAG writes. Written
	// via Service.SetPushKeys → Object.LocalSet; absent until the
	// mirror first runs (e.g. joiner without read access yet).
	FieldPushKeys = "push"
	// FieldOwnRole is a DEVICE-LOCAL string (schema.ScopeLocal): this
	// account's own ACL permission in the space, in the canonical
	// space.Permission wire vocabulary ("owner" / "admin" / "writer" /
	// "reader" / "guest" / "none"). Mirrored from ACL state by the same
	// per-space ACL watcher that maintains FieldPushKeys — local, not
	// synced, for the same reason (every device derives it from the
	// same converged ACL). Written via Service.SetOwnRole →
	// Object.LocalSet; absent until the mirror first runs, which
	// readers must treat as "unknown yet", not as no-access.
	FieldOwnRole = "ownRole"
	// FieldGuestKey marks a guest-mode (public-access) space: the shared
	// read-only guest identity's private key, string-encoded, written
	// once by Service (JoinGuest path) when the account adds the space
	// via a guest invite. SYNCED — any of the account's devices opens
	// the space with the same identity. Presence is the guest-mode
	// discriminator for space loading and the write gate; empty on all
	// other rows. Account-private via the tech space's owner-only ACL.
	FieldGuestKey = "guestKey"
	// FieldIssuedInviteKeys is this account's custody of the invite
	// private keys it has issued for the space, one subkey per kind
	// (IssuedKeyMember / IssuedKeyGuest), each a string-encoded private
	// key. SYNCED so every device of the issuing account can re-show or
	// revoke the same invite (the private part is not recoverable from
	// the ACL — records carry only the public key). Per-kind subkeys
	// merge per-path: minting a member invite on one device and a guest
	// key on another never clobber each other. Written / cleared via
	// Service.SetIssuedInviteKey by ACL.CreateInvite / CreateGuestKey
	// and the revoke paths. Distinct from FieldGuestKey so an issuer's
	// own row never reads as guest-mode.
	FieldIssuedInviteKeys = "issuedInviteKeys"
	// FieldDerived marks a row written by the account's own
	// Spaces().Derive (SYNCED bool, stamped at row create or healed by
	// SetDerived; set-once — the handler pins it like `type`). Gates
	// every delete refusal for derived spaces, including the handler's
	// own remoteStatus=deleted rejection; why they are permanent is on
	// space.ErrIsDerivedSpace. Absent on created / joined / tracked /
	// 1-1 rows.
	FieldDerived = "derived"
)

// FieldIssuedInviteKeys subkeys — the issued-key kinds. Slugs, not
// space.InviteKind bytes, so the row stays readable.
const (
	// IssuedKeyMember is the RequestToJoin member-invite key custody —
	// written by ACL.CreateInvite (any account permitted to mint).
	IssuedKeyMember = "member"
	// IssuedKeyGuest is the shared read-only guest identity custody —
	// written by ACL.CreateGuestKey (owner only).
	IssuedKeyGuest = "guest"
)

// FieldPushKeys subfield names — the wire shape of the `push` object.
// Values are encoded exactly like anytype-heart's space-view details
// (spacePushNotificationKey / spacePushNotificationEncryptionKey) so
// receiver-side client code is portable: spaceKey is base64(std) of
// the protobuf-marshalled ed25519 private key, encKey base64(std) of
// the raw AES key, encKeyId hex(sha256(raw)) = pushapi.Message.KeyId.
const (
	PushKeySpaceKey = "spaceKey"
	PushKeyEncKey   = "encKey"
	PushKeyEncKeyId = "encKeyId"
)

// Status lattice values. `Deleted` is terminal — once a record's
// localStatus or remoteStatus reaches it, no further status edits land.
// Per docs/02-tech-space.md § "Key Decisions (continued)":
// "deleted spaces stay in the index with status=deleted, never
// physically removed."
const (
	StatusActive   = "active"
	StatusArchived = "archived"
	StatusDeleted  = "deleted"

	// OneToOneDeletedStatus is the SYNCED remoteStatus written when a 1-1
	// space is deleted. Unlike StatusDeleted it is NOT terminal and never
	// drives a coordinator SpaceDelete: a derived 1-1 is not removed from
	// the nodes, only offloaded on every device. It propagates the delete
	// account-wide (each device offloads its local copy) yet stays
	// re-creatable — a later OneToOne(peer) flips the row back to active.
	// Surfaced to callers as space.StatusDeleted.
	OneToOneDeletedStatus = "oneToOneDeleted"

	// GuestDeletedRemoteStatus is the SYNCED remoteStatus written when a
	// guest-mode (public-access) space is deleted. Like the 1-1 marker
	// it is NOT terminal and never drives a coordinator SpaceDelete: a
	// guest space is not owned on the network, only offloaded on every
	// device, and it stays re-addable — a later JoinGuest with a valid
	// token flips the row back to active. Surfaced as
	// space.StatusDeleted.
	GuestDeletedRemoteStatus = "guestDeleted"

	// InvitePendingRemoteStatus is the SYNCED remoteStatus on a regular
	// space another account added us to directly (ACL AddAccounts). We are
	// already a full ACL member; approval is a local materialization gate.
	// Synced — unlike the device-local 1-1 pending — because the synced
	// inbox cursor means only ONE of our devices processes the invite
	// message, so the row itself must carry pending to the others. NOT
	// terminal: AcceptInvite flips it to active, DeclineInvite to declined.
	InvitePendingRemoteStatus = "invitePending"
	// InviteDeclinedRemoteStatus is the SYNCED, sticky, NON-terminal
	// remoteStatus written when a direct-add invite is declined. Suppresses
	// the request on every device; a later AcceptInvite overrides it. No
	// ACL write happens on decline — the account stays an ACL member.
	InviteDeclinedRemoteStatus = "inviteDeclined"
)

// IsDeleted reports whether a row is in any deleted/offloaded state — the
// account-wide tombstone (StatusDeleted on either status field) or the
// 1-1 synced offload marker. Used by the boot eager-loader, the Subscribe
// translator, and the deletion reconciler to treat both uniformly.
func (r SpaceIndexRecord) IsDeleted() bool {
	return r.RemoteStatus == StatusDeleted || r.LocalStatus == StatusDeleted ||
		r.RemoteStatus == OneToOneDeletedStatus ||
		r.RemoteStatus == GuestDeletedRemoteStatus
}

// Sentinels — wrap crdt.ErrValidation in handler returns.
var (
	ErrTypeImmutable      = errors.New("techspace: `type` is pinned after first non-empty write")
	ErrStatusTerminal     = errors.New("techspace: status=deleted is terminal")
	ErrDeleteOpNotAllowed = errors.New("techspace: deletion is via remoteStatus=deleted, not a delete op")
	ErrDerivedImmutable   = errors.New("techspace: `derived` is pinned after first true write")
	ErrDerivedUndeletable = errors.New("techspace: derived rows refuse status=deleted")
)

// statusFields are the SYNCED fields whose terminal-Deleted rule is
// enforced. localStatus is device-local (handler-exclusive, never
// synced) so it's absent here — only remoteStatus carries the
// account-wide terminal delete.
var statusFields = map[string]struct{}{
	FieldRemoteStatus: {},
}

// SpaceIndexHandler validates ops on the tech-space space-index
// dataset. Per docs/02-tech-space.md § "Space Index" and § "Key
// Decisions (continued)":
//
//   - `type` is set-once: empty/absent means "unknown" (rows created
//     on join/track before the space's header is readable) and may be
//     filled exactly once — from the loaded space's on-wire header —
//     after which it is immutable;
//   - `localStatus` / `remoteStatus` cannot move OUT of "deleted"
//     (terminal — deleted spaces stay in the index);
//   - `derived` is set-once like `type`, and derived rows refuse
//     remoteStatus=deleted from any writer (see FieldDerived);
//   - delete ops are rejected wholesale; deletion is a status edit,
//     not a CRDT delete.
//
// Owner-only writes are guaranteed by the tech space's ACL at the
// any-sync layer; this handler is defence in depth on the apply side.
type SpaceIndexHandler struct{}

func (SpaceIndexHandler) Init(_ context.Context) error { return nil }

// BeforeCreate accepts any `type` value, including empty/absent —
// rows registered on join/track are created before the space's header
// is readable, so their type is unknown until the first load backfills
// it (set-once, enforced by BeforeModify). Strict allow-listing of
// type values is deferred until the canonical space-type enum is
// consolidated.
//
// It also stamps `createdAt` (added-to-account time) from the change's
// timestamp via sink.Derive — derived from the change envelope, so every
// device replaying the same create lands on the same value. (Convergence
// caveats in the FieldCreatedAt doc.)
func (SpaceIndexHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	if ctx != nil && ctx.Change != nil && sink != nil && ctx.Change.Timestamp > 0 {
		// Fresh arena per call — the derived Op holds it alive until
		// the apply loop drains the sink (see drainDerivedTo). The
		// envelope carries unix SECONDS; an instant is millis.
		a := &anyenc.Arena{}
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{FieldCreatedAt},
			Payload: a.NewDateTimeMillis(ctx.Change.Timestamp * 1000),
		})
	}
	return nil
}

// BeforeModify enforces the headRuleErr rule table on single-path ops.
//
// The set-once gate reads the LOCAL pre-op state, so two concurrent
// fills with different values would pin divergently per device. That
// is safe only because the sole writer (load's header backfill) writes
// a pure function of the immutable space header — identical on every
// device. Do not add a second `type` writer that isn't.
func (SpaceIndexHandler) BeforeModify(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if len(op.Path) == 0 {
		return rejectMultiField(op.Payload, ctx.Before)
	}
	return headRuleErr(op.Path[0], op.Payload, ctx.Before)
}

// headRuleErr is the per-field write rule shared by BeforeModify's
// single-path branch and rejectMultiField's walk — one rule table, so
// a pin added to one path can't be bypassed through the other:
//   - `type` / `derived` are set-once (see the Field docs);
//   - status fields never move out of "deleted" (terminal);
//   - a derived row refuses remoteStatus=deleted from any writer —
//     derived spaces are permanent (space.ErrIsDerivedSpace), and the
//     synced flag is only as strong as this apply-side gate.
func headRuleErr(head string, payload, before *anyenc.Value) error {
	if head == FieldType && currentType(before) != "" {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrTypeImmutable)
	}
	if head == FieldDerived && currentDerived(before) {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrDerivedImmutable)
	}
	if _, isStatus := statusFields[head]; isStatus {
		if currentStatus(before, head) == StatusDeleted {
			return fmt.Errorf("%w: %w (field %q)", crdt.ErrValidation, ErrStatusTerminal, head)
		}
		if payloadString(payload) == StatusDeleted && currentDerived(before) {
			return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrDerivedUndeletable)
		}
	}
	return nil
}

// payloadString reads a string payload; "" for nil / non-string
// (unset ops, object bundles).
func payloadString(payload *anyenc.Value) string {
	if payload == nil || payload.Type() != anyenc.TypeString {
		return ""
	}
	return string(payload.GetStringBytes())
}

// BeforeDelete rejects every delete attempt — there is no physical
// removal in the space index. Callers wanting to remove a space write
// `localStatus = deleted` instead.
func (SpaceIndexHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrDeleteOpNotAllowed)
}

// rejectMultiField walks the payload of a multi-field $set/$unset and
// returns the first key that violates the immutability or terminal-status
// rules. It reports per the WHOLE op (one error, first hit) — the crdt
// apply loop turns that into per-key salvage: on rejection it re-probes
// each key as a single-path op (which lands in BeforeModify's single-path
// branch below), so only the offending key is shed and the op's valid
// siblings still apply (crdt.recordModifier.beforeModifyApply). This
// keeps an old create that bundled `type` with name/status from vanishing
// when it replays through the modify path.
func rejectMultiField(payload, before *anyenc.Value) error {
	if payload == nil || payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := payload.Object()
	var hit error
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if hit != nil {
			return
		}
		head := string(k)
		if i := strings.IndexByte(head, '.'); i >= 0 {
			head = head[:i]
		}
		hit = headRuleErr(head, v, before)
	})
	return hit
}

// currentDerived reads `derived` off the pre-op record. False covers
// absent record, missing field, and an explicit false — all "not yet
// pinned" for the set-once rule.
func currentDerived(before *anyenc.Value) bool {
	return before != nil && before.GetBool(FieldDerived)
}

// currentType reads `type` off the pre-op record. Empty string covers
// absent record, missing field, and an explicit "" — all of which mean
// "unknown, still fillable" for the set-once rule.
func currentType(before *anyenc.Value) string {
	if before == nil {
		return ""
	}
	v := before.Get(FieldType)
	if v == nil || v.Type() != anyenc.TypeString {
		return ""
	}
	return string(v.GetStringBytes())
}

// currentStatus reads the named status field off the pre-op record.
// Returns the empty string if the record is absent or the field is
// missing/non-string — both of which mean "not yet deleted" for the
// terminal-status check.
func currentStatus(before *anyenc.Value, field string) string {
	if before == nil {
		return ""
	}
	v := before.Get(field)
	if v == nil || v.Type() != anyenc.TypeString {
		return ""
	}
	return string(v.GetStringBytes())
}

