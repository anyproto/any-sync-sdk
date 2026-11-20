package techspace

import (
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/space"
)

// SpaceIndexRecord is the typed view of one row in the space-index
// dataset. Mirrors docs/tech-space.md § "Space Index" + the field
// constants in spaceindex.go (Field*).
//
// Decoded from the on-disk anyenc value via DecodeSpaceIndexRecord;
// produced for writes via NewSpaceIndexCreate / Encode helpers.
//
// Every field is optional on read because tech-space rules allow
// records that started small and grew. Type and at least one of the
// status fields are populated on a normal Create path.
type SpaceIndexRecord struct {
	// Id is the spaceId — primary key in the dataset.
	Id string

	// Type mirrors the space header's SpaceType — one of the
	// space.SpaceType* constants (anytype.space / anytype.chatspace
	// / anytype.onetoone). First-write-wins: pinned for life by
	// SpaceIndexHandler.BeforeModify.
	Type string

	// SpaceType is the app-level tag mirrored from the in-space
	// spaceIndex.spaceType (surfaced as space.SpaceInfo.SpaceType).
	// Independent of Type and not pinned — the watcher overwrites it
	// with the converged in-space value.
	SpaceType string

	Name        string
	IconCID     string
	Description string

	// LocalStatus / RemoteStatus carry the StatusActive / StatusArchived
	// / StatusDeleted vocabulary. Once either reaches StatusDeleted the
	// handler refuses moves out of it.
	LocalStatus  string
	RemoteStatus string

	// AclHeadId is the ACL head id from RequestJoin, recorded on a
	// joining row so the post-acceptance waiter can detect a decline.
	// Device-local (FieldAclHeadId, ScopeLocal); empty on non-joining
	// rows. Written via Service.SetAclHeadId after the row exists.
	AclHeadId string

	// OneToOnePeer is the other participant's account identity on a
	// derived 1-1 row (FieldOneToOnePeer, synced). Required to materialize
	// the 1-1 storage on accept. Empty on non-1-1 rows.
	OneToOnePeer string

	// OneToOneInviteState is the device-local send obligation marker
	// (FieldOneToOneInviteState, ScopeLocal): "toSend" while this device
	// still owes the peer an inbox notification, cleared once delivered.
	OneToOneInviteState string

	// InviteNotifyPending is the device-local direct-add send outbox
	// (FieldInviteNotifyPending, ScopeLocal): identities this device still
	// owes a RegularInvite inbox notification after AddAccounts. Entries
	// are cleared one-by-one on confirmed delivery.
	InviteNotifyPending []string

	// CreatedAt is the added-to-account time in unix seconds, stamped by
	// SpaceIndexHandler.BeforeCreate when the row first lands (see
	// FieldCreatedAt). Zero on rows created before the field existed —
	// treat 0 as "unknown".
	CreatedAt int64

	// Settings is the client-writable free-form settings object
	// (FieldSettings, synced account-wide), decoded to Go natives —
	// string / float64 / bool per JSON semantics. Nil when never
	// written. Edited per key via Service.SetSettings.
	Settings map[string]any

	// PushKeys is the device-local push-notification key material
	// (FieldPushKeys, ScopeLocal), mirrored from ACL state by the
	// per-space ACL mirror watcher. Nil until the mirror first runs.
	// Written via Service.SetPushKeys.
	PushKeys *space.PushKeys

	// OwnRole is this account's own ACL permission in the space
	// (FieldOwnRole, ScopeLocal), mirrored from ACL state by the same
	// per-space watcher as PushKeys. PermissionNone until the mirror
	// first runs — "unknown yet", not a verdict. Written via
	// Service.SetOwnRole.
	OwnRole space.Permission

	// GuestKey is the shared read-only guest identity's private key
	// (FieldGuestKey, synced) on a guest-mode row — the space was added
	// via a guest invite and loads signing as this identity. Empty on
	// all other rows; presence is the guest-mode discriminator.
	GuestKey string

	// Derived marks a row written by the account's own Spaces().Derive
	// (FieldDerived, synced, set-once). Gates the Delete refusal —
	// derived spaces are permanent. False on created / joined /
	// tracked / 1-1 rows.
	Derived bool

	// P2PAdvertise is the per-space p2p advertising switch
	// (FieldP2PAdvertise, synced): true (the default, absent field
	// included) publishes this account's devices into the space's global
	// p2p records.
	P2PAdvertise bool

	// IssuedInviteKeys is this account's custody of the invite private
	// keys it issued for the space, keyed by kind — IssuedKeyMember /
	// IssuedKeyGuest (FieldIssuedInviteKeys, synced). Nil / missing kind
	// means no custody: this account never minted that invite here, or
	// revoked it.
	IssuedInviteKeys map[string]string
}

// IssuedInviteKey returns the custody entry for one issued-key kind,
// "" when absent.
func (r SpaceIndexRecord) IssuedInviteKey(kind string) string {
	return r.IssuedInviteKeys[kind]
}

// DecodeSpaceIndexRecord pulls the fields off an anyenc value as
// returned by Controller.Get / iterator.Doc().Value(). Returns the
// zero value for missing fields. The id is read from the reserved
// crdt.IdField — the CRDT layer always stamps it on UpsertId.
func DecodeSpaceIndexRecord(v *anyenc.Value) SpaceIndexRecord {
	if v == nil {
		return SpaceIndexRecord{}
	}
	r := SpaceIndexRecord{
		Id:                  v.GetString("id"),
		Type:                v.GetString(FieldType),
		SpaceType:           v.GetString(FieldSpaceType),
		Name:                v.GetString(FieldName),
		Description:         v.GetString(FieldDescription),
		IconCID:             v.GetString(FieldIcon),
		LocalStatus:         v.GetString(FieldLocalStatus),
		RemoteStatus:        v.GetString(FieldRemoteStatus),
		AclHeadId:           v.GetString(FieldAclHeadId),
		OneToOnePeer:        v.GetString(FieldOneToOnePeer),
		OneToOneInviteState: v.GetString(FieldOneToOneInviteState),
		OwnRole:             space.ParsePermission(v.GetString(FieldOwnRole)),
		GuestKey:            v.GetString(FieldGuestKey),
		Derived:             v.GetBool(FieldDerived),
		P2PAdvertise:        v.Get(FieldP2PAdvertise) == nil || v.GetBool(FieldP2PAdvertise),
		// The stamp is a TypeDateTime instant; rows written before that
		// carried epoch seconds and read the old way until the re-index
		// reaches them (anyencx.StampSeconds handles both).
		CreatedAt: anyencx.StampSeconds(v.Get(FieldCreatedAt)),
	}
	if arr := v.GetArray(FieldInviteNotifyPending); len(arr) > 0 {
		r.InviteNotifyPending = make([]string, 0, len(arr))
		for _, e := range arr {
			if s := e.GetStringBytes(); len(s) > 0 {
				r.InviteNotifyPending = append(r.InviteNotifyPending, string(s))
			}
		}
	}
	if st := v.Get(FieldSettings); st != nil && st.Type() == anyenc.TypeObject {
		// GoType converts recursively to JSON-shaped natives
		// (map[string]any / []any / string / float64 / bool / nil).
		if m, ok := st.GoType().(map[string]any); ok {
			r.Settings = m
		}
	}
	if ik := v.Get(FieldIssuedInviteKeys); ik != nil && ik.Type() == anyenc.TypeObject {
		if obj, err := ik.Object(); err == nil {
			keys := make(map[string]string, obj.Len())
			obj.Visit(func(k []byte, val *anyenc.Value) {
				if s := val.GetStringBytes(); len(s) > 0 {
					keys[string(k)] = string(s)
				}
			})
			if len(keys) > 0 {
				r.IssuedInviteKeys = keys
			}
		}
	}
	if pk := v.Get(FieldPushKeys); pk != nil && pk.Type() == anyenc.TypeObject {
		keys := space.PushKeys{
			SpaceKey: pk.GetString(PushKeySpaceKey),
			EncKey:   pk.GetString(PushKeyEncKey),
			EncKeyId: pk.GetString(PushKeyEncKeyId),
		}
		// Tolerate partial writes from older/newer mirrors: surface the
		// object only when the decrypt-critical pair is present.
		if keys.EncKey != "" && keys.EncKeyId != "" {
			r.PushKeys = &keys
		}
	}
	return r
}

// EncodeCreate packs a fresh SpaceIndexRecord into the multi-field
// $set payload that SpaceIndexHandler.BeforeCreate expects. Caller
// supplies the arena so the resulting *anyenc.Value can be embedded
// in a larger Change payload without an extra copy.
//
// Empty-string fields are omitted from the payload; an omitted Type
// means "unknown", backfilled set-once from the space header after the
// first load.
func (r SpaceIndexRecord) EncodeCreate(a *anyenc.Arena) *anyenc.Value {
	obj := a.NewObject()
	if r.Type != "" {
		obj.Set(FieldType, a.NewString(r.Type))
	}
	if r.SpaceType != "" {
		obj.Set(FieldSpaceType, a.NewString(r.SpaceType))
	}
	if r.Name != "" {
		obj.Set(FieldName, a.NewString(r.Name))
	}
	if r.Description != "" {
		obj.Set(FieldDescription, a.NewString(r.Description))
	}
	if r.IconCID != "" {
		obj.Set(FieldIcon, a.NewString(r.IconCID))
	}
	// LocalStatus is intentionally NOT written here — it's a device-local
	// field (FieldLocalStatus is crdt.LocalFieldPrefix-prefixed) and a
	// synced create may not touch local paths. Set it via
	// Service.SetLocalStatus (Object.LocalSet) after the row exists.
	// Absence means active.
	if r.RemoteStatus != "" {
		obj.Set(FieldRemoteStatus, a.NewString(r.RemoteStatus))
	}
	if r.OneToOnePeer != "" {
		obj.Set(FieldOneToOnePeer, a.NewString(r.OneToOnePeer))
	}
	if r.GuestKey != "" {
		obj.Set(FieldGuestKey, a.NewString(r.GuestKey))
	}
	if r.Derived {
		obj.Set(FieldDerived, a.NewTrue())
	}
	// CreatedAt is intentionally NOT written here — it's handler-derived
	// (ScopeDerived; BeforeCreate stamps it from the change timestamp)
	// and an input op writing it would be rejected by the controller.
	return obj
}
