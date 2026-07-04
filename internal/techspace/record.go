package techspace

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// SpaceIndexRecord is the typed view of one row in the space-index
// dataset. Mirrors docs/02-tech-space.md § "Space Index" + the field
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
		// Float64 read — GetInt narrows through `int` and would truncate
		// on 32-bit platforms; anyenc numbers are float64 on the wire.
		CreatedAt: int64(v.GetFloat64(FieldCreatedAt)),
	}
	if arr := v.GetArray(FieldInviteNotifyPending); len(arr) > 0 {
		r.InviteNotifyPending = make([]string, 0, len(arr))
		for _, e := range arr {
			if s := e.GetStringBytes(); len(s) > 0 {
				r.InviteNotifyPending = append(r.InviteNotifyPending, string(s))
			}
		}
	}
	return r
}

// EncodeCreate packs a fresh SpaceIndexRecord into the multi-field
// $set payload that SpaceIndexHandler.BeforeCreate expects. Caller
// supplies the arena so the resulting *anyenc.Value can be embedded
// in a larger Change payload without an extra copy.
//
// Empty-string fields are omitted from the payload — handler rules
// only require Type to be present.
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
	// CreatedAt is intentionally NOT written here — it's handler-derived
	// (ScopeDerived; BeforeCreate stamps it from the change timestamp)
	// and an input op writing it would be rejected by the controller.
	return obj
}
