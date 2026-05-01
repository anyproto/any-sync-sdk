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

	Name        string
	IconCID     string
	Description string

	// LocalStatus / RemoteStatus carry the StatusActive / StatusArchived
	// / StatusDeleted vocabulary. Once either reaches StatusDeleted the
	// handler refuses moves out of it.
	LocalStatus  string
	RemoteStatus string
}

// DecodeSpaceIndexRecord pulls the fields off an anyenc value as
// returned by Controller.Get / iterator.Doc().Value(). Returns the
// zero value for missing fields. The id is read from the reserved
// crdt.IdField — the CRDT layer always stamps it on UpsertId.
func DecodeSpaceIndexRecord(v *anyenc.Value) SpaceIndexRecord {
	if v == nil {
		return SpaceIndexRecord{}
	}
	return SpaceIndexRecord{
		Id:           v.GetString("id"),
		Type:         v.GetString(FieldType),
		Name:         v.GetString(FieldName),
		IconCID:      v.GetString(FieldIcon),
		LocalStatus:  v.GetString(FieldLocalStatus),
		RemoteStatus: v.GetString(FieldRemoteStatus),
	}
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
	if r.Name != "" {
		obj.Set(FieldName, a.NewString(r.Name))
	}
	if r.IconCID != "" {
		obj.Set(FieldIcon, a.NewString(r.IconCID))
	}
	if r.LocalStatus != "" {
		obj.Set(FieldLocalStatus, a.NewString(r.LocalStatus))
	}
	if r.RemoteStatus != "" {
		obj.Set(FieldRemoteStatus, a.NewString(r.RemoteStatus))
	}
	return obj
}
