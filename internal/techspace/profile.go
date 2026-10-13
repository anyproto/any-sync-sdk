package techspace

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// ProfileSchema declares the `profile` dataset fields — the account's
// own profile, synced across its devices.
func ProfileSchema() schema.Dataset {
	str := func() *schema.Schema { return schema.Leaf(schema.KindString) }
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldProfileName, Name: "Name", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Account display name, published to identityRepo.", XFormat: map[string]any{"type": "text"}},
		{Id: FieldProfileDescription, Name: "Description", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Account description.", XFormat: map[string]any{"type": "longtext"}},
		{Id: FieldProfileIcon, Name: "Icon", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Avatar content address: the rootCid of the avatar file."},
		{Id: FieldProfileIconFileId, Name: "Icon file", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Payloads row id of the avatar file in the tech space."},
		{Id: FieldProfileIconPublished, Name: "Icon published", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeSynced,
			Description: "Whether the avatar reference has reached identityRepo (set by the device that pushed it)."},
	}}
}

// Profile dataset / handler for the tech-space.
//
// One row per account, keyed by the constant ProfileSelfId. The dataset
// holds the local source-of-truth for the account's identityRepo
// profile: Name, Description, and the avatar as IconCID (its content
// address) plus IconFileId (the tech-space file row that holds the
// bytes and the file key). The SDK reads it on boot and republishes to
// identityRepo so a fresh-boot or freshly-restored device pushes the
// latest profile without an explicit user call.

const (
	// ProfileDataset name on the space-index tree. We piggyback on the
	// space-index tree to avoid a second derived tree just for one
	// row — the controller supports multiple datasets per tree, so the
	// extra cost is one handler registration.
	ProfileDataset = "profile"

	// ProfileSelfId is the only valid record id in the dataset. The
	// account's identityRepo profile is account-private (one writer);
	// no need for a per-id keyspace.
	ProfileSelfId = "self"

	// ProfileHandlerVersion stamped on every change. Bump when adding
	// new validated fields.
	ProfileHandlerVersion = "profileHandler-v2"

	// FieldProfileName / FieldProfileDescription / FieldProfileIcon
	// match the on-the-wire layout EncodeProfile produces;
	// FieldProfileIconFileId stays local to the account's devices.
	FieldProfileName          = "name"
	FieldProfileDescription   = "description"
	FieldProfileIcon          = "iconCID"
	FieldProfileIconFileId    = "iconFileId"
	FieldProfileIconPublished = "iconPublished"
)

// ProfileRecord is the typed view of the single profile row.
type ProfileRecord struct {
	Name        string
	Description string
	// IconCID is the avatar's content address; empty without an avatar.
	IconCID string
	// IconFileId is the avatar's file row in the tech space, the source
	// of its rootCid, file key and durability.
	IconFileId string
	// IconPublished records that a device of the account pushed the
	// profile with the avatar reference: the file is durable, so every
	// device may publish it even before the receipt syncs to it.
	IconPublished bool
}

// IsEmpty reports whether the record has nothing worth pushing.
func (r ProfileRecord) IsEmpty() bool {
	return r.Name == "" && r.Description == "" && r.IconCID == ""
}

// DecodeProfileRecord pulls fields off an anyenc value as returned by
// Controller.Get. Returns the zero value when v is nil.
func DecodeProfileRecord(v *anyenc.Value) ProfileRecord {
	if v == nil {
		return ProfileRecord{}
	}
	return ProfileRecord{
		Name:          v.GetString(FieldProfileName),
		Description:   v.GetString(FieldProfileDescription),
		IconCID:       v.GetString(FieldProfileIcon),
		IconFileId:    v.GetString(FieldProfileIconFileId),
		IconPublished: v.GetBool(FieldProfileIconPublished),
	}
}

// ProfileHandler validates ops on the profile dataset. The dataset
// holds exactly one row (id = ProfileSelfId); we reject deletes and
// any non-self id wholesale. Every other op passes — the schema is
// flat strings, no immutability rules, owner-only at the ACL layer.
type ProfileHandler struct{}

func (ProfileHandler) Init(_ context.Context) error { return nil }

func (ProfileHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id != ProfileSelfId {
		return crdt.ErrValidation
	}
	return nil
}

func (ProfileHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Op, _ *crdt.Sink) error {
	if rec.Id != ProfileSelfId {
		return crdt.ErrValidation
	}
	return nil
}

func (ProfileHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return crdt.ErrValidation
}
