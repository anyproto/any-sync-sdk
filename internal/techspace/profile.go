package techspace

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// Profile dataset / handler for the tech-space.
//
// One row per account, keyed by the constant ProfileSelfId. The dataset
// holds the local source-of-truth for the account's identityRepo
// profile (Name, Description, IconCID). The SDK reads it on boot and
// republishes to identityRepo so a fresh-boot or freshly-restored
// device pushes the latest profile without an explicit user call.

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
	ProfileHandlerVersion = "profileHandler-v1"

	// FieldProfileName / FieldProfileDescription / FieldProfileIcon
	// match the on-the-wire layout EncodeAccountMetadata produces.
	FieldProfileName        = "name"
	FieldProfileDescription = "description"
	FieldProfileIcon        = "iconCID"
)

// ProfileRecord is the typed view of the single profile row.
type ProfileRecord struct {
	Name        string
	Description string
	IconCID     string
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
		Name:        v.GetString(FieldProfileName),
		Description: v.GetString(FieldProfileDescription),
		IconCID:     v.GetString(FieldProfileIcon),
	}
}

// encodeUpsert packs the record into the multi-field $set payload the
// handler accepts on first-write or full-replace.
func (r ProfileRecord) encodeUpsert(a *anyenc.Arena) *anyenc.Value {
	obj := a.NewObject()
	obj.Set(FieldProfileName, a.NewString(r.Name))
	obj.Set(FieldProfileDescription, a.NewString(r.Description))
	obj.Set(FieldProfileIcon, a.NewString(r.IconCID))
	return obj
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
