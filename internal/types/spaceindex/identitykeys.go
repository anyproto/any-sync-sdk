// The identity keys exchange — one row per participant of a one-to-one
// space, living as a dataset on the in-space spaceIndex object. Each
// participant publishes its own identity metadata symkey under its own
// account identity; the other side reads it and can then decrypt the
// participant's identityRepo profile. A 1-1 ACL is immutable and carries
// no per-writer metadata, so this dataset is the in-space channel that
// replaces the join-record metadata a regular space uses. See
// docs/13-one-to-one-spaces.md § Key exchange inside the space.

package spaceindex

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

const (
	// IdentityKeysDataset is the dataset name on the spaceIndex object.
	// Registered on every controller (uniform handler set); only the
	// spaceIndex object of a one-to-one space carries rows by
	// convention.
	IdentityKeysDataset = "identityKeys"

	// IdentityKeysHandlerVersion is stamped on every identityKeys change.
	IdentityKeysHandlerVersion = "identityKeys-v1"

	// FieldIdentityKeySymKey is the participant's identity metadata
	// symkey in the string form of space.MarshalSymKey — the same
	// encoding the ACL join-record metadata and the inbox invite carry.
	// Row id is the participant's account identity. SYNCED.
	FieldIdentityKeySymKey = "symKey"
)

// IdentityKeysSchema declares the identityKeys dataset.
func IdentityKeysSchema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldIdentityKeySymKey, Name: "Metadata symkey", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced,
			Description: "The participant's identity metadata symkey; decrypts their identityRepo profile."},
	}}
}

// IdentityKeysHandler validates ops on the identityKeys dataset.
// Deterministic — every replica evaluates the same rules on the same
// causal prefix, so the authorization holds against every writer, not
// just the typed API.
type IdentityKeysHandler struct{}

func (IdentityKeysHandler) Init(_ context.Context) error { return nil }

// BeforeCreate runs the full per-op rules over the whole change: the
// apply pipeline runs ONLY BeforeCreate on a record's first change (no
// per-op BeforeModify). An invalid op drops the whole RecordChange.
func (IdentityKeysHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if err := checkIdentityKeyOwner(ctx, rec); err != nil {
		return err
	}
	for i := range rec.Ops {
		if err := validateIdentityKeyOp(&rec.Ops[i]); err != nil {
			return err
		}
	}
	return nil
}

func (IdentityKeysHandler) BeforeModify(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if err := checkIdentityKeyOwner(ctx, rec); err != nil {
		return err
	}
	return validateIdentityKeyOp(op)
}

// BeforeDelete rejects all record deletes: the row id is the
// participant's identity and the tombstone is sticky — a delete would
// ban that participant's key in this space forever.
func (IdentityKeysHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: identity key records are permanent", crdt.ErrValidation)
}

// checkIdentityKeyOwner is the authorization rule: a participant
// writes only the row keyed by its own identity. The creator is the
// signer of the change (stamped by the apply pipeline from the
// any-sync envelope), so the rule cannot be spoofed by a payload. A
// change with no creator is refused — the rule must hold for it to be
// applied at all.
func checkIdentityKeyOwner(ctx *crdt.ChangeCtx, rec *crdt.RecordChange) error {
	if rec.Id == "" {
		return fmt.Errorf("%w: empty identity", crdt.ErrValidation)
	}
	if ctx == nil || ctx.Change == nil || ctx.Change.Creator == "" {
		return fmt.Errorf("%w: identity key change has no creator", crdt.ErrValidation)
	}
	if ctx.Change.Creator != rec.Id {
		return fmt.Errorf("%w: identity key row %q is writable by that identity only", crdt.ErrValidation, rec.Id)
	}
	return nil
}

// validateIdentityKeyOp is the single per-op rule set, shared by the
// create and modify hooks: one field, $set to a non-empty string.
func validateIdentityKeyOp(op *crdt.Op) error {
	if len(op.Path) != 1 || op.Path[0] != FieldIdentityKeySymKey {
		return fmt.Errorf("%w: identityKeys ops must target %q", crdt.ErrValidation, FieldIdentityKeySymKey)
	}
	if op.Type != crdt.OpSet || !nonEmptyStringPayload(op.Payload) {
		return fmt.Errorf("%w: symKey must be $set to a non-empty string", crdt.ErrValidation)
	}
	return nil
}

// IdentityKeyOf reads the symkey off an identityKeys row; "" for a nil,
// tombstoned or malformed row.
func IdentityKeyOf(row *anyenc.Value) string {
	if row == nil || row.Get(crdt.DeletedAtField) != nil {
		return ""
	}
	return row.GetString(FieldIdentityKeySymKey)
}
