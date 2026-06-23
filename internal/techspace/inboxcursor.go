package techspace

import (
	"context"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Inbox-cursor dataset for the tech-space.
//
// One row per account (id = InboxCursorSelfId) holding the SYNCED
// coordinator-inbox read position — the ObjectID-hex offset of the last
// inbox message the ACCOUNT has processed. Account-scoped on purpose: the
// coordinator inbox is per-receiver-identity and its messages are
// immutable + ObjectID-ordered, so the furthest-processed offset is a
// single shared high-water-mark, not per-device state. A fresh device
// seeds from it instead of replaying the whole inbox; everything below it
// is already represented by synced 1-1 rows (the correctness truth).
//
// Re-processing is idempotent (the 1-1 row is the handled-marker), so a
// momentary cross-device regression of this value just causes a harmless,
// deduplicated re-fetch — never loss. (See docs/13 § "Heart bugs we fix"
// #3: idempotent receive is what makes the synced cursor safe.)

const (
	// InboxCursorDataset name on the space-index tree. Piggybacks on the
	// space-index tree like ProfileDataset — one extra handler reg.
	InboxCursorDataset = "inboxCursor"

	// InboxCursorSelfId is the only valid record id (account-private, one
	// writer per field at the ACL layer).
	InboxCursorSelfId = "self"

	// InboxCursorHandlerVersion stamped on every change.
	InboxCursorHandlerVersion = "inboxCursorHandler-v1"

	// FieldInboxCursorOffset holds the ObjectID-hex offset.
	FieldInboxCursorOffset = "offset"
)

// InboxCursorSchema declares the single synced offset field.
func InboxCursorSchema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldInboxCursorOffset, Name: "Offset", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
	}}
}

// InboxCursorHandler validates ops on the inbox-cursor dataset: exactly
// one row (id = InboxCursorSelfId), no deletes. Mirrors ProfileHandler.
type InboxCursorHandler struct{}

func (InboxCursorHandler) Init(_ context.Context) error { return nil }

func (InboxCursorHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id != InboxCursorSelfId {
		return crdt.ErrValidation
	}
	return nil
}

func (InboxCursorHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Op, _ *crdt.Sink) error {
	if rec.Id != InboxCursorSelfId {
		return crdt.ErrValidation
	}
	return nil
}

func (InboxCursorHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return crdt.ErrValidation
}

// inboxCursorOffset reads the offset off an anyenc value, "" when absent.
func inboxCursorOffset(v *anyenc.Value) string {
	if v == nil {
		return ""
	}
	return v.GetString(FieldInboxCursorOffset)
}
