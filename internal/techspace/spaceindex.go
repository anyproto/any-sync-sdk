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
)

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
	FieldType         = "type"
	FieldName         = "name"
	FieldDescription  = "description"
	FieldIcon         = "icon"
	FieldLocalStatus  = "localStatus"
	FieldRemoteStatus = "remoteStatus"
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
)

// Sentinels — wrap crdt.ErrValidation in handler returns.
var (
	ErrMissingType         = errors.New("techspace: space-index record requires `type`")
	ErrTypeImmutable       = errors.New("techspace: `type` is pinned after first write")
	ErrStatusTerminal      = errors.New("techspace: status=deleted is terminal")
	ErrDeleteOpNotAllowed  = errors.New("techspace: deletion is via localStatus=deleted, not a delete op")
)

// statusFields are the fields whose terminal-Deleted rule is enforced.
var statusFields = map[string]struct{}{
	FieldLocalStatus:  {},
	FieldRemoteStatus: {},
}

// SpaceIndexHandler validates ops on the tech-space space-index
// dataset. Per docs/02-tech-space.md § "Space Index" and § "Key
// Decisions (continued)":
//
//   - `type` is first-write-wins (a space's type is immutable once
//     the index entry exists);
//   - `localStatus` / `remoteStatus` cannot move OUT of "deleted"
//     (terminal — deleted spaces stay in the index);
//   - delete ops are rejected wholesale; deletion is a status edit,
//     not a CRDT delete.
//
// Owner-only writes are guaranteed by the tech space's ACL at the
// any-sync layer; this handler is defence in depth on the apply side.
type SpaceIndexHandler struct{}

func (SpaceIndexHandler) Dataset() string              { return SpaceIndexDataset }
func (SpaceIndexHandler) Version() int                 { return 1 }
func (SpaceIndexHandler) Init(_ context.Context) error { return nil }

// BeforeCreate requires the `type` field to be present and a non-empty
// string. Strict allow-listing of type values is deferred until the
// canonical space-type enum is consolidated; for now any non-empty
// label passes.
func (SpaceIndexHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	t, ok := extractStringField(rec.Ops, FieldType)
	if !ok || t == "" {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrMissingType)
	}
	return nil
}

// BeforeModify enforces:
//   - `type` is pinned (any op touching it drops);
//   - status edits are rejected when the current status is "deleted".
func (SpaceIndexHandler) BeforeModify(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if len(op.Path) == 0 {
		return rejectMultiField(op.Payload, ctx.Before)
	}
	head := op.Path[0]
	if head == FieldType {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrTypeImmutable)
	}
	if _, isStatus := statusFields[head]; isStatus {
		if currentStatus(ctx.Before, head) == StatusDeleted {
			return fmt.Errorf("%w: %w (field %q)", crdt.ErrValidation, ErrStatusTerminal, head)
		}
	}
	return nil
}

// BeforeDelete rejects every delete attempt — there is no physical
// removal in the space index. Callers wanting to remove a space write
// `localStatus = deleted` instead.
func (SpaceIndexHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrDeleteOpNotAllowed)
}

// rejectMultiField walks the payload of a multi-field $set/$unset and
// drops the whole op if any key violates the immutability or terminal-
// status rules. We reject the whole op (not just the offending key)
// because anyenc payloads can't be edited in place mid-validate, and
// callers shouldn't bundle constrained keys with unconstrained ones.
func rejectMultiField(payload, before *anyenc.Value) error {
	if payload == nil || payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := payload.Object()
	var hit error
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		if hit != nil {
			return
		}
		key := string(k)
		head := key
		if i := strings.IndexByte(head, '.'); i >= 0 {
			head = head[:i]
		}
		if head == FieldType {
			hit = fmt.Errorf("%w: %w", crdt.ErrValidation, ErrTypeImmutable)
			return
		}
		if _, isStatus := statusFields[head]; isStatus {
			if currentStatus(before, head) == StatusDeleted {
				hit = fmt.Errorf("%w: %w (field %q)", crdt.ErrValidation, ErrStatusTerminal, head)
			}
		}
	})
	return hit
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

// extractStringField walks rec.Ops looking for a creation-shape op
// that sets `field` to a string. Two valid shapes (mirrors the
// PropertyHandler kind extractor):
//
//   - Multi-field $set with empty Path and `field` as a top-level
//     key in the payload object.
//   - Single-field $set with Path = [field] and a string payload.
func extractStringField(ops []crdt.Op, field string) (string, bool) {
	for i := range ops {
		op := &ops[i]
		if op.Type != crdt.OpSet {
			continue
		}
		if len(op.Path) == 0 {
			if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
				continue
			}
			v := op.Payload.Get(field)
			if v != nil && v.Type() == anyenc.TypeString {
				return string(v.GetStringBytes()), true
			}
			continue
		}
		if len(op.Path) == 1 && op.Path[0] == field {
			if op.Payload != nil && op.Payload.Type() == anyenc.TypeString {
				return string(op.Payload.GetStringBytes()), true
			}
		}
	}
	return "", false
}
