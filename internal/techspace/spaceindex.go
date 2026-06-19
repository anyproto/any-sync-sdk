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
		{Id: FieldCreatedAt, Name: "Created at", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeDerived},
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
	// FieldCreatedAt is the added-to-account time: unix seconds, stamped
	// by BeforeCreate from the creating change's timestamp when the row
	// first lands locally (Create / Derive / OneToOne / Join all create
	// the row once). ScopeDerived — handler-only, no input op may write
	// it, so it's immutable for life.
	//
	// Caveats (accepted — the value is advisory ordering metadata):
	//   - stamping is per-device first-touch: a device whose store
	//     materialized the row under an older handler reads 0 forever
	//     (no backfill), while a device replaying the same DAG with this
	//     handler stamps the real value;
	//   - two devices independently creating the same row (e.g. both
	//     Derive/Join before tech-space sync converges) each keep their
	//     own change's timestamp — typically seconds apart.
	// Callers treat 0 as "unknown".
	FieldCreatedAt = "createdAt"
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
	ErrMissingType        = errors.New("techspace: space-index record requires `type`")
	ErrTypeImmutable      = errors.New("techspace: `type` is pinned after first write")
	ErrStatusTerminal     = errors.New("techspace: status=deleted is terminal")
	ErrDeleteOpNotAllowed = errors.New("techspace: deletion is via remoteStatus=deleted, not a delete op")
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

func (SpaceIndexHandler) Init(_ context.Context) error { return nil }

// BeforeCreate requires the `type` field to be present and a non-empty
// string. Strict allow-listing of type values is deferred until the
// canonical space-type enum is consolidated; for now any non-empty
// label passes.
//
// It also stamps `createdAt` (added-to-account time) from the change's
// timestamp via sink.Derive — derived from the change envelope, so every
// device replaying the same create lands on the same value. (Convergence
// caveats in the FieldCreatedAt doc.)
func (SpaceIndexHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	t, ok := extractStringField(rec.Ops, FieldType)
	if !ok || t == "" {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrMissingType)
	}
	if ctx != nil && ctx.Change != nil && sink != nil && ctx.Change.Timestamp > 0 {
		// Fresh arena per call — the derived Op holds it alive until
		// the apply loop drains the sink (see drainDerivedTo).
		// Float64 constructor: anyenc numbers are float64 on the wire,
		// and NewNumberInt would truncate int64 on 32-bit platforms.
		a := &anyenc.Arena{}
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{FieldCreatedAt},
			Payload: a.NewNumberFloat64(float64(ctx.Change.Timestamp)),
		})
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
