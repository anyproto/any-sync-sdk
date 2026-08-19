// The bundles registry — one row per bundle installed into
// the space (a marketplace bundle or a hardcoded setup like bao),
// living as a dataset on the in-space spaceIndex object. Each row
// carries the winning root object id (an LWW register — concurrent
// installs converge to one deterministic winner) plus the add-only set
// of every root ever claimed, so conflicts stay visible for merge and
// loser cleanup. See docs/bundles.md.

package spaceindex

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

const (
	// BundlesDataset is the dataset name on the spaceIndex object.
	// Registered on every controller (uniform handler set) but only the
	// spaceIndex object carries rows by convention — the typed Bundles
	// API always targets it.
	BundlesDataset = "bundles"

	// BundlesHandlerVersion is stamped on every bundles change.
	BundlesHandlerVersion = "bundles-v1"

	// Row id is the stable bundle identifier (marketplace id or a
	// hardcoded slug like "bao/v1"). All fields SYNCED.

	// FieldBundleName is the bundle's display name.
	FieldBundleName = "name"
	// FieldBundleRootId is the winning root object id — a plain LWW
	// register. Reading it answers "is this bundle installed and where
	// is its root"; children are derived from it (DeriveObjectOpts.
	// ParentId), so the one id transitively names the whole setup.
	FieldBundleRootId = "rootId"
	// FieldBundleRoots is the add-only set of every root object id ever
	// claimed for this bundle. $addToSet ONLY — a $set would stamp
	// _ver[roots] and gate out concurrent adds (the losing device's
	// claim must always land, or its objects become invisible garbage).
	// Never shrunk: losers stay listed as the audit trail; their death
	// is recorded by tree deletion, not by mutating this array.
	FieldBundleRoots = "roots"
)

// BundlesSchema declares the bundles dataset.
func BundlesSchema() schema.Dataset {
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldBundleName, Name: "Name", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: FieldBundleRootId, Name: "Root object", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: FieldBundleRoots, Name: "Claimed roots", Schema: &schema.Schema{Kind: schema.KindArray, Items: schema.Leaf(schema.KindString)}, Scope: schema.ScopeSynced},
	}}
}

// BundlesHandler validates ops on the bundles dataset. Deterministic —
// every replica evaluates the same rules on the same causal prefix, so
// the gates cover every writer, not just the typed API.
type BundlesHandler struct{}

func (BundlesHandler) Init(_ context.Context) error { return nil }

// BeforeCreate runs the full per-op rules over the whole change: the
// apply pipeline runs ONLY BeforeCreate on a record's first change (no
// per-op BeforeModify), so every invariant must hold here too or it is
// unenforced exactly once per bundle id — and a first-change `$set
// roots` would stamp _ver[roots] and gate out legitimate claims
// forever. An invalid op drops the whole RecordChange (per-record
// hook), which is the right granularity for a malformed create.
func (BundlesHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id == "" {
		return fmt.Errorf("%w: empty bundle id", crdt.ErrValidation)
	}
	for i := range rec.Ops {
		if err := validateBundleOp(rec, &rec.Ops[i]); err != nil {
			return err
		}
	}
	return nil
}

func (BundlesHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if rec.Id == "" {
		return fmt.Errorf("%w: empty bundle id", crdt.ErrValidation)
	}
	return validateBundleOp(rec, op)
}

// validateBundleOp is the single per-op rule set, shared by the create
// and modify hooks.
func validateBundleOp(rec *crdt.RecordChange, op *crdt.Op) error {
	// Explicit single-field paths only. A root-level multi-field $set
	// (empty path) decomposes per-key at apply time and could smuggle a
	// `roots` replace past the per-field rules below.
	if len(op.Path) != 1 {
		return fmt.Errorf("%w: bundles ops must target one top-level field", crdt.ErrValidation)
	}
	switch op.Path[0] {
	case FieldBundleName:
		if op.Type != crdt.OpSet || !stringPayload(op.Payload) {
			return fmt.Errorf("%w: name must be $set to a string", crdt.ErrValidation)
		}
	case FieldBundleRootId:
		if op.Type != crdt.OpSet || !nonEmptyStringPayload(op.Payload) {
			return fmt.Errorf("%w: rootId must be $set to a non-empty string", crdt.ErrValidation)
		}
		// Claim invariant: a winner assertion must claim the same id in
		// `roots` within the same RecordChange, so roots ⊇ {rootId}
		// holds in every reachable state and losers are always
		// computable as roots − rootId.
		if !claimsRoot(rec, op.Payload) {
			return fmt.Errorf("%w: rootId $set without matching $addToSet on roots", crdt.ErrValidation)
		}
	case FieldBundleRoots:
		if op.Type != crdt.OpAddToSet || !nonEmptyStringPayload(op.Payload) {
			return fmt.Errorf("%w: roots is add-only ($addToSet of a non-empty string)", crdt.ErrValidation)
		}
	default:
		return fmt.Errorf("%w: unknown bundles field %q", crdt.ErrValidation, op.Path[0])
	}
	return nil
}

// BeforeDelete rejects all record deletes: the row id is the bundle's
// global identity and the tombstone is sticky — a delete would ban the
// bundle id in this space forever. Uninstall semantics, when needed,
// will be a field-level state, not a record delete.
func (BundlesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return fmt.Errorf("%w: bundle records are permanent", crdt.ErrValidation)
}

// claimsRoot reports whether rec carries an $addToSet on `roots` whose
// payload equals rootId (both known string-typed by the caller's
// per-op checks running on every op of the change).
func claimsRoot(rec *crdt.RecordChange, rootId *anyenc.Value) bool {
	want := rootId.GetStringBytes()
	for i := range rec.Ops {
		o := &rec.Ops[i]
		if o.Type != crdt.OpAddToSet || len(o.Path) != 1 || o.Path[0] != FieldBundleRoots {
			continue
		}
		if o.Payload != nil && o.Payload.Type() == anyenc.TypeString && string(o.Payload.GetStringBytes()) == string(want) {
			return true
		}
	}
	return false
}

func stringPayload(v *anyenc.Value) bool {
	return v != nil && v.Type() == anyenc.TypeString
}

func nonEmptyStringPayload(v *anyenc.Value) bool {
	return stringPayload(v) && len(v.GetStringBytes()) > 0
}
