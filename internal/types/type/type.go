// Package typetype is the built-in `type` meta-type — the shape of
// type objects themselves. Ships the description of the meta-type and
// the handler that validates writes to a type object's `properties`
// dataset (the property-definition records).
//
// Directory is internal/types/type/; the package is declared
// `typetype` because `type` is a Go keyword and can't be a package
// name.
//
// `type` ships as a derived object in every space (well-known id).
package typetype

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// WellKnownDeriveSeed mints the same `type` type object id on every
// peer.
const WellKnownDeriveSeed = "builtin:type"

// Display metadata for the `type` meta-type object.
const (
	Name        = "Type"
	Description = "A type — defines properties (and optionally datasets) for the objects that implement it"
)

// DatasetPropertyDefs is the dataset on a type object that holds its
// property-*definition* records (id, name, kind, ...). On disk the
// collection is named "properties"; the Go identifier was chosen to
// disambiguate from property *values*, which live in a separate
// dataset called "objects" (properties.Dataset, handled by
// properties.SystemPropertiesHandler). Every object registers both
// handlers — only type objects actually write to this one.
const DatasetPropertyDefs = "properties"

// HandlerVersion is the DataVersion string stamped onto every change
// this handler emits against the `properties` dataset of a type
// object. Per docs/types-properties-proposal.md § "Change-level
// DataVersion", this dataset uses a hardcoded handler-chosen
// identifier (not a shortId) — bump the suffix if the validation
// rules ever change in a way that must reject stale writers.
const HandlerVersion = "typePropertyHandler-v1"

// Property-record field names. The shape is hardcoded in Go (no JSON
// Schema applies on this dataset — see
// docs/types-properties-proposal.md § "Schema format — decision").
const (
	FieldKey         = "key"         // user-facing stable identifier (e.g. "actors")
	FieldKind        = "kind"        // "string"/"number"/"boolean"/"null"/"array"/"object"
	FieldName        = "name"        // human label, mutable
	FieldDescription = "description" // mutable
	FieldXKey        = "x-key"       // caller-side mapping key, mutable
	FieldItems       = "items"       // recursive sub-shape for arrays
	FieldProperties  = "properties"  // recursive sub-shape for objects
	FieldRequired    = "required"    // []string, mutable per docs/06
	FieldMeta        = "meta"        // opaque consumer flag map (string→string), mutable
)

// schemaBearingFields are pinned for the life of the property record.
// Edits to any of these on an existing record drop the offending op.
// Per docs/types-properties-proposal.md § "Schema evolution rules":
// "Modify an existing property's schema-bearing fields (`kind`,
// `items`, `properties`) — rejected at write time. Kinds are pinned
// for life." `key` is included because storing-by-propId means the
// key identifies the property's user-side identity; renaming would
// silently break callers indexing by it (re-add for that case).
var schemaBearingFields = map[string]struct{}{
	FieldKey:        {},
	FieldKind:       {},
	FieldItems:      {},
	FieldProperties: {},
}

// ErrMissingKind indicates a property record was created without a
// `kind` field. Wraps crdt.ErrValidation so callers can match either.
var ErrMissingKind = errors.New("typetype: property record requires `kind`")

// PropertyHandler validates ops on a type object's `properties`
// dataset and projects shortId rows into the sibling shortIds dataset
// on every "important" change (add / remove). Per
// docs/types-properties-proposal.md § "Schema evolution rules" and
// § "ShortId — derivation".
type PropertyHandler struct{}

func (PropertyHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates a creation: the record must declare a known
// `kind`. Mints a shortId from the change's ChangeId and projects a
// row into ShortIdsDataset. The apply loop stamps that row's `_ver`
// with this change's VersionId — same versionId as the property
// record itself, so the gate is consistent.
func (PropertyHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	kindLabel, ok := extractKind(rec.Ops)
	if !ok {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrMissingKind)
	}
	if _, ok := schema.ParseKind(kindLabel); !ok {
		return fmt.Errorf("%w: unsupported kind %q", crdt.ErrValidation, kindLabel)
	}
	sink.Project(ShortIdsDataset, shortIdRow(ctx.Change.ChangeId, rec.Id, kindLabel))
	return nil
}

// BeforeModify rejects edits to schema-bearing fields on an existing
// record (`key`, `kind`, `items`, `properties`). Display-only edits
// (`name`, `description`, `x-key`, `required`) pass through.
// Not an "important" change — no shortId minting.
func (PropertyHandler) BeforeModify(_ *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if len(op.Path) == 0 {
		// Multi-field $set/$unset — guard each top-level key.
		return rejectIfMultiFieldTouchesSchema(op.Payload)
	}
	if _, locked := schemaBearingFields[op.Path[0]]; locked {
		return fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, op.Path[0])
	}
	return nil
}

// BeforeDelete classifies the removal as an "important" change and
// projects a shortId row marking it. Existing record data on
// per-object stores isn't cleaned up — subsequent writes touching
// the removed property drop op-by-op via the unknown-property rule.
func (PropertyHandler) BeforeDelete(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	sink.Project(ShortIdsDataset, removalShortIdRow(ctx.Change.ChangeId, rec.Id))
	return nil
}

// extractKind walks rec.Ops looking for a creation-shape op that
// carries the `kind` field. Two valid shapes:
//
//   - Multi-field $set with empty Path and `kind` as a top-level key
//     in the payload object.
//   - Single-field $set with Path = ["kind"] and a string payload.
//
// Returns the on-wire label (not parsed) and true if found.
func extractKind(ops []crdt.Op) (string, bool) {
	for i := range ops {
		op := &ops[i]
		if op.Type != crdt.OpSet {
			continue
		}
		if len(op.Path) == 0 {
			if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
				continue
			}
			v := op.Payload.Get(FieldKind)
			if v != nil && v.Type() == anyenc.TypeString {
				return string(v.GetStringBytes()), true
			}
			continue
		}
		if len(op.Path) == 1 && op.Path[0] == FieldKind {
			if op.Payload != nil && op.Payload.Type() == anyenc.TypeString {
				return string(op.Payload.GetStringBytes()), true
			}
		}
	}
	return "", false
}

// rejectIfMultiFieldTouchesSchema scans a multi-field $set/$unset
// payload for keys in schemaBearingFields. Returns an error wrapping
// crdt.ErrValidation on the first hit; nil otherwise.
//
// Dotted-path keys (e.g. "items.kind") count as touching the
// top-level field that owns the schema-bearing prefix.
func rejectIfMultiFieldTouchesSchema(payload *anyenc.Value) error {
	if payload == nil || payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := payload.Object()
	var hit string
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		if hit != "" {
			return
		}
		key := string(k)
		// Top-level field is everything before the first '.'.
		head := key
		if i := strings.IndexByte(head, '.'); i >= 0 {
			head = head[:i]
		}
		if _, locked := schemaBearingFields[head]; locked {
			hit = head
		}
	})
	if hit != "" {
		return fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, hit)
	}
	return nil
}
