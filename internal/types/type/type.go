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
	FieldScope       = "scope"       // "synced"/"account"/"local" — write/sync class, pinned
	FieldName        = "name"        // human label, mutable
	FieldDescription = "description" // mutable
	FieldXKey        = "x-key"       // caller-side mapping key, mutable
	FieldXKind       = "x-kind"      // free-form classification hint, mutable
	FieldItems       = "items"       // recursive sub-shape for arrays
	FieldProperties  = "properties"  // recursive sub-shape for objects
	FieldRequired    = "required"    // []string, mutable per docs/06
	FieldMeta        = "meta"        // opaque consumer flag map (string→string), mutable
	FieldFormat      = "format"      // value-format object — see the FormatKey* sub-keys
)

// Sub-keys of the `format` object. `format.type` is pinned like `kind`
// (it constrains the kind); `format.ui` and `format.filter` are mutable
// string leaves. The filter is a mongo-style condition stored as its
// JSON text — a string leaf so concurrent edits replace each other as a
// unit instead of field-merging two conditions into garbage.
//
// `format.options` (select / multiselect enumerated choices) and
// `format.meta` (format-level config) are mutable nested objects. Their
// leaves are all string paths — format.options.<key>.{name,color,pos}
// and format.options.<key>.meta.<k> and format.meta.<k> — so they ride
// the same string-leaf CRDT rule as ui/filter (per-path $set/$unset,
// concurrent adds of distinct keys converge).
const (
	FormatKeyType    = "type"    // "links"/"date"/"datetime"/"tags"/"select"/"multiselect" — pinned
	FormatKeyUi      = "ui"      // presentation hint, opaque string, mutable
	FormatKeyFilter  = "filter"  // condition JSON text, opaque string, mutable
	FormatKeyOptions = "options" // select/multiselect option set (key → {name,color,pos,meta}), mutable
	FormatKeyMeta    = "meta"    // format-level config bag (string→string), mutable

	// Option leaf sub-keys under format.options.<key>.
	OptionKeyName  = "name"  // display label, mutable string
	OptionKeyColor = "color" // presentation color, mutable string
	OptionKeyPos   = "pos"   // lexid display-order key, mutable string
	OptionKeyMeta  = "meta"  // per-option opaque bag (string→string), mutable
)

// formatTypeKindLabel maps each known format-type label to the `kind`
// label it requires. The SDK checks only this structural coupling —
// format semantics (ui vocabulary, filter syntax, value shapes) are a
// consumer concern.
var formatTypeKindLabel = map[string]string{
	"links":       "array",
	"date":        "string",
	"datetime":    "string",
	"tags":        "array",
	"select":      "string",
	"multiselect": "array",
}

// schemaBearingFields are pinned for the life of the property record.
// Edits to any of these on an existing record drop the offending op.
// Per docs/types-properties-proposal.md § "Schema evolution rules":
// "Modify an existing property's schema-bearing fields (`kind`,
// `items`, `properties`) — rejected at write time. Kinds are pinned
// for life." `key` is included because storing-by-propId means the
// key identifies the property's user-side identity; renaming would
// silently break callers indexing by it (re-add for that case).
// `scope` is pinned for the same reason kinds are: a propId's write
// route and version domain must never change (changing scope = mint a
// new property; see docs/scoped-properties-proposal.md).
var schemaBearingFields = map[string]struct{}{
	FieldKey:        {},
	FieldKind:       {},
	FieldScope:      {},
	FieldItems:      {},
	FieldProperties: {},
}

// isPinnedPath reports whether a write targeting `path` touches pinned
// state. Two cases:
//
//   - The head segment is a schema-bearing field — the whole subtree is
//     pinned (the historical top-level rule).
//   - The path is `format` or descends into `format.type`. A broad
//     write to `format` itself is pinned because replacing the object
//     replaces `type`, and BeforeModify has no prior state to prove it
//     didn't change; mutations must target the `format.ui` /
//     `format.filter` leaves.
func isPinnedPath(path []string) bool {
	if len(path) == 0 {
		return false
	}
	if _, locked := schemaBearingFields[path[0]]; locked {
		return true
	}
	if path[0] == FieldFormat {
		return len(path) == 1 || path[1] == FormatKeyType
	}
	return false
}

// IsPinnedPath is the exported form of isPinnedPath, so the space layer
// (PatchProperty) can reject writes to pinned state client-side and
// fail the whole patch fast, rather than relying on the handler's
// per-op drop (which would partially apply a mixed patch).
func IsPinnedPath(path []string) bool { return isPinnedPath(path) }

// ErrMissingKind indicates a property record was created without a
// `kind` field. Wraps crdt.ErrValidation so callers can match either.
var ErrMissingKind = errors.New("typetype: property record requires `kind`")

// ErrBadScope indicates a property record declared an unknown scope
// label, or the reserved "derived" scope (SDK built-ins only). Wraps
// crdt.ErrValidation so callers can match either.
var ErrBadScope = errors.New("typetype: property `scope` must be one of synced/account/local")

// ErrBadFormatType indicates a property record declared an unknown
// `format.type` label. Wraps crdt.ErrValidation.
var ErrBadFormatType = errors.New("typetype: property `format.type` must be one of links/date/datetime/tags/select/multiselect")

// ErrBadFormatShape indicates a structurally malformed `format`: not an
// object at create, a missing/non-string `type`, a non-string `ui` /
// `filter`, dotted `format.*` keys in a creation change, or a non-$set/
// $unset op on a format leaf. Wraps crdt.ErrValidation.
var ErrBadFormatShape = errors.New("typetype: property `format` must be an object with string `type`/`ui`/`filter`")

// ErrFormatKindMismatch indicates the declared `format.type` requires a
// different `kind` (links/tags ⇒ array; date/datetime ⇒ string). Wraps
// crdt.ErrValidation.
var ErrFormatKindMismatch = errors.New("typetype: property `format.type` is incompatible with `kind`")

// PropertyHandler validates ops on a type object's `properties`
// dataset and projects shortId rows into the sibling shortIds dataset
// on every "important" change (add / remove). Per
// docs/types-properties-proposal.md § "Schema evolution rules" and
// § "ShortId — derivation".
type PropertyHandler struct{}

func (PropertyHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates a creation: the record must declare a known
// `kind`, and — when present — a creatable `scope` (synced / account /
// local; absent means synced; "derived" is reserved for SDK built-ins).
// Mints a shortId from the change's ChangeId and projects a row into
// ShortIdsDataset. The apply loop stamps that row's `_ver` with this
// change's VersionId — same versionId as the property record itself,
// so the gate is consistent.
func (PropertyHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	kindLabel, ok := extractKind(rec.Ops)
	if !ok {
		return fmt.Errorf("%w: %w", crdt.ErrValidation, ErrMissingKind)
	}
	if _, ok := schema.ParseKind(kindLabel); !ok {
		return fmt.Errorf("%w: unsupported kind %q", crdt.ErrValidation, kindLabel)
	}
	if scopeLabel, present := extractField(rec.Ops, FieldScope); present {
		sc, known := schema.ParseScope(scopeLabel)
		if !known || sc == schema.ScopeDerived {
			return fmt.Errorf("%w: %w (got %q)", crdt.ErrValidation, ErrBadScope, scopeLabel)
		}
	}
	if err := validateFormatCreate(rec.Ops, kindLabel); err != nil {
		return err
	}
	sink.Project(ShortIdsDataset, shortIdRow(ctx.Change.ChangeId, rec.Id, kindLabel))
	return nil
}

// validateFormatCreate checks the structure of a `format` declaration in
// a creation change. Format must arrive as a whole object under the
// `format` key (the canonical shape AddProperty emits) — dotted
// `format.*` keys and deep-path ops are rejected so there is exactly one
// creation shape to validate. Inside the object: `type` must be a known
// label whose required kind matches the record's `kind`; `ui` and
// `filter` must be strings when present. Note `tags` passes here — an
// inbound definition from a newer SDK stays valid — while the local
// AddProperty pre-flight still rejects it until the tag table lands.
//
// Semantics (ui vocabulary, filter syntax) are deliberately NOT checked
// — see the package doc on the structure/semantics split.
func validateFormatCreate(ops []crdt.Op, kindLabel string) error {
	format, err := extractFormatObject(ops)
	if err != nil {
		return err
	}
	if format == nil {
		return nil
	}
	typeVal := format.Get(FormatKeyType)
	if typeVal == nil || typeVal.Type() != anyenc.TypeString {
		return fmt.Errorf("%w: %w: missing or non-string `format.type`", crdt.ErrValidation, ErrBadFormatShape)
	}
	typeLabel := string(typeVal.GetStringBytes())
	requiredKind, known := formatTypeKindLabel[typeLabel]
	if !known {
		return fmt.Errorf("%w: %w (got %q)", crdt.ErrValidation, ErrBadFormatType, typeLabel)
	}
	if kindLabel != requiredKind {
		return fmt.Errorf("%w: %w: format %q requires kind %q, got %q",
			crdt.ErrValidation, ErrFormatKindMismatch, typeLabel, requiredKind, kindLabel)
	}
	for _, key := range []string{FormatKeyUi, FormatKeyFilter} {
		if v := format.Get(key); v != nil && v.Type() != anyenc.TypeString {
			return fmt.Errorf("%w: %w: `format.%s` must be a string", crdt.ErrValidation, ErrBadFormatShape, key)
		}
	}
	return nil
}

// extractFormatObject walks creation ops looking for the `format`
// object, mirroring extractField's two accepted shapes (multi-field
// $set payload key, or single-field $set with Path = ["format"]). Any
// other op shape touching format — dotted `format.*` payload keys, a
// deeper path, a non-object value — is an error rather than a silent
// skip, so a creation can't smuggle format state past validation.
func extractFormatObject(ops []crdt.Op) (*anyenc.Value, error) {
	var format *anyenc.Value
	for i := range ops {
		op := &ops[i]
		if len(op.Path) > 0 {
			if op.Path[0] != FieldFormat {
				continue
			}
			if op.Type != crdt.OpSet || len(op.Path) != 1 || op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
				return nil, fmt.Errorf("%w: %w: format must be created as a whole object", crdt.ErrValidation, ErrBadFormatShape)
			}
			format = op.Payload
			continue
		}
		if op.Type != crdt.OpSet || op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
			continue
		}
		obj, _ := op.Payload.Object()
		var shapeErr error
		obj.Visit(func(k []byte, v *anyenc.Value) {
			if shapeErr != nil {
				return
			}
			key := string(k)
			if key == FieldFormat {
				if v.Type() != anyenc.TypeObject {
					shapeErr = fmt.Errorf("%w: %w: `format` must be an object", crdt.ErrValidation, ErrBadFormatShape)
					return
				}
				format = v
				return
			}
			if strings.HasPrefix(key, FieldFormat+".") {
				shapeErr = fmt.Errorf("%w: %w: dotted key %q not allowed at create", crdt.ErrValidation, ErrBadFormatShape, key)
			}
		})
		if shapeErr != nil {
			return nil, shapeErr
		}
	}
	return format, nil
}

// BeforeModify rejects edits to pinned state on an existing record:
// the schema-bearing fields (`key`, `kind`, `scope`, `items`,
// `properties`), broad `format` replaces, and `format.type`. Display
// edits (`name`, `description`, `x-key`, `required`) and the mutable
// format leaves (`format.ui`, `format.filter` — string $set / $unset
// only) pass through. Not an "important" change — no shortId minting.
func (PropertyHandler) BeforeModify(_ *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if len(op.Path) == 0 {
		// Multi-field $set/$unset — guard each key.
		return rejectIfMultiFieldTouchesPinned(op)
	}
	if isPinnedPath(op.Path) {
		return fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, strings.Join(op.Path, "."))
	}
	if op.Path[0] == FieldFormat {
		return checkFormatLeafOp(op.Type, op.Path, op.Payload)
	}
	return nil
}

// checkFormatLeafOp validates a mutation of a non-pinned format leaf
// (`format.ui` / `format.filter`): only string $set and $unset are
// admitted, keeping the leaves scalar strings for all writers. The
// string contents stay opaque — semantics are a consumer concern.
func checkFormatLeafOp(opType crdt.OpType, path []string, payload *anyenc.Value) error {
	switch opType {
	case crdt.OpUnset:
		return nil
	case crdt.OpSet:
		if payload == nil || payload.Type() != anyenc.TypeString {
			return fmt.Errorf("%w: %w: %q must be a string", crdt.ErrValidation, ErrBadFormatShape, strings.Join(path, "."))
		}
		return nil
	default:
		return fmt.Errorf("%w: %w: op %s not allowed on %q", crdt.ErrValidation, ErrBadFormatShape, opType, strings.Join(path, "."))
	}
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
// carries the `kind` field. See extractField for the valid shapes.
func extractKind(ops []crdt.Op) (string, bool) {
	return extractField(ops, FieldKind)
}

// extractField walks ops looking for a creation-shape $set carrying a
// string value for `field`. Two valid shapes:
//
//   - Multi-field $set with empty Path and `field` as a top-level key
//     in the payload object.
//   - Single-field $set with Path = [field] and a string payload.
//
// Returns the on-wire label (not parsed) and true if found.
func extractField(ops []crdt.Op, field string) (string, bool) {
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

// rejectIfMultiFieldTouchesPinned scans a multi-field $set/$unset
// payload for keys whose dotted path is pinned (isPinnedPath), and —
// for $set — enforces the string shape on format leaves. Returns an
// error wrapping crdt.ErrValidation on the first hit; nil otherwise.
//
// Dotted-path keys (e.g. "items.kind", "format.type") count as
// touching the pinned path that owns them.
func rejectIfMultiFieldTouchesPinned(op *crdt.Op) error {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := op.Payload.Object()
	var hit error
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if hit != nil {
			return
		}
		path := strings.Split(string(k), ".")
		if isPinnedPath(path) {
			hit = fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, string(k))
			return
		}
		if path[0] == FieldFormat {
			hit = checkFormatLeafOp(op.Type, path, v)
		}
	})
	return hit
}
