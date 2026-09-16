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

// MetaTypeMarker is the reserved value every type object carries in
// `any.type` — what distinguishes a type object from a regular one
// (see spaceobjects.LiveTypeRowsFilter). A definition object has no
// type of its own: the slot holds the marker.
//
// TypeId is the meta-type's id: the namespace its type-only property
// values live under (`record.type.xkey`) and the id surfaced through
// Space.Types(). The two differ because a `_`-prefixed top-level field
// is protocol-owned (crdt.validatePath), so the marker cannot double
// as a storage namespace.
//
// Both are reserved — user-derived type ids are content-addressable
// and never produce either string.
const (
	MetaTypeMarker = "__type__"
	TypeId         = "type"
)

// Display metadata for the `type` meta-type object.
const (
	Name        = "Type"
	Description = "A type — defines the properties, parts and layout of the objects that have it"
)

// FieldXKeyProp is the property id of the meta-type's `xkey` — the
// caller-side programmatic handle for the type itself, stored at
// `record.type.xkey`. Distinct from FieldXKey (`x-key`), which is the
// same idea one level down, on a property DEFINITION record.
const FieldXKeyProp = "xkey"

// BuiltInProperty is one hardcoded property definition. Same shape as
// anytype.BuiltInProperty so the types registry surfaces built-ins
// uniformly with user-defined types.
type BuiltInProperty struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope schema.Scope
	// Description and XFormat are the descriptive slice: a display
	// description and the opaque descriptor bag (docs/06 § The
	// `x-format` descriptor). Surfaced by Types().Properties and
	// dataset discovery like a user definition's; never interpreted
	// or enforced by the SDK.
	Description string
	XFormat     map[string]any
}

// Properties lists the meta-type's hardcoded property definitions —
// the values that are meaningful only on a type object. They live in
// the `type` namespace rather than `any` so the membership check in
// properties.SystemPropertiesHandler fences them off: a row that
// doesn't carry the marker cannot hold them at all.
//
// The `objects` DataVersion is deliberately NOT bumped for the move
// off `any.xkey`: a peer that doesn't know a version parks every
// change for that dataset, so bumping would halt all property sync
// with older peers to protect one field. Mixed versions instead drop
// the unknown-namespace op per-op — the type's other metadata still
// applies, and its xkey resolves on upgraded peers only.
var Properties = []BuiltInProperty{
	{Id: FieldXKeyProp, Name: "XKey", Kind: schema.KindString, Scope: schema.ScopeSynced,
		Description: "Programmatic handle of the type; consumers keep it unique per space."},
	{Id: FieldLayoutProp, Name: "Layout", Kind: schema.KindObject, Scope: schema.ScopeSynced,
		Description: "Layout descriptor of the primary type: {type, config}."},
	{Id: FieldHiddenProp, Name: "Hidden", Kind: schema.KindBoolean, Scope: schema.ScopeSynced,
		Description: "Keeps the type out of default listings and pickers.",
		XFormat:     map[string]any{"type": "checkbox"}},
	{Id: FieldMetaProp, Name: "Meta", Kind: schema.KindObject, Scope: schema.ScopeSynced,
		Description: "Consumer flags, one scalar per key."},
}

// Rendering metadata a type object carries in its own namespace:
// `type.layout` is the type's layout descriptor ({type, config},
// written whole, opaque to the SDK).
const FieldLayoutProp = "layout"

// `type.hidden` keeps a type out of default listings and pickers
// (self-typed bundle roots carry it); `type.meta` is the open bag of
// consumer flags — one scalar per key, written per key so writers
// touching different keys merge (`meta.index`, a client's own tags).
const (
	FieldHiddenProp = "hidden"
	FieldMetaProp   = "meta"
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

// PropertyHandlerLocalVersion is this handler's LOCAL logic version
// (HandlerReg.Version) — bumped when a validation change means an
// already-materialized set of definitions would come out different, so
// the SDK replays the type object's tree (docs/08-versioning.md).
//
// It is also how a peer recovers definitions its previous build dropped:
// the wire DataVersion deliberately stays put (bumping it would park
// every property change on peers that don't know the new version — see
// the Properties note above), so an older build rejects a definition it
// cannot validate, and the replay after the upgrade applies it.
//
// v2: `datetime` is a kind, and the date formats accept it.
// v3: the typed `format` object is gone (no format→kind coupling, no
// pinned `format.type`); `x-format` must be an object, created whole.
const PropertyHandlerLocalVersion = 3

// Property-record field names. The shape is hardcoded in Go (no JSON
// Schema applies on this dataset — see
// docs/types-properties-proposal.md § "Schema format — decision").
const (
	FieldKey         = "key"         // user-facing stable identifier (e.g. "actors")
	FieldKind        = "kind"        // "string"/"number"/"boolean"/"null"/"array"/"object"/"datetime"
	FieldScope       = "scope"       // "synced"/"account"/"local" — write/sync class, pinned
	FieldName        = "name"        // human label, mutable
	FieldDescription = "description" // mutable
	FieldXKey        = "x-key"       // caller-side handle, mutable
	FieldItems       = "items"       // recursive sub-shape for arrays
	FieldProperties  = "properties"  // recursive sub-shape for objects
	FieldRequired    = "required"    // []string, mutable per docs/06
	FieldMeta        = "meta"        // opaque consumer flag map (string→string), mutable
	FieldXFormat     = "x-format"    // opaque descriptor object, mutable — see below
)

// `x-format` is the descriptor bag: everything descriptive about a
// property or a dataset field — semantic slug, icon, ordering key,
// option set, relation targets, per-format config. The SDK stores it
// opaquely. Two structural rules and nothing else: it is an object,
// and a creation writes it whole (dotted `x-format.*` keys at create
// are rejected, so there is exactly one creation shape to validate).
// Afterwards it is patched per path like any CRDT member — the
// vocabulary, the leaf-only patch rule and value validation belong to
// the consumer (the `any` server), never to a handler.

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
// state: the head segment is a schema-bearing field, so the whole
// subtree is pinned. Everything else — including every path under
// `x-format` — is mutable.
func isPinnedPath(path []string) bool {
	if len(path) == 0 {
		return false
	}
	_, locked := schemaBearingFields[path[0]]
	return locked
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

// ErrBadXFormat indicates a structurally malformed `x-format` in a
// creation change: not an object, or written through dotted
// `x-format.*` keys instead of as one whole object. Shared by property
// and dataset-field records. Wraps crdt.ErrValidation.
var ErrBadXFormat = errors.New("typetype: `x-format` must be an object created whole")

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
	if err := validateXFormatCreate(rec.Ops); err != nil {
		return err
	}
	sink.Project(ShortIdsDataset, shortIdRow(ctx.Change.ChangeId, rec.Id, kindLabel))
	return nil
}

// validateXFormatCreate checks the one structural rule on `x-format` in
// a creation change: when present it is an object written whole under
// the `x-format` key (the shape AddProperty / AddDataset emit) — dotted
// `x-format.*` keys and deep-path ops are rejected so there is exactly
// one creation shape. Nothing inside the object is inspected.
func validateXFormatCreate(ops []crdt.Op) error {
	_, err := extractObjectField(ops, FieldXFormat)
	return err
}

// extractObjectField walks creation ops looking for the object-valued
// `field`, mirroring extractField's two accepted shapes (multi-field
// $set payload key, or single-field $set with Path = [field]). Any
// other op shape touching the field — dotted `<field>.*` payload keys,
// a deeper path, a non-object value — is an error rather than a silent
// skip, so a creation can't smuggle a half-written object past
// validation. Returns nil, nil when the field is absent.
func extractObjectField(ops []crdt.Op, field string) (*anyenc.Value, error) {
	var found *anyenc.Value
	for i := range ops {
		op := &ops[i]
		if len(op.Path) > 0 {
			if op.Path[0] != field {
				continue
			}
			if op.Type != crdt.OpSet || len(op.Path) != 1 || op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
				return nil, fmt.Errorf("%w: %w: %s must be created as a whole object", crdt.ErrValidation, ErrBadXFormat, field)
			}
			found = op.Payload
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
			if key == field {
				if v.Type() != anyenc.TypeObject {
					shapeErr = fmt.Errorf("%w: %w: `%s` must be an object", crdt.ErrValidation, ErrBadXFormat, field)
					return
				}
				found = v
				return
			}
			if strings.HasPrefix(key, field+".") {
				shapeErr = fmt.Errorf("%w: %w: dotted key %q not allowed at create", crdt.ErrValidation, ErrBadXFormat, key)
			}
		})
		if shapeErr != nil {
			return nil, shapeErr
		}
	}
	return found, nil
}

// BeforeModify rejects edits to pinned state on an existing record:
// the schema-bearing fields (`key`, `kind`, `scope`, `items`,
// `properties`). Everything else — `name`, `description`, `x-key`,
// `required`, `meta.*` and every path under `x-format` — passes
// through with no value checks, bar one: a $set of the whole
// `x-format` must be an object (checkXFormatWholeSet), so the
// create-time shape holds for the record's life. Not an "important"
// change — no shortId minting — with one exception: a creation-shaped
// upsert landing on an existing record.
//
// That is a concurrent duplicate create — two devices declaring a
// bundle property under its deterministic id while apart. The change
// is a create on its author's replica (BeforeCreate projected its
// shortId row there) and a modify everywhere else. The author stamps
// its later data writes with the newest shortId it knows, which may
// be this one, so every replica must know it too or those writes park
// forever: the row is projected here as well. Idempotent — the same
// row id on every replica — and the record itself merges as any
// modify does (the pinned keys are shed, the rest is identical).
func (PropertyHandler) BeforeModify(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, op *crdt.Op, sink *crdt.Sink) error {
	if len(op.Path) == 0 {
		if rec.Upsert && op.Type == crdt.OpSet {
			if kindLabel, ok := extractKind([]crdt.Op{*op}); ok {
				sink.Project(ShortIdsDataset, shortIdRow(ctx.Change.ChangeId, rec.Id, kindLabel))
			}
		}
		// Multi-field $set/$unset — guard each key.
		return rejectIfMultiFieldTouchesPinned(op)
	}
	if isPinnedPath(op.Path) {
		return fmt.Errorf("%w: %q is pinned after first write", crdt.ErrValidation, strings.Join(op.Path, "."))
	}
	return checkXFormatWholeSet(op.Type, op.Path, op.Payload)
}

// checkXFormatWholeSet keeps `x-format` an object on every peer: a $set
// whose path is exactly `x-format` must carry an object. Paths below it
// are free, and $unset of the whole bag is a clear, not a shape change.
// Shared by the property and dataset-def handlers.
func checkXFormatWholeSet(opType crdt.OpType, path []string, payload *anyenc.Value) error {
	if opType != crdt.OpSet || len(path) != 1 || path[0] != FieldXFormat {
		return nil
	}
	if payload == nil || payload.Type() != anyenc.TypeObject {
		return fmt.Errorf("%w: %w: a whole `%s` set must carry an object", crdt.ErrValidation, ErrBadXFormat, FieldXFormat)
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
// payload for keys whose dotted path is pinned (isPinnedPath), and
// applies the whole-`x-format` object rule to a bare `x-format` key.
// Returns an error wrapping crdt.ErrValidation on the first hit; nil
// otherwise.
//
// Dotted-path keys (e.g. "items.kind") count as touching the pinned
// path that owns them.
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
		hit = checkXFormatWholeSet(op.Type, path, v)
	})
	return hit
}
