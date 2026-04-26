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

import "github.com/anyproto/any-sync-sdk/internal/crdt"

// WellKnownDeriveSeed mints the same `type` type object id on every
// peer.
const WellKnownDeriveSeed = "builtin:type"

// Display metadata for the `type` meta-type object.
const (
	Name        = "Type"
	Description = "A type — defines properties (and optionally datasets) for the objects that implement it"
)

// DatasetProperties is the name of the dataset on a type object that
// holds its property-definition records.
const DatasetProperties = "properties"

// HandlerVersion is the DataVersion string stamped onto every change
// this handler emits against the `properties` dataset of a type
// object. Per docs/types-properties-proposal.md § "Change-level
// DataVersion", this dataset uses a hardcoded handler-chosen
// identifier (not a shortId) — bump the suffix if the validation
// rules ever change in a way that must reject stale writers.
const HandlerVersion = "typePropertyHandler-v1"

// PropertyHandler validates ops on a type object's `properties`
// dataset. The concrete rules will enforce, per
// docs/types-properties-proposal.md § "Schema evolution rules":
//
//   - first-write-wins on `id` (record id, immutable) and `type`
//     (kind), so a property's shape is pinned for life;
//   - classify "important" changes (property added / removed) so the
//     registry can mint a new shortId for the per-space `properties`
//     dataset's DataVersion;
//   - reject modifications to schema-bearing fields (`kind`, `items`,
//     `properties`) on existing records; display-only edits pass.
//
// Scaffolded as a no-op until the registry wiring lands.
type PropertyHandler struct{}

func (PropertyHandler) Dataset() string                              { return DatasetProperties }
func (PropertyHandler) Version() int                                 { return 1 }
func (PropertyHandler) Validate(_ crdt.RecordChange, _ crdt.Op) error { return nil }
