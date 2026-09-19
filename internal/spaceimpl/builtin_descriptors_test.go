package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// slugKinds is the kind each slug the SDK declares requires — the
// subset of the consumer vocabulary (the `any` server's
// docs/27-descriptors.md) built-ins use. An unlisted slug fails: the
// SDK never interprets descriptors, so one it does not know the shape
// of has no business in a Go declaration.
var slugKinds = map[string]schema.Kind{
	"text":     schema.KindString,
	"longtext": schema.KindString,
	"checkbox": schema.KindBoolean,
	"datetime": schema.KindDatetime,
}

// Every SDK-declared property and dataset field carries the descriptive
// slice: a name, a description, and — when a descriptor is declared —
// a slug that fits the declared kind.
func TestBuiltInDescriptiveSlice(t *testing.T) {
	check := func(t *testing.T, where, name, description string, kind schema.Kind, xf map[string]any) {
		assert.NotEmpty(t, name, "%s: no name", where)
		assert.NotEmpty(t, description, "%s: no description", where)
		if xf == nil {
			return
		}
		slug, ok := xf["type"].(string)
		if !ok || slug == "" {
			t.Errorf("%s: x-format without a string type", where)
			return
		}
		want, known := slugKinds[slug]
		if !known {
			t.Errorf("%s: slug %q not in the built-in vocabulary", where, slug)
			return
		}
		assert.Equal(t, want, kind, "%s: slug %q on the wrong kind", where, slug)
	}
	for _, p := range anytype.Properties {
		check(t, anytype.TypeId+"."+p.Id, p.Name, p.Description, p.Kind, p.XFormat)
	}
	for _, p := range spaceindex.Properties {
		check(t, spaceindex.TypeId+"."+p.Id, p.Name, p.Description, p.Kind, p.XFormat)
	}
	for _, p := range typetype.Properties {
		check(t, typetype.TypeId+"."+p.Id, p.Name, p.Description, p.Kind, p.XFormat)
	}
	for _, p := range collectiontype.Properties {
		check(t, collectiontype.TypeId+"."+p.Id, p.Name, p.Description, p.Kind, p.XFormat)
	}
	// The public views carry the slice through, deep-copied.
	for typeId, defs := range map[string][]space.PropertyDef{
		anytype.TypeId:        builtInAnyProperties(),
		spaceindex.TypeId:     builtInSpaceIndexProperties(),
		typetype.TypeId:       builtInMetaTypeProperties(),
		collectiontype.TypeId: builtInMetaCollectionProperties(),
	} {
		for _, d := range defs {
			assert.NotEmpty(t, d.Description, "%s.%s: description dropped by the view", typeId, d.Id)
		}
	}
	anyName := builtInAnyProperties()[6]
	assert.Equal(t, "name", anyName.Id)
	anyName.XFormat["type"] = "mutated"
	assert.Equal(t, "text", builtInAnyProperties()[6].XFormat["type"], "view aliases the shared table")
	for name, ds := range map[string]schema.Dataset{
		techspace.SpaceIndexDataset:  techspace.SpaceIndexSchema(),
		techspace.DevicesDataset:     techspace.DevicesSchema(),
		techspace.ProfileDataset:     techspace.ProfileSchema(),
		techspace.IdentitiesDataset:  techspace.IdentitiesSchema(),
		techspace.CRDTVersionDataset: techspace.CRDTVersionSchema(),
		techspace.InboxCursorDataset: techspace.InboxCursorSchema(),
		spaceindex.BundlesDataset:    spaceindex.BundlesSchema(),
		spaceindex.InviteKeysDataset: spaceindex.InviteKeysSchema(),
		"payloads":                   payloads.Schema(),
	} {
		for _, f := range ds.Fields {
			var kind schema.Kind
			if f.Schema != nil {
				kind = f.Schema.Kind
			}
			check(t, name+"."+f.Id, f.Name, f.Description, kind, f.XFormat)
		}
	}
}
