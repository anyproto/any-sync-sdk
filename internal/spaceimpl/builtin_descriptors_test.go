package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// Every SDK-declared property and dataset field carries the descriptive
// slice: a name, a description, and — when a descriptor is declared —
// a string `type` slug. The consumer vocabulary (slug ↔ kind) is the
// `any` server's to check; here the SDK pins only that nothing ships
// undescribed.
func TestBuiltInDescriptiveSlice(t *testing.T) {
	checkXFormat := func(t *testing.T, where string, xf map[string]any) {
		if xf == nil {
			return
		}
		slug, ok := xf["type"].(string)
		assert.True(t, ok && slug != "", "%s: x-format without a string type", where)
	}
	for _, defs := range map[string][]space.PropertyDef{
		anytype.TypeId:    builtInAnyProperties(),
		spaceindex.TypeId: builtInSpaceIndexProperties(),
		typetype.TypeId:   builtInMetaTypeProperties(),
	} {
		for _, d := range defs {
			assert.NotEmpty(t, d.Name, "%s: no name", d.Id)
			assert.NotEmpty(t, d.Description, "%s: no description", d.Id)
			checkXFormat(t, d.Id, d.XFormat)
		}
	}
	datasets := map[string]schema.Dataset{
		techspace.SpaceIndexDataset: techspace.SpaceIndexSchema(),
		"devices":                   techspace.DevicesSchema(),
		"profile":                   techspace.ProfileSchema(),
		"identities":                techspace.IdentitiesSchema(),
		"crdtVersion":               techspace.CRDTVersionSchema(),
		"inboxCursor":               techspace.InboxCursorSchema(),
		"bundles":                   spaceindex.BundlesSchema(),
		"payloads":                  payloads.Schema(),
	}
	for name, ds := range datasets {
		for _, f := range ds.Fields {
			where := name + "." + f.Id
			assert.NotEmpty(t, f.Name, "%s: no name", where)
			assert.NotEmpty(t, f.Description, "%s: no description", where)
			checkXFormat(t, where, f.XFormat)
		}
	}
	// The `any` table is what the objects-row descriptors are built
	// from; a slug there must match the declared kind's family.
	for _, p := range anytype.Properties {
		if slug, _ := p.XFormat["type"].(string); slug == "datetime" {
			assert.Equal(t, schema.KindDatetime, p.Kind, "%s: datetime slug on a non-datetime kind", p.Id)
		}
	}
}
