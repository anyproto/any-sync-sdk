package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/space"
)

// A time stamp's value is handler-produced. A declaration naming another
// kind is rejected rather than normalized: the draft is signed into the
// type object and read back by discovery, so accepting one the apply
// path then overrides would publish a lie.
func TestDraftFieldDecl_TimeStampRequiresDatetime(t *testing.T) {
	for _, stamp := range []space.Stamp{space.StampCreateTime, space.StampModifyTime} {
		_, err := draftFieldDecl(&space.DatasetFieldDraft{
			Key: "createdAt", Stamp: stamp, Kind: space.PropertyKindNumber,
		})
		assert.Error(t, err, "stamp %v with kind number", stamp)

		f, err := draftFieldDecl(&space.DatasetFieldDraft{Key: "createdAt", Stamp: stamp})
		require.NoError(t, err, "stamp %v with kind omitted", stamp)
		require.NotNil(t, f.Schema)
		assert.Equal(t, schema.KindDatetime, f.Schema.Kind)
	}
	// A Shape saying the same thing another way is refused too.
	_, err := draftFieldDecl(&space.DatasetFieldDraft{
		Key: "createdAt", Stamp: space.StampCreateTime, Shape: schema.Leaf(schema.KindNumber),
	})
	assert.Error(t, err, "stamped field declared through Shape")
}

// The descriptive slice rides the declaration untouched: description
// and the opaque descriptor bag land on the schema field the record
// encoder and discovery read from.
func TestDraftFieldDecl_CarriesDescriptorSlice(t *testing.T) {
	f, err := draftFieldDecl(&space.DatasetFieldDraft{
		Key: "stage", Kind: space.PropertyKindArray, Description: "Pipeline stage",
		Shape:   &schema.Schema{Kind: schema.KindArray, Items: schema.Leaf(schema.KindString)},
		XFormat: map[string]any{"type": "choice", "options": map[string]any{"lead": map[string]any{"name": "Lead"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "Pipeline stage", f.Description)
	assert.Equal(t, "choice", f.XFormat["type"])
	require.NotNil(t, f.Schema.Items)
	assert.Equal(t, schema.KindString, f.Schema.Items.Kind)
}
