package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

func testModules() Modules {
	return NewModules(
		ModuleInfo{Name: "editor", Canonical: "editor_blocks"},
		ModuleInfo{Name: "chat", Canonical: "chat_messages", CanonicalOnly: true},
		ModuleInfo{Name: "plain"},
	)
}

// A declaration follows its key: the module's canonical name is the
// canonical collection, any other key is namespaced under the type.
func TestModules_DraftCollection(t *testing.T) {
	modules := testModules()
	cases := []struct {
		name, typeId, key, module string
		want                      string
		canonical                 bool
	}{
		{"canonical key", "type", "editor_blocks", "editor", "editor_blocks", true},
		{"another key", "type", "summary", "editor", "type_summary", false},
		{"canonical-only canonical key", "type", "chat_messages", "chat", "chat_messages", true},
		{"records", "type", "segments", RecordsModule, "type_segments", false},
		{"no canonical", "type", "notes", "plain", "type_notes", false},
		// Another module's canonical name is an ordinary key here.
		{"foreign canonical name", "type", "chat_messages", "editor", "type_chat_messages", false},
		// The key decides, not the resulting name: a namespaced
		// collection that spells a canonical name stays namespaced.
		{"name spells a canonical", "editor", "blocks", "editor", "editor_blocks", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, canonical, err := modules.DraftCollection(tc.typeId, tc.key, tc.module)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.canonical, canonical)
			assert.Equal(t, tc.canonical, modules.IsCanonicalKey(tc.module, tc.key))
		})
	}

	refused := []struct{ name, key, module string }{
		{"canonical-only with another key", "thread", "chat"},
		{"canonical-only without a key", "", "chat"},
		{"records without a key", "", RecordsModule},
		{"no canonical without a key", "", "plain"},
		{"not a slug", "Bad Key", "editor"},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := modules.DraftCollection("type", tc.key, tc.module)
			require.ErrorIs(t, err, schema.ErrDecl)
		})
	}

	_, _, err := modules.DraftCollection("type", "x", "sketch")
	require.ErrorIs(t, err, ErrUnknownModule)

	assert.Equal(t, "editor_blocks", modules.CanonicalKey("editor"))
	assert.Empty(t, modules.CanonicalKey(RecordsModule))
	assert.Empty(t, modules.CanonicalKey("sketch"))
}

// A stored head follows its canonical marker, whatever its key: a head
// keyed by the canonical name without the marker is namespaced, and a
// marked head under any other key is refused.
func TestModules_Collection_StoredMarker(t *testing.T) {
	modules := testModules()

	got, err := modules.Collection("type", "editor_blocks", "editor", true)
	require.NoError(t, err)
	assert.Equal(t, "editor_blocks", got)

	got, err = modules.Collection("type", "editor_blocks", "editor", false)
	require.NoError(t, err)
	assert.Equal(t, "type_editor_blocks", got, "an unmarked head is namespaced even under the canonical name")

	refused := []struct {
		name, key, module string
		canonical         bool
	}{
		{"marked under another key", "notes", "editor", true},
		{"marked on a module without a canonical", "notes", "plain", true},
		{"marked records", "segments", RecordsModule, true},
		{"canonical-only unmarked under the canonical name", "chat_messages", "chat", false},
		{"canonical-only unmarked under another key", "thread", "chat", false},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			_, err := modules.Collection("type", tc.key, tc.module, tc.canonical)
			require.ErrorIs(t, err, schema.ErrDecl)
		})
	}
}
