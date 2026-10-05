package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// The key decides the collection: the module's canonical name is the
// canonical collection, any other key is namespaced under the type.
func TestModules_Collection(t *testing.T) {
	modules := NewModules(
		ModuleInfo{Name: "editor", Canonical: "editor_blocks"},
		ModuleInfo{Name: "chat", Canonical: "chat_messages", CanonicalOnly: true},
		ModuleInfo{Name: "plain"},
	)
	cases := []struct {
		name, key, module string
		want              string
		canonical         bool
	}{
		{"canonical key", "editor_blocks", "editor", "editor_blocks", true},
		{"another key", "summary", "editor", "type_summary", false},
		{"canonical-only canonical key", "chat_messages", "chat", "chat_messages", true},
		{"records", "segments", RecordsModule, "type_segments", false},
		{"no canonical", "notes", "plain", "type_notes", false},
		// Another module's canonical name is an ordinary key here.
		{"foreign canonical name", "chat_messages", "editor", "type_chat_messages", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := modules.Collection("type", tc.key, tc.module)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.canonical, modules.IsCanonical(tc.module, got))
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
			_, err := modules.Collection("type", tc.key, tc.module)
			require.ErrorIs(t, err, schema.ErrDecl)
		})
	}

	_, err := modules.Collection("type", "x", "sketch")
	require.ErrorIs(t, err, ErrUnknownModule)

	assert.Equal(t, "editor_blocks", modules.CanonicalKey("editor"))
	assert.Empty(t, modules.CanonicalKey(RecordsModule))
	assert.Empty(t, modules.CanonicalKey("sketch"))
}
