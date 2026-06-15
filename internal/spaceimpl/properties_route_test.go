package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// routeRegistry declares one type with one prop per scope plus a
// scopeless (pre-scope) prop that must read as synced.
func routeRegistry() *types.StubRegistry {
	r := &types.StubRegistry{}
	r.Set("movie", "legacy", schema.KindString) // no scope recorded → synced
	r.SetScoped("movie", "title", schema.KindString, schema.ScopeSynced)
	r.SetScoped("movie", "read", schema.KindBoolean, schema.ScopeAccount)
	r.SetScoped("movie", "pin", schema.KindBoolean, schema.ScopeLocal)
	return r
}

func TestResolveRoute_SingleScope(t *testing.T) {
	reg := routeRegistry()

	for _, tc := range []struct {
		name  string
		patch map[string]any
		want  schema.Scope
	}{
		{"synced", map[string]any{"title": "x"}, schema.ScopeSynced},
		{"pre-scope default", map[string]any{"legacy": "x"}, schema.ScopeSynced},
		{"account", map[string]any{"read": true}, schema.ScopeAccount},
		{"local", map[string]any{"pin": true}, schema.ScopeLocal},
		{"account multi-key", map[string]any{"read": true}, schema.ScopeAccount},
	} {
		got, _, err := resolveRoute(reg, "movie", tc.patch)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
}

// Unresolvable keys / types don't pick a route — the synced route's
// strict PreValidate owns the precise rejection downstream.
func TestResolveRoute_UnresolvedFallsToSynced(t *testing.T) {
	reg := routeRegistry()

	got, _, err := resolveRoute(reg, "movie", map[string]any{"ghost": 1})
	require.NoError(t, err)
	assert.Equal(t, schema.ScopeSynced, got)

	got, _, err = resolveRoute(reg, "unknown-type", map[string]any{"title": "x"})
	require.NoError(t, err)
	assert.Equal(t, schema.ScopeSynced, got)

	// Typed-nil registry (raw-mode store) — bring-up passthrough.
	var nilReg *types.LiveRegistry
	got, _, err = resolveRoute(nilReg, "movie", map[string]any{"title": "x"})
	require.NoError(t, err)
	assert.Equal(t, schema.ScopeSynced, got)
}

func TestResolveRoute_MixedScopesRejected(t *testing.T) {
	reg := routeRegistry()
	_, _, err := resolveRoute(reg, "movie", map[string]any{"title": "x", "read": true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spans multiple scopes")
	assert.Contains(t, err.Error(), "synced: [title]")
	assert.Contains(t, err.Error(), "account: [read]")
}

// An unresolved key riding along with a resolved non-synced key takes
// the resolved route (where its strict validation will reject it with
// the precise unknown_property error).
func TestResolveRoute_UnresolvedRidesResolvedRoute(t *testing.T) {
	reg := routeRegistry()
	got, _, err := resolveRoute(reg, "movie", map[string]any{"read": true, "ghost": 1})
	require.NoError(t, err)
	assert.Equal(t, schema.ScopeAccount, got)
}
