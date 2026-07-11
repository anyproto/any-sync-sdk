package techspace_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

// settingsOps builds the exact ops Service.SetSettings emits, failing
// the test on a validation error. Reuses the spaceindex_test fixture
// (controller + makeChange) — the ops route through the same handler /
// schema the live service uses.
func settingsOps(t *testing.T, arena *anyenc.Arena, set map[string]any, unset []string) []crdt.Op {
	t.Helper()
	ops, err := techspace.SettingsOps(arena, set, unset)
	require.NoError(t, err)
	return ops
}

func createSpaceRow(t *testing.T, ctrl *crdt.Controller, spaceId string, extra map[string]string) {
	t.Helper()
	arena := &anyenc.Arena{}
	fields := map[string]string{techspace.FieldType: "private"}
	for k, v := range extra {
		fields[k] = v
	}
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true, setMulti(arena, fields),
	)))
}

// ----------------------------------------------------------------------------
// Round trip — set lands, decodes to Go natives.
// ----------------------------------------------------------------------------

func TestSettings_SetRoundTrip(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const spaceId = "space-settings"
	createSpaceRow(t, ctrl, spaceId, nil)

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", spaceId, false,
		settingsOps(t, arena, map[string]any{
			"theme":     "dark",
			"pinned":    true,
			"sortOrder": 42,
		}, nil)...,
	)))

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, spaceId))
	require.NotNil(t, rec.Settings)
	assert.Equal(t, "dark", rec.Settings["theme"])
	assert.Equal(t, true, rec.Settings["pinned"])
	// Numbers ride as anyenc float64 (JSON semantics) — ints read back
	// as float64 by contract.
	assert.Equal(t, float64(42), rec.Settings["sortOrder"])
}

// A later per-key edit touches only its key — the sibling survives.
func TestSettings_PerKeyEditKeepsSiblings(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const spaceId = "space-perkey"
	createSpaceRow(t, ctrl, spaceId, nil)
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", spaceId, false,
		settingsOps(t, arena, map[string]any{"theme": "dark", "pinned": true}, nil)...,
	)))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", spaceId, false,
		settingsOps(t, arena, map[string]any{"theme": "light"}, nil)...,
	)))

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, spaceId))
	assert.Equal(t, "light", rec.Settings["theme"], "edited key updated")
	assert.Equal(t, true, rec.Settings["pinned"], "untouched sibling survived")
}

// ----------------------------------------------------------------------------
// Unset — removes exactly its key.
// ----------------------------------------------------------------------------

func TestSettings_UnsetRemovesKey(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const spaceId = "space-unset"
	createSpaceRow(t, ctrl, spaceId, nil)
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", spaceId, false,
		settingsOps(t, arena, map[string]any{"theme": "dark", "pinned": true}, nil)...,
	)))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", spaceId, false,
		settingsOps(t, arena, nil, []string{"theme"})...,
	)))

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, spaceId))
	require.NotNil(t, rec.Settings)
	_, hasTheme := rec.Settings["theme"]
	assert.False(t, hasTheme, "unset key removed")
	assert.Equal(t, true, rec.Settings["pinned"], "sibling survived the unset")
}

// ----------------------------------------------------------------------------
// Mirror coexistence — SetSpaceMetadata's multi-field $set must not
// clobber the settings subtree (it writes only its own columns).
// ----------------------------------------------------------------------------

func TestSettings_MetadataMirrorDoesNotClobber(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const spaceId = "space-mirror"
	createSpaceRow(t, ctrl, spaceId, nil)
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", spaceId, false,
		settingsOps(t, arena, map[string]any{"theme": "dark"}, nil)...,
	)))

	// The exact op shape SetSpaceMetadata emits: one multi-field $set
	// over name/description/icon/spaceType.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", spaceId, false,
		setMulti(arena, map[string]string{
			techspace.FieldName:        "Renamed",
			techspace.FieldDescription: "desc",
			techspace.FieldIcon:        "cid",
			techspace.FieldSpaceType:   "anytype.space",
		}),
	)))

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, spaceId))
	assert.Equal(t, "Renamed", rec.Name, "metadata landed")
	require.NotNil(t, rec.Settings, "settings survived the metadata mirror write")
	assert.Equal(t, "dark", rec.Settings["theme"])
}

// ----------------------------------------------------------------------------
// Deleted rows — settings stay writable. The handler's terminal rule
// guards only status fields (statusFields), so a tombstone row's
// settings still land. Pinned behavior: the row remains the account's
// record of the space, and per-space client state (e.g. "hide from
// archive list") is meaningful precisely on dead rows.
// ----------------------------------------------------------------------------

func TestSettings_WritableOnDeletedRow(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const spaceId = "space-tombstone"
	createSpaceRow(t, ctrl, spaceId, map[string]string{
		techspace.FieldRemoteStatus: techspace.StatusDeleted,
	})

	res, err := ctrl.ApplyChangeWithResult(ctx, makeChange("v2", spaceId, false,
		settingsOps(t, arena, map[string]any{"hidden": true}, nil)...,
	))
	require.NoError(t, err)
	assert.Empty(t, res.Rejections, "settings write on a deleted row must not be rejected")

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, spaceId))
	assert.Equal(t, techspace.StatusDeleted, rec.RemoteStatus, "tombstone intact")
	require.NotNil(t, rec.Settings)
	assert.Equal(t, true, rec.Settings["hidden"], "settings landed on the tombstone")
}

// ----------------------------------------------------------------------------
// SettingsOps validation.
// ----------------------------------------------------------------------------

func TestSettingsOps_Validation(t *testing.T) {
	arena := &anyenc.Arena{}
	cases := []struct {
		name  string
		set   map[string]any
		unset []string
		want  error
	}{
		{"empty patch", nil, nil, techspace.ErrSettingsEmpty},
		{"empty set key", map[string]any{"": "v"}, nil, techspace.ErrSettingsBadKey},
		{"dotted set key", map[string]any{"a.b": "v"}, nil, techspace.ErrSettingsBadKey},
		{"empty unset key", nil, []string{""}, techspace.ErrSettingsBadKey},
		{"dotted unset key", nil, []string{"a.b"}, techspace.ErrSettingsBadKey},
		{"set and unset overlap", map[string]any{"k": "v"}, []string{"k"}, techspace.ErrSettingsKeyOverlap},
		{"unsupported value", map[string]any{"k": []string{"no"}}, nil, techspace.ErrSettingsBadValue},
		{"nil value", map[string]any{"k": nil}, nil, techspace.ErrSettingsBadValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := techspace.SettingsOps(arena, tc.set, tc.unset)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestSettingsOps_Shape(t *testing.T) {
	arena := &anyenc.Arena{}
	ops, err := techspace.SettingsOps(arena,
		map[string]any{"b": "2", "a": "1"}, []string{"z"})
	require.NoError(t, err)
	require.Len(t, ops, 3)

	// Set ops first, sorted by key (deterministic changes), then unset.
	assert.Equal(t, crdt.OpSet, ops[0].Type)
	assert.Equal(t, []string{techspace.FieldSettings, "a"}, ops[0].Path)
	assert.Equal(t, crdt.OpSet, ops[1].Type)
	assert.Equal(t, []string{techspace.FieldSettings, "b"}, ops[1].Path)
	assert.Equal(t, crdt.OpUnset, ops[2].Type)
	assert.Equal(t, []string{techspace.FieldSettings, "z"}, ops[2].Path)
}
