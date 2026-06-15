package spaceobjects

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
)

// stubHandler is the minimal Handler used to exercise registration
// validation without standing up an actual apply pipeline. Name /
// version / indexes live on the handler.Dataset, not here.
type stubHandler struct{}

func (stubHandler) Init(_ context.Context) error { return nil }
func (stubHandler) BeforeCreate(*handler.ChangeCtx, *handler.RecordChange, *handler.Sink) error {
	return nil
}
func (stubHandler) BeforeModify(*handler.ChangeCtx, *handler.RecordChange, *handler.Op, *handler.Sink) error {
	return nil
}
func (stubHandler) BeforeDelete(*handler.ChangeCtx, *handler.RecordChange, *handler.Sink) error {
	return nil
}

func TestValidateExternalTypes(t *testing.T) {
	cases := []struct {
		name     string
		extTypes []handler.Type
		err      string
	}{
		{
			name:     "empty ok",
			extTypes: nil,
		},
		{
			name: "valid single dataset",
			extTypes: []handler.Type{
				{Id: "movie", Datasets: []handler.Dataset{
					{Name: "scenes", DataVersion: "scenes-v1", Handler: stubHandler{}},
				}},
			},
		},
		{
			name: "valid: one type owns many datasets",
			extTypes: []handler.Type{
				{Id: "movie", Datasets: []handler.Dataset{
					{Name: "scenes", DataVersion: "scenes-v1", Handler: stubHandler{}},
					{Name: "credits", DataVersion: "credits-v1", Handler: stubHandler{}},
				}},
				{Id: "task", Datasets: []handler.Dataset{
					{Name: "subtasks", DataVersion: "subtasks-v1", Handler: stubHandler{}},
				}},
			},
		},
		{
			name:     "valid: pure-declaration type (no datasets, no properties)",
			extTypes: []handler.Type{{Id: "tag"}},
		},
		{
			name: "valid: property-only type",
			extTypes: []handler.Type{{
				Id: "nav",
				Properties: []handler.PropertyDecl{
					{Id: "type", Kind: handler.PropertyKindNumber},
					{Id: "pos", Kind: handler.PropertyKindString},
				},
			}},
		},
		{
			name: "valid: datasets + properties on one type",
			extTypes: []handler.Type{{
				Id:         "doc",
				Datasets:   []handler.Dataset{{Name: "doc_blocks", DataVersion: "doc-v1", Handler: stubHandler{}}},
				Properties: []handler.PropertyDecl{{Id: "title", Kind: handler.PropertyKindString}},
			}},
		},
		{
			name:     "empty type id",
			extTypes: []handler.Type{{Id: "", Datasets: []handler.Dataset{{Name: "x", DataVersion: "v1", Handler: stubHandler{}}}}},
			err:      "empty Id",
		},
		{
			name: "duplicate type id",
			extTypes: []handler.Type{
				{Id: "movie", Datasets: []handler.Dataset{{Name: "a", DataVersion: "v1", Handler: stubHandler{}}}},
				{Id: "movie", Datasets: []handler.Dataset{{Name: "b", DataVersion: "v1", Handler: stubHandler{}}}},
			},
			err: `duplicate type Id "movie"`,
		},
		{
			name: "property with invalid kind",
			extTypes: []handler.Type{{
				Id:         "nav",
				Properties: []handler.PropertyDecl{{Id: "pos"}}, // zero Kind
			}},
			err: "invalid Kind",
		},
		{
			name: "property with reserved id",
			extTypes: []handler.Type{{
				Id:         "nav",
				Properties: []handler.PropertyDecl{{Id: "_x", Kind: handler.PropertyKindString}},
			}},
			err: "reserved or invalid Id",
		},
		{
			name: "duplicate property id",
			extTypes: []handler.Type{{
				Id: "nav",
				Properties: []handler.PropertyDecl{
					{Id: "pos", Kind: handler.PropertyKindString},
					{Id: "pos", Kind: handler.PropertyKindNumber},
				},
			}},
			err: "duplicate property Id",
		},
		{
			name:     "nil handler",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{Name: "x", DataVersion: "v1"}}}},
			err:      "nil Handler",
		},
		{
			name:     "empty dataset name",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{DataVersion: "v1", Handler: stubHandler{}}}}},
			err:      "empty Name",
		},
		{
			name:     "empty data version",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{Name: "x", Handler: stubHandler{}}}}},
			err:      "empty DataVersion",
		},
		{
			name:     "collides with built-in objects",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{Name: "objects", DataVersion: "v1", Handler: stubHandler{}}}}},
			err:      `"objects" is reserved`,
		},
		{
			name:     "collides with built-in properties",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{Name: "properties", DataVersion: "v1", Handler: stubHandler{}}}}},
			err:      `"properties" is reserved`,
		},
		{
			name:     "collides with built-in shortIds",
			extTypes: []handler.Type{{Id: "movie", Datasets: []handler.Dataset{{Name: "shortIds", DataVersion: "v1", Handler: stubHandler{}}}}},
			err:      `"shortIds" is reserved`,
		},
		{
			name: "duplicate dataset across same type",
			extTypes: []handler.Type{{
				Id: "movie",
				Datasets: []handler.Dataset{
					{Name: "scenes", DataVersion: "v1", Handler: stubHandler{}},
					{Name: "scenes", DataVersion: "v2", Handler: stubHandler{}},
				},
			}},
			err: `duplicate dataset name "scenes"`,
		},
		{
			name: "duplicate dataset across types",
			extTypes: []handler.Type{
				{Id: "movie", Datasets: []handler.Dataset{{Name: "scenes", DataVersion: "v1", Handler: stubHandler{}}}},
				{Id: "task", Datasets: []handler.Dataset{{Name: "scenes", DataVersion: "v2", Handler: stubHandler{}}}},
			},
			err: `duplicate dataset name "scenes"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalTypes(tc.extTypes)
			if tc.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestStore_DataVersion_External(t *testing.T) {
	extTypes := []handler.Type{
		{Id: "movie", Datasets: []handler.Dataset{
			{Name: "scenes", DataVersion: "scenes-v3", Handler: stubHandler{}},
		}},
	}
	require.NoError(t, ValidateExternalTypes(extTypes))

	store := NewStore(nil, nil, nil, "spaceA", nil, extTypes)

	// Built-in still resolves.
	v, err := store.DataVersion("objects")
	require.NoError(t, err)
	assert.Equal(t, "systemPropertyHandler-v1", v)

	// External type's dataset resolves.
	v, err = store.DataVersion("scenes")
	require.NoError(t, err)
	assert.Equal(t, "scenes-v3", v)

	// Unknown dataset still errors.
	_, err = store.DataVersion("nope")
	assert.True(t, errors.Is(err, ErrUnknownDataset))
}

func TestStore_DatasetOwner(t *testing.T) {
	extTypes := []handler.Type{
		{Id: "movie", Datasets: []handler.Dataset{
			{Name: "scenes", DataVersion: "v1", Handler: stubHandler{}},
			{Name: "credits", DataVersion: "v1", Handler: stubHandler{}},
		}},
		{Id: "nav", Properties: []handler.PropertyDecl{{Id: "pos", Kind: handler.PropertyKindString}}},
	}
	require.NoError(t, ValidateExternalTypes(extTypes))
	store := NewStore(nil, nil, nil, "spaceA", nil, extTypes)

	// Both datasets of one type map to that owner (N→1).
	owner, ok := store.DatasetOwner("scenes")
	assert.True(t, ok)
	assert.Equal(t, "movie", owner)
	owner, ok = store.DatasetOwner("credits")
	assert.True(t, ok)
	assert.Equal(t, "movie", owner)

	// Built-in and property-only types own no external dataset.
	_, ok = store.DatasetOwner("objects")
	assert.False(t, ok)
	_, ok = store.DatasetOwner("nope")
	assert.False(t, ok)
}
