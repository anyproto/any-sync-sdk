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
// validation without standing up an actual apply pipeline.
type stubHandler struct{ name string }

func (s stubHandler) Dataset() string                                        { return s.name }
func (s stubHandler) Version() int                                           { return 1 }
func (stubHandler) Init(_ context.Context) error                             { return nil }
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
			name: "valid single",
			extTypes: []handler.Type{
				{
					Id: "movie",
					Handlers: []handler.Registration{
						{Handler: stubHandler{name: "scenes"}, DataVersion: "scenes-v1"},
					},
				},
			},
		},
		{
			name: "valid multiple types and handlers",
			extTypes: []handler.Type{
				{
					Id: "movie",
					Handlers: []handler.Registration{
						{Handler: stubHandler{name: "scenes"}, DataVersion: "scenes-v1"},
						{Handler: stubHandler{name: "credits"}, DataVersion: "credits-v1"},
					},
				},
				{
					Id: "task",
					Handlers: []handler.Registration{
						{Handler: stubHandler{name: "subtasks"}, DataVersion: "subtasks-v1"},
					},
				},
			},
		},
		{
			name:     "empty type id",
			extTypes: []handler.Type{{Id: "", Handlers: []handler.Registration{{Handler: stubHandler{name: "x"}, DataVersion: "v1"}}}},
			err:      "empty Id",
		},
		{
			name: "duplicate type id",
			extTypes: []handler.Type{
				{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "a"}, DataVersion: "v1"}}},
				{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "b"}, DataVersion: "v1"}}},
			},
			err: `duplicate type Id "movie"`,
		},
		{
			name:     "type without handlers",
			extTypes: []handler.Type{{Id: "movie"}},
			err:      "zero handlers",
		},
		{
			name:     "nil handler",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{DataVersion: "v1"}}}},
			err:      "nil Handler",
		},
		{
			name:     "empty dataset",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: ""}, DataVersion: "v1"}}}},
			err:      "empty Dataset",
		},
		{
			name:     "empty data version",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "x"}}}}},
			err:      "empty DataVersion",
		},
		{
			name:     "collides with built-in objects",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "objects"}, DataVersion: "v1"}}}},
			err:      `dataset "objects" is reserved`,
		},
		{
			name:     "collides with built-in properties",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "properties"}, DataVersion: "v1"}}}},
			err:      `dataset "properties" is reserved`,
		},
		{
			name:     "collides with built-in shortIds",
			extTypes: []handler.Type{{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "shortIds"}, DataVersion: "v1"}}}},
			err:      `dataset "shortIds" is reserved`,
		},
		{
			name: "duplicate dataset across same type",
			extTypes: []handler.Type{{
				Id: "movie",
				Handlers: []handler.Registration{
					{Handler: stubHandler{name: "scenes"}, DataVersion: "v1"},
					{Handler: stubHandler{name: "scenes"}, DataVersion: "v2"},
				},
			}},
			err: `duplicate dataset "scenes"`,
		},
		{
			name: "duplicate dataset across types",
			extTypes: []handler.Type{
				{Id: "movie", Handlers: []handler.Registration{{Handler: stubHandler{name: "scenes"}, DataVersion: "v1"}}},
				{Id: "task", Handlers: []handler.Registration{{Handler: stubHandler{name: "scenes"}, DataVersion: "v2"}}},
			},
			err: `duplicate dataset "scenes"`,
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
		{
			Id: "movie",
			Handlers: []handler.Registration{
				{Handler: stubHandler{name: "scenes"}, DataVersion: "scenes-v3"},
			},
		},
	}
	require.NoError(t, ValidateExternalTypes(extTypes))

	store := NewStore(nil, nil, nil, "spaceA", nil, extTypes)

	// Built-in still resolves.
	v, err := store.DataVersion("objects")
	require.NoError(t, err)
	assert.Equal(t, "systemPropertyHandler-v1", v)

	// External type's handler resolves.
	v, err = store.DataVersion("scenes")
	require.NoError(t, err)
	assert.Equal(t, "scenes-v3", v)

	// Unknown dataset still errors.
	_, err = store.DataVersion("nope")
	assert.True(t, errors.Is(err, ErrUnknownDataset))
}
