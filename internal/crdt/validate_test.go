package crdt

import (
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateChange_Accepts confirms the happy path: well-formed
// change passes without touching the DB.
func TestValidateChange_Accepts(t *testing.T) {
	ctrl := newTestController(t)
	a := &anyenc.Arena{}
	ch := Change{
		Dataset:     testDS,
		DataVersion: testDataVersion,
		Records: []RecordChange{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []Op{{Type: OpSet, Path: []string{"name"}, Payload: a.NewString("Hello")}},
		}},
	}
	require.NoError(t, ctrl.ValidateChange(ch))
}

// TestValidateChange_RejectsEmptyDataVersion mirrors ApplyChange's
// existing constraint, but at the pre-AddContent moment.
func TestValidateChange_RejectsEmptyDataVersion(t *testing.T) {
	ctrl := newTestController(t)
	err := ctrl.ValidateChange(Change{Dataset: testDS})
	require.ErrorIs(t, err, ErrMissingDataVersion)
}

// TestValidateChange_RejectsUnknownDataset confirms an unregistered
// dataset name fails fast.
func TestValidateChange_RejectsUnknownDataset(t *testing.T) {
	ctrl := newTestController(t)
	err := ctrl.ValidateChange(Change{
		Dataset:     "no-such-dataset",
		DataVersion: testDataVersion,
	})
	require.ErrorIs(t, err, ErrUnknownDataset)
}

// TestValidateChange_RejectsInvalidPath catches structurally bad
// op paths (reserved field, dotted segment, empty segment) before
// they hit AddContent.
func TestValidateChange_RejectsInvalidPath(t *testing.T) {
	ctrl := newTestController(t)
	a := &anyenc.Arena{}
	cases := []struct {
		name string
		path []string
	}{
		{"reserved id", []string{"id"}},
		{"reserved underscore", []string{"_ver"}},
		{"empty segment", []string{""}},
		{"dotted segment", []string{"a.b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ctrl.ValidateChange(Change{
				Dataset:     testDS,
				DataVersion: testDataVersion,
				Records: []RecordChange{{
					Id:  "rec-1",
					Ops: []Op{{Type: OpSet, Path: tc.path, Payload: a.NewString("x")}},
				}},
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrValidation),
				"expected ErrValidation wrapper, got %v", err)
		})
	}
}

// TestValidateChange_RejectsEmptyIdWithoutUpsert: empty record id
// is only legal in upsert mode (auto-derives from ChangeId at apply
// time). Without Upsert we'd hit a silent no-op at apply — fail
// fast at validate.
func TestValidateChange_RejectsEmptyIdWithoutUpsert(t *testing.T) {
	ctrl := newTestController(t)
	err := ctrl.ValidateChange(Change{
		Dataset:     testDS,
		DataVersion: testDataVersion,
		Records: []RecordChange{{
			Id:     "",
			Upsert: false,
			Ops:    []Op{{Type: OpSet, Path: []string{"name"}}},
		}},
	})
	require.ErrorIs(t, err, ErrEmptyIdRequiresUpsert)
}

// TestValidateChange_AcceptsEmptyIdWithUpsert proves empty-id + Upsert
// passes pre-validation (the actual id derivation runs at apply
// time, when ChangeId is known).
func TestValidateChange_AcceptsEmptyIdWithUpsert(t *testing.T) {
	ctrl := newTestController(t)
	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ValidateChange(Change{
		Dataset:     testDS,
		DataVersion: testDataVersion,
		Records: []RecordChange{{
			Id:     "",
			Upsert: true,
			Ops:    []Op{{Type: OpSet, Path: []string{"name"}, Payload: a.NewString("x")}},
		}},
	}))
}
