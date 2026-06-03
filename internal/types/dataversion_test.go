package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

func TestEncodeDataVersion(t *testing.T) {
	cases := []struct {
		name  string
		pairs []types.DataVersionPair
		want  string
	}{
		{"nil", nil, ""},
		{"empty slice", []types.DataVersionPair{}, ""},
		{"single", []types.DataVersionPair{{TypeId: "tA", ShortId: "sA"}}, "tA:sA"},
		{"multi", []types.DataVersionPair{{TypeId: "tA", ShortId: "sA"}, {TypeId: "tB", ShortId: "sB"}}, "tA:sA;tB:sB"},
		// A pair with no shortId means the type has had no important
		// change yet — nothing to gate on, so it's skipped.
		{"skip empty shortId", []types.DataVersionPair{{TypeId: "tA", ShortId: "sA"}, {TypeId: "tB", ShortId: ""}}, "tA:sA"},
		{"skip empty typeId", []types.DataVersionPair{{TypeId: "", ShortId: "sA"}, {TypeId: "tB", ShortId: "sB"}}, "tB:sB"},
		{"all empty yields empty", []types.DataVersionPair{{TypeId: "tA", ShortId: ""}, {TypeId: "", ShortId: "sB"}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, types.EncodeDataVersion(tc.pairs))
		})
	}
}

func TestParseDataVersion(t *testing.T) {
	t.Run("empty is nil, no error", func(t *testing.T) {
		got, err := types.ParseDataVersion("")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("single", func(t *testing.T) {
		got, err := types.ParseDataVersion("tA:sA")
		require.NoError(t, err)
		assert.Equal(t, []types.DataVersionPair{{TypeId: "tA", ShortId: "sA"}}, got)
	})

	t.Run("multi", func(t *testing.T) {
		got, err := types.ParseDataVersion("tA:sA;tB:sB")
		require.NoError(t, err)
		assert.Equal(t, []types.DataVersionPair{
			{TypeId: "tA", ShortId: "sA"},
			{TypeId: "tB", ShortId: "sB"},
		}, got)
	})

	// Malformed inputs the gate must reject (so it can't silently treat a
	// garbled version as a valid single pair). A parse error is what the
	// gate turns into "legacy / pass-through".
	for _, bad := range []struct {
		name, in string
	}{
		{"no colon", "ts"},
		{"empty typeId (leading colon)", ":sA"},
		{"empty shortId (trailing colon)", "tA:"},
		{"second pair malformed", "tA:sA;bad"},
		{"bare separator", ";"},
		{"trailing separator", "tA:sA;"},
	} {
		t.Run("invalid/"+bad.name, func(t *testing.T) {
			_, err := types.ParseDataVersion(bad.in)
			assert.ErrorIs(t, err, types.ErrInvalidDataVersion)
		})
	}
}

func TestDataVersion_RoundTrip(t *testing.T) {
	in := []types.DataVersionPair{{TypeId: "tA", ShortId: "sA"}, {TypeId: "tB", ShortId: "sB"}}
	got, err := types.ParseDataVersion(types.EncodeDataVersion(in))
	require.NoError(t, err)
	assert.Equal(t, in, got)
}
