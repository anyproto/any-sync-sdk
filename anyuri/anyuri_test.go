package anyuri_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/anyuri"
)

func TestBuildParseRoundTrip(t *testing.T) {
	cases := []struct {
		text string
		want anyuri.URI
	}{
		{"any://obj1", anyuri.URI{ObjectId: "obj1"}},
		{"any://space1/obj1", anyuri.URI{SpaceId: "space1", ObjectId: "obj1"}},
		{"any://obj1#turn_3", anyuri.URI{ObjectId: "obj1", Fragment: "turn_3"}},
		{"any://space1/obj1#turn_3", anyuri.URI{SpaceId: "space1", ObjectId: "obj1", Fragment: "turn_3"}},
	}
	for _, tc := range cases {
		u, err := anyuri.Parse(tc.text)
		require.NoError(t, err, tc.text)
		assert.Equal(t, tc.want, u, tc.text)
		assert.Equal(t, tc.text, u.String(), "String must round-trip")
		assert.True(t, anyuri.IsValid(tc.text))
	}

	assert.Equal(t, "any://obj1", anyuri.Build("obj1"))
	assert.Equal(t, "any://space1/obj1", anyuri.BuildGlobal("space1", "obj1"))
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, bad := range []string{
		"",
		"obj1",
		"any:/obj1",
		"http://obj1",
		"any://",
		"any:///obj1",
		"any://space1/",
		"any://a/b/c",
		"any://#frag",
	} {
		_, err := anyuri.Parse(bad)
		require.Error(t, err, "%q must not parse", bad)
		assert.True(t, errors.Is(err, anyuri.ErrInvalid), "%q error must wrap ErrInvalid", bad)
		assert.False(t, anyuri.IsValid(bad))
	}
}
