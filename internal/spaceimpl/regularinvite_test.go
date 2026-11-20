package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegularInviteBody_RoundTrip(t *testing.T) {
	in := regularInviteBody{
		SpaceId:   "space1",
		SymKey:    "symkey-bytes",
		Name:      "Team space",
		SpaceType: "app.project",
	}
	raw, err := encodeRegularInviteBody(in)
	require.NoError(t, err)

	out, err := decodeRegularInviteBody(raw)
	require.NoError(t, err)
	in.Version = regularInviteVersion
	assert.Equal(t, in, out)
}

func TestRegularInviteBody_HintsOptional(t *testing.T) {
	raw, err := encodeRegularInviteBody(regularInviteBody{SpaceId: "space1"})
	require.NoError(t, err)
	out, err := decodeRegularInviteBody(raw)
	require.NoError(t, err)
	assert.Equal(t, "space1", out.SpaceId)
	assert.Empty(t, out.SymKey)
	assert.Empty(t, out.Name)
	assert.Empty(t, out.SpaceType)
}

func TestRegularInviteBody_Rejections(t *testing.T) {
	// Encode requires a spaceId.
	_, err := encodeRegularInviteBody(regularInviteBody{})
	assert.ErrorIs(t, err, errInvalidRegularInvite)

	// Junk bytes.
	_, err = decodeRegularInviteBody([]byte("not json"))
	assert.ErrorIs(t, err, errInvalidRegularInvite)

	// Unknown version.
	_, err = decodeRegularInviteBody([]byte(`{"v":99,"spaceId":"s1"}`))
	assert.ErrorIs(t, err, errInvalidRegularInvite)

	// Missing spaceId.
	_, err = decodeRegularInviteBody([]byte(`{"v":1}`))
	assert.ErrorIs(t, err, errInvalidRegularInvite)

	// A 1-1 style raw-symkey body must not decode as a regular invite.
	_, err = decodeRegularInviteBody([]byte("raw-symkey-string"))
	assert.ErrorIs(t, err, errInvalidRegularInvite)
}
