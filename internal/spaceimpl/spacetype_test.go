package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/space"
)

// The derive header payload is the cold-restore source of truth for a
// space's app-level SpaceType: it is hashed into the derived id and read
// back from the header. These tests pin its two required properties —
// deterministic encoding (same input → identical bytes, since the bytes
// feed the content-addressed id) and lossless type recovery.

func TestEncodeDerivePayload_Deterministic(t *testing.T) {
	seed := []byte("project:demo")
	a := encodeDerivePayload(seed, "copilot.agent")
	b := encodeDerivePayload(seed, "copilot.agent")
	assert.Equal(t, a, b, "same (seed, type) must encode to identical bytes")

	assert.NotEqual(t, a, encodeDerivePayload(seed, "other"),
		"different type → different payload → different derived id")
	assert.NotEqual(t, a, encodeDerivePayload([]byte("other"), "copilot.agent"),
		"different seed → different payload → different derived id")
}

func TestDecodeDerivePayload_RecoversType(t *testing.T) {
	cases := []string{space.SpaceTypeRegular, "copilot.agent", "x"}
	for _, want := range cases {
		got, ok := decodeDerivePayload(encodeDerivePayload([]byte("s"), want))
		assert.True(t, ok, "encoded payload must decode")
		assert.Equal(t, want, got)
	}
}

func TestDecodeDerivePayload_RejectsForeign(t *testing.T) {
	// Raw seed (legacy / non-structured) and onetoone-style proto bytes
	// are not our JSON envelope — decode must report not-ok so callers
	// fall back to the header SpaceType.
	for _, b := range [][]byte{nil, {}, []byte("rawseed"), {0x0a, 0x02, 0x01, 0x02}} {
		_, ok := decodeDerivePayload(b)
		assert.False(t, ok, "non-envelope payload %q must not decode", b)
	}
}

func TestDeriveSpaceTypeTag_DefaultsToRegular(t *testing.T) {
	assert.Equal(t, space.SpaceTypeRegular, deriveSpaceTypeTag(""))
	assert.Equal(t, "copilot.agent", deriveSpaceTypeTag("copilot.agent"))
}
