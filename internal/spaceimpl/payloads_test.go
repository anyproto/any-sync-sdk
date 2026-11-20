package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// TestPayloadsDeriveOpts_SignedOwnerUnchanged pins the signed-owner
// derivation byte-for-byte to what it was before the derived-owner branch
// existed: ParentId=ownerId, seed=WellKnownDeriveSeed. This is the
// no-migration guarantee — existing signed-owner payloads objects keep
// their ids.
func TestPayloadsDeriveOpts_SignedOwnerUnchanged(t *testing.T) {
	const owner = "bafyownersigned"
	got := payloadsDeriveOpts(owner, false)
	assert.Equal(t, spaceobjects.DeriveOpts{
		ChangeType:    payloads.ChangeType,
		ChangePayload: []byte(payloads.WellKnownDeriveSeed),
		ParentId:      owner,
		Unencrypted:   true,
	}, got)
}

// TestPayloadsDeriveOpts_DerivedOwnerUnparented pins the derived-owner
// derivation: unparented (any-sync rejects a derived parent) with the
// ownerId folded into the seed. The ParentId-clear and seed-swap are
// coupled — both must be present together.
func TestPayloadsDeriveOpts_DerivedOwnerUnparented(t *testing.T) {
	const owner = "bafyownerderived"
	got := payloadsDeriveOpts(owner, true)
	assert.Equal(t, spaceobjects.DeriveOpts{
		ChangeType:    payloads.ChangeType,
		ChangePayload: []byte(payloads.DerivedOwnerSeed(owner)),
		ParentId:      "",
		Unencrypted:   true,
	}, got)
	// Coupling: the derived branch must clear ParentId AND change the seed.
	assert.Empty(t, got.ParentId, "derived-owner payloads must be unparented")
	assert.NotEqual(t, []byte(payloads.WellKnownDeriveSeed), got.ChangePayload,
		"derived-owner payloads must fold ownerId into the seed")
	// The plaintext class flag is identical on both branches — only
	// ParentId + seed change — so both remain node-readable payloads trees.
	assert.True(t, got.Unencrypted)
	assert.Equal(t, payloads.ChangeType, got.ChangeType)
}

// TestPayloadsDeriveOpts_PerOwnerDistinct verifies that distinct derived
// owners produce distinct opts (no collision) and that a derived owner's
// opts never match the signed shape of the SAME owner — the two classes
// resolve to different payloads objects.
func TestPayloadsDeriveOpts_PerOwnerDistinct(t *testing.T) {
	a := payloadsDeriveOpts("bafyownerA", true)
	b := payloadsDeriveOpts("bafyownerB", true)
	assert.NotEqual(t, a.ChangePayload, b.ChangePayload, "distinct derived owners must not collide")

	signed := payloadsDeriveOpts("bafyownerA", false)
	derived := payloadsDeriveOpts("bafyownerA", true)
	assert.NotEqual(t, signed.ParentId, derived.ParentId)
	assert.NotEqual(t, signed.ChangePayload, derived.ChangePayload)
}
