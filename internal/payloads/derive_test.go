package payloads

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDerivedOwnerSeed pins the derived-owner seed format and the two
// properties the per-owner payloads id relies on: it folds the ownerId in
// (so the seed is unique per derived owner) and is deterministic (so every
// peer resolves the same id).
func TestDerivedOwnerSeed(t *testing.T) {
	const owner = "bafyowner123"

	// Exact format: WellKnownDeriveSeed + "/" + ownerId.
	assert.Equal(t, "builtin:payloads/"+owner, DerivedOwnerSeed(owner))
	assert.Equal(t, WellKnownDeriveSeed+"/"+owner, DerivedOwnerSeed(owner))

	// Idempotent — a pure function of ownerId.
	assert.Equal(t, DerivedOwnerSeed(owner), DerivedOwnerSeed(owner))

	// Per-owner unique — distinct owners never share a seed (so their
	// unparented payloads objects never collide on one id).
	assert.NotEqual(t, DerivedOwnerSeed(owner), DerivedOwnerSeed("bafyowner456"))

	// Never collides with the signed-owner seed, whatever the ownerId —
	// a signed and a derived owner can't resolve to the same payloads id.
	assert.NotEqual(t, WellKnownDeriveSeed, DerivedOwnerSeed(owner))
	assert.NotEqual(t, WellKnownDeriveSeed, DerivedOwnerSeed(""))
}
