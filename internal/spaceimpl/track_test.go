package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/space"
)

func TestValidateSpaceId(t *testing.T) {
	// A well-formed any-sync spaceId: `<cid>.<repKey-base36>`.
	valid := "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2a5s8kx60jkm"
	assert.NoError(t, validateSpaceId(valid))

	for name, id := range map[string]string{
		"empty":            "",
		"no dot":           "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		"trailing dot":     "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.",
		"leading dot":      ".2a5s8kx60jkm",
		"bad cid":          "not-a-cid.2a5s8kx60jkm",
		"bad rep key":      "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.!!!",
		"rep key overflow": "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.zzzzzzzzzzzzzzzzzz",
	} {
		assert.ErrorIs(t, validateSpaceId(id), space.ErrBadSpaceId, "case %q must be rejected", name)
	}
}
