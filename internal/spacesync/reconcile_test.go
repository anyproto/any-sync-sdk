package spacesync

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSettingsHeadChecksum_OrderIndependent(t *testing.T) {
	// A settings tree can hold >1 head after a concurrent/merged edit; two
	// peers with the same head SET (any order) must agree on the checksum.
	assert.Equal(t,
		settingsHeadChecksum([]string{"a", "b"}),
		settingsHeadChecksum([]string{"b", "a"}),
	)
	assert.NotEqual(t,
		settingsHeadChecksum([]string{"a"}),
		settingsHeadChecksum([]string{"a", "b"}),
	)
	assert.Equal(t, "", settingsHeadChecksum(nil))
}
