package techspace_test

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

// The advertising switch is on unless the row says otherwise.
func TestSpaceIndexRecordAdvertiseDefaultsOn(t *testing.T) {
	arena := &anyenc.Arena{}
	row := arena.NewObject()
	row.Set("id", arena.NewString("s1"))
	assert.True(t, techspace.DecodeSpaceIndexRecord(row).P2PAdvertise, "absent field")

	row.Set(techspace.FieldP2PAdvertise, arena.NewFalse())
	assert.False(t, techspace.DecodeSpaceIndexRecord(row).P2PAdvertise)

	row.Set(techspace.FieldP2PAdvertise, arena.NewTrue())
	assert.True(t, techspace.DecodeSpaceIndexRecord(row).P2PAdvertise)

	assert.False(t, techspace.DecodeSpaceIndexRecord(nil).P2PAdvertise, "nil is not a decoded row")
}
