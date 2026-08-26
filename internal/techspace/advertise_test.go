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
	assert.True(t, techspace.DecodeSpaceIndexRecord(row).Advertise, "absent field")

	row.Set(techspace.FieldAdvertise, arena.NewFalse())
	assert.False(t, techspace.DecodeSpaceIndexRecord(row).Advertise)

	row.Set(techspace.FieldAdvertise, arena.NewTrue())
	assert.True(t, techspace.DecodeSpaceIndexRecord(row).Advertise)

	assert.True(t, techspace.DecodeSpaceIndexRecord(nil).Advertise == false, "zero record is not a decoded row")
}
