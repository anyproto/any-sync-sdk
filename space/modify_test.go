package space

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A shared dataset's stored id is the object's prefix plus the plain
// record id, and both helpers leave the other form alone.
func TestSharedRecordId(t *testing.T) {
	assert.Equal(t, "obj1/r1", SharedRecordId("obj1", "r1"))
	assert.Equal(t, "obj1/r1", SharedRecordId("obj1", "obj1/r1"))
	assert.Equal(t, "r1", PlainRecordId("obj1", "obj1/r1"))
	assert.Equal(t, "r1", PlainRecordId("obj1", "r1"))
	assert.Equal(t, "obj2/r1", PlainRecordId("obj1", "obj2/r1"), "another object's id is not this object's record")
}
