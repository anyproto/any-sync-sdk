package crdt

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDroppedOnly(t *testing.T) {
	rollback := errors.New("rollback failed")
	assert.True(t, droppedOnly(errRecordDropped))
	assert.True(t, droppedOnly(errors.Join(errRecordDropped)))
	assert.True(t, droppedOnly(fmt.Errorf("upsert: %w", errors.Join(errRecordDropped))))
	assert.False(t, droppedOnly(errors.Join(errRecordDropped, rollback)))
	assert.False(t, droppedOnly(rollback))
}
