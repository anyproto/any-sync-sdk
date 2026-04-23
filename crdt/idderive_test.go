package crdt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeriveRecordId_Deterministic(t *testing.T) {
	assert.Equal(t, DeriveRecordId("chA"), DeriveRecordId("chA"))
	assert.NotEqual(t, DeriveRecordId("chA"), DeriveRecordId("chB"))
}

func TestDeriveRecordId_LengthBounded(t *testing.T) {
	// 8-byte uint64 base58-encodes to at most 11 chars (log58(2^64) ≈ 10.93).
	for _, id := range []string{
		"chA", "chB",
		"bafyreidabcdefghijklmnopqrstuvwxyz1234567890abcdefghi",
		"",
		"🎬",
	} {
		got := DeriveRecordId(id)
		assert.LessOrEqual(t, len(got), 11, "got=%q for input %q", got, id)
		assert.Greater(t, len(got), 0, "derived id should not be empty")
	}
}
