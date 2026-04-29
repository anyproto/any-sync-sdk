package object

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

func TestVersionAllocator_Monotonic(t *testing.T) {
	a := NewVersionAllocator("")
	prev := crdt.VersionId("")
	for i := 0; i < 10; i++ {
		v := a.Next()
		assert.Greater(t, string(v), string(prev), "iter %d", i)
		prev = v
	}
}

func TestVersionAllocator_LastTracks(t *testing.T) {
	a := NewVersionAllocator("")
	v1 := a.Next()
	assert.Equal(t, v1, a.Last())
	v2 := a.Next()
	assert.Greater(t, string(v2), string(v1))
	assert.Equal(t, v2, a.Last())
}

func TestVersionAllocator_BumpAdvances(t *testing.T) {
	a := NewVersionAllocator("")
	first := a.Next()
	a.Bump("zzzz")
	next := a.Next()
	assert.Greater(t, string(next), "zzzz")
	_ = first
}

func TestVersionAllocator_BumpNoRegression(t *testing.T) {
	a := NewVersionAllocator("zzzz")
	a.Bump("aaaa") // lower than current — no-op
	next := a.Next()
	assert.Greater(t, string(next), "zzzz")
}
