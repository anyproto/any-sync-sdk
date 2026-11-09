package spaceobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGapSet(t *testing.T) {
	g := NewGapSet()
	assert.Equal(t, 0, g.Count())
	g.Set("a", true)
	g.Set("b", true)
	g.Set("a", true)
	assert.True(t, g.Has("a"))
	assert.Equal(t, 2, g.Count())
	g.Set("a", false)
	g.Set("zzz", false)
	assert.False(t, g.Has("a"))
	assert.True(t, g.Has("b"))
	assert.Equal(t, 1, g.Count())
}
