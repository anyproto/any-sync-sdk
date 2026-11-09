package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// A space's gap set is one object across store rebuilds, counts for the
// close snapshot, and goes with the space on offload.
func TestService_GapSetLifecycle(t *testing.T) {
	s := &Service{gapSets: map[string]*spaceobjects.GapSet{}}
	assert.Equal(t, 0, s.ApplyGapCount("sp"), "a space never loaded has no gaps")

	g := s.gapSetFor("sp")
	assert.Same(t, g, s.gapSetFor("sp"), "a rebuilt store gets the same set")
	g.Set("obj", true)
	assert.Equal(t, 1, s.ApplyGapCount("sp"))
	assert.Equal(t, 0, s.ApplyGapCount("other"))

	s.dropGapSet("sp")
	assert.Equal(t, 0, s.ApplyGapCount("sp"))
	assert.NotSame(t, g, s.gapSetFor("sp"), "an offloaded space starts over")
}
