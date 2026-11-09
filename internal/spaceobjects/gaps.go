package spaceobjects

import "sync"

// GapSet holds the ids of a space's objects with a stored change that
// may be unapplied (object.Config.OnGap). It outlives the objects'
// eviction and the store's rebuild, so the clean-close watermark
// snapshot can leave the boot catch-up to them; a purge removes the
// entry. Safe for concurrent use.
type GapSet struct {
	m sync.Map
}

// NewGapSet returns an empty set.
func NewGapSet() *GapSet { return &GapSet{} }

// Set records (gap true) or clears (gap false) objectId's gap. A nil
// set records nothing.
func (g *GapSet) Set(objectId string, gap bool) {
	if g == nil {
		return
	}
	if gap {
		g.m.Store(objectId, struct{}{})
	} else {
		g.m.Delete(objectId)
	}
}

// Has reports whether objectId has a gap recorded.
func (g *GapSet) Has(objectId string) bool {
	if g == nil {
		return false
	}
	_, ok := g.m.Load(objectId)
	return ok
}

// Count is the number of objects with a gap recorded.
func (g *GapSet) Count() int {
	if g == nil {
		return 0
	}
	n := 0
	g.m.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
