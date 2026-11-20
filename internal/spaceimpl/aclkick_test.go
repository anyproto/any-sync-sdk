package spaceimpl

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/stretchr/testify/assert"
)

type countingUpdater struct{ n atomic.Int64 }

func (c *countingUpdater) UpdateAcl(list.AclList) { c.n.Add(1) }

// The mux owns syncacl's single AclUpdater slot for two subscribers
// (members + ACL mirror watchers) — every kick must reach all of them,
// and a removed subscriber must stop receiving (the construction-race
// loser path).
func TestAclKickMux_FanoutAddRemove(t *testing.T) {
	m := &aclKickMux{}
	a, b := &countingUpdater{}, &countingUpdater{}

	m.UpdateAcl(nil) // no subscribers — must not panic
	m.add(a)
	m.add(b)
	m.UpdateAcl(nil)
	assert.EqualValues(t, 1, a.n.Load())
	assert.EqualValues(t, 1, b.n.Load())

	m.remove(a)
	m.remove(a) // double-remove is a no-op
	m.UpdateAcl(nil)
	assert.EqualValues(t, 1, a.n.Load(), "removed subscriber must stop receiving")
	assert.EqualValues(t, 2, b.n.Load())
}

// Kicks arrive from the syncacl write path while watchers register /
// deregister from wiring paths — the mux must tolerate that
// concurrency (run under -race).
func TestAclKickMux_ConcurrentAddRemoveFanout(t *testing.T) {
	m := &aclKickMux{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u := &countingUpdater{}
			for j := 0; j < 100; j++ {
				m.add(u)
				m.UpdateAcl(nil)
				m.remove(u)
			}
		}()
	}
	wg.Wait()
	m.UpdateAcl(nil) // all removed — must not panic
}
