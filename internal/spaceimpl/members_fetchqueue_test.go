package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMemberWatcher_ProfileRequestsCoalesce pins the fetch queue: a
// request never blocks, identities accumulate until profileLoop takes
// them, a nil request means every member, and one signal covers any
// number of requests.
func TestMemberWatcher_ProfileRequestsCoalesce(t *testing.T) {
	w := &memberWatcher{
		fetchIds: make(map[string]struct{}),
		fetchCh:  make(chan struct{}, 1),
	}

	w.requestProfiles([]string{"a", "b"})
	w.requestProfiles([]string{"b", "c"})
	assert.Len(t, w.fetchCh, 1, "requests coalesce into one signal")

	all, ids := w.takeProfileRequests()
	assert.False(t, all)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, ids)

	all, ids = w.takeProfileRequests()
	assert.False(t, all, "a take drains the queue")
	assert.Empty(t, ids)

	w.requestProfiles(nil)
	all, _ = w.takeProfileRequests()
	assert.True(t, all, "nil asks for every member")
}
