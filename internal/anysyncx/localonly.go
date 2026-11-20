package anysyncx

import "sync"

// localOnlySpaces is the set of spaceIds pinned to this device: a
// local-only space is never announced to sync nodes, never pushed, and
// never signed by the coordinator. Shared between the peer-manager
// provider (which hands local-only spaces an inert manager) and the
// credential provider (which refuses to request a SpaceSign receipt for
// them, as defense in depth — with no peers the push path is never
// reached).
//
// Headless mode marks the tech space here: a broker's tech space is a
// private registry of tracked foreign spaces, not an account space the
// network should ever learn about.
type localOnlySpaces struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func newLocalOnlySpaces() *localOnlySpaces {
	return &localOnlySpaces{ids: make(map[string]struct{})}
}

func (l *localOnlySpaces) mark(spaceId string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids[spaceId] = struct{}{}
}

func (l *localOnlySpaces) has(spaceId string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.ids[spaceId]
	return ok
}
