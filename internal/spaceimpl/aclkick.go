// Per-space ACL-update fan-out. any-sync's syncacl exposes a SINGLE
// AclUpdater slot (SetAclUpdater — last writer wins), but two SDK
// watchers want the kick: the members watcher and the push-key
// watcher. The mux owns the slot and fans UpdateAcl out to every
// registered subscriber.

package spaceimpl

import (
	"sync"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl/headupdater"
)

// aclKickMux fans one syncacl AclUpdater slot out to N subscribers.
// Subscribers' UpdateAcl implementations must be non-blocking —
// syncacl calls the slot synchronously on the ACL write path (both
// watchers coalesce into a buffered-1 kick channel).
type aclKickMux struct {
	mu   sync.Mutex
	subs []headupdater.AclUpdater
}

func (m *aclKickMux) UpdateAcl(a list.AclList) {
	m.mu.Lock()
	subs := make([]headupdater.AclUpdater, len(m.subs))
	copy(subs, m.subs)
	m.mu.Unlock()
	for _, s := range subs {
		s.UpdateAcl(a)
	}
}

func (m *aclKickMux) add(u headupdater.AclUpdater) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs = append(m.subs, u)
}

// remove detaches u. Required for watchers that lose a construction
// race after subscribing — without it a stopped watcher stays a
// subscriber (and is retained) for the space's whole loaded lifetime.
func (m *aclKickMux) remove(u headupdater.AclUpdater) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, s := range m.subs {
		if s == u {
			m.subs = append(m.subs[:i], m.subs[i+1:]...)
			return
		}
	}
}

// aclKickFanout returns spaceId's mux, creating it on first use, and
// (re)installs it as acl's updater. Reinstalling on every call is the
// point: a space offload evicts the loaded space, and the reload
// builds a fresh syncacl whose slot starts empty — each wiring path
// that holds a live acl handle routes through here so the slot is
// always repointed at the surviving mux. The offload path deletes the
// map entry (with its dead subscribers); rewiring re-registers them.
func (s *Service) aclKickFanout(spaceId string, acl list.AclList) *aclKickMux {
	s.mu.Lock()
	m := s.aclMuxes[spaceId]
	if m == nil {
		m = &aclKickMux{}
		// pubsub membership revalidation rides every mux: registering at
		// creation (under s.mu) covers each path that wires ACL watchers
		// — ensureSpaceIndexWiring on all load routes, members watcher —
		// exactly once per loaded lifetime, and dies with the mux on
		// offload.
		m.add(&pubsubAclWatcher{spaceId: spaceId, app: s.app})
		s.aclMuxes[spaceId] = m
	}
	s.mu.Unlock()
	if su, ok := acl.(syncacl.SyncAcl); ok {
		su.SetAclUpdater(m)
	}
	return m
}

// aclKickRemove detaches u from spaceId's mux, if one exists. Used by
// construction-race losers whose registration already happened inside
// their constructor.
func (s *Service) aclKickRemove(spaceId string, u headupdater.AclUpdater) {
	s.mu.Lock()
	m := s.aclMuxes[spaceId]
	s.mu.Unlock()
	if m != nil {
		m.remove(u)
	}
}
