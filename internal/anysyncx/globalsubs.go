package anysyncx

import (
	"sync"
	"time"
)

const (
	// maxSubscriptionSpaceIds bounds one SpaceSubscription control
	// message: the ids are remote input and each one costs a lookup.
	maxSubscriptionSpaceIds = 64
	// maxSpaceIdLen bounds a space id (cid + "." + replication key).
	maxSpaceIdLen = 128
)

// validSpaceId reports a well-formed space id: the cid and replication
// key alphabets only, so a remote id can never spell an internal stream
// tag (those carry a "/").
func validSpaceId(id string) bool {
	if id == "" || len(id) > maxSpaceIdLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '.':
		default:
			return false
		}
	}
	return true
}

// validSpaceIds filters a control message's ids to well-formed ones,
// capped at maxSubscriptionSpaceIds.
func validSpaceIds(ids []string) []string {
	if len(ids) > maxSubscriptionSpaceIds {
		ids = ids[:maxSubscriptionSpaceIds]
	}
	out := ids[:0:0]
	for _, id := range ids {
		if validSpaceId(id) {
			out = append(out, id)
		}
	}
	return out
}

// globalSubs records which global peers asked this device for pushes
// of which spaces. An ask is re-sent on a cadence by the asking side,
// so an entry that is not refreshed expires: a peer whose node came
// back but whose withdrawal was lost, or whose stream died, stops
// receiving pushes on its own. The registry never touches the pool.
type globalSubs struct {
	mu   sync.Mutex
	subs map[string]map[string]time.Time // spaceId → peerId → last ask
	ttl  time.Duration
	now  func() time.Time
}

func newGlobalSubs(ttl time.Duration) *globalSubs {
	return &globalSubs{subs: map[string]map[string]time.Time{}, ttl: ttl, now: time.Now}
}

// Add records or refreshes peerId's ask for spaceIds.
func (s *globalSubs) Add(peerId string, spaceIds ...string) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, spaceId := range spaceIds {
		peers := s.subs[spaceId]
		if peers == nil {
			peers = map[string]time.Time{}
			s.subs[spaceId] = peers
		}
		peers[peerId] = now
	}
}

// Remove withdraws peerId's ask for spaceIds.
func (s *globalSubs) Remove(peerId string, spaceIds ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, spaceId := range spaceIds {
		if peers := s.subs[spaceId]; peers != nil {
			delete(peers, peerId)
			if len(peers) == 0 {
				delete(s.subs, spaceId)
			}
		}
	}
}

// Subscribers lists the peers with a live ask for spaceId, dropping
// expired ones on the way.
func (s *globalSubs) Subscribers(spaceId string) []string {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := s.subs[spaceId]
	var out []string
	for peerId, at := range peers {
		if now.Sub(at) > s.ttl {
			delete(peers, peerId)
			continue
		}
		out = append(out, peerId)
	}
	if len(peers) == 0 {
		delete(s.subs, spaceId)
	}
	return out
}
