package p2p

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/config"
)

// Tier is a peer's liveness class, derived from how long ago it was
// last seen. It sets how eagerly the connector dials the peer and
// whether the peer is offered to sync at all.
type Tier uint8

const (
	// TierActive — seen recently; kept connected with a short backoff.
	TierActive Tier = iota
	// TierStale — probed on a slow cadence, after active peers.
	TierStale
	// TierDormant — probed at startup and every few hours.
	TierDormant
	// TierDisabled — never dialed, dropped from the addr book and the
	// inbound allowlist until fresh evidence arrives.
	TierDisabled
)

// String returns a stable lowercase token for logging / status.
func (t Tier) String() string {
	switch t {
	case TierActive:
		return "active"
	case TierStale:
		return "stale"
	case TierDormant:
		return "dormant"
	default:
		return "disabled"
	}
}

// Thresholds are the tier boundaries on the age of a peer's LastSeen.
type Thresholds struct {
	Stale, Dormant, Disable time.Duration
}

// ThresholdsFrom reads the tier boundaries from the global config
// (defaults already applied by WithDefaults).
func ThresholdsFrom(g config.Global) Thresholds {
	return Thresholds{Stale: g.StaleAfter, Dormant: g.DormantAfter, Disable: g.DisableAfter}
}

// TierFor classifies a LastSeen age.
func TierFor(age time.Duration, th Thresholds) Tier {
	switch {
	case age >= th.Disable:
		return TierDisabled
	case age >= th.Dormant:
		return TierDormant
	case age >= th.Stale:
		return TierStale
	default:
		return TierActive
	}
}

// PeerRecord is the persisted liveness record of one peer.
type PeerRecord struct {
	// LastSeen is the newest evidence the peer is alive: a key-value
	// heartbeat (publisher clock, clamped to now) or a local
	// connection.
	LastSeen time.Time `json:"lastSeen"`
	// LastAttempt is the last global dial attempt.
	LastAttempt time.Time `json:"lastAttempt,omitempty"`
	// Failures counts consecutive failed global dials; a success or
	// fresh evidence resets it.
	Failures int `json:"failures,omitempty"`
}

// statusFile is the on-disk shape of the status book.
type statusFile struct {
	Peers map[string]PeerRecord `json:"peers"`
}

const statusSaveDelay = 2 * time.Second

// StatusBook keeps every known peer's liveness record, persisted as
// one JSON file under DataDir (same low-ceremony persistence as the
// p2p port file), written debounced. Records outlive restarts so a
// device that was offline for weeks resumes with the right tiers
// instead of dialing every stale peer at boot.
type StatusBook struct {
	path string
	th   Thresholds
	now  func() time.Time

	mu        sync.Mutex
	peers     map[string]PeerRecord
	dirty     bool
	saveTimer *time.Timer
	closed    bool
	// onAdvance fires (outside the lock) when a peer's LastSeen moved
	// forward — the connector's reactivation wake-up.
	onAdvance func(peerId string)
}

// NewStatusBook creates a book persisted at path; empty path keeps it
// in memory only.
func NewStatusBook(path string, th Thresholds) *StatusBook {
	return &StatusBook{
		path:  path,
		th:    th,
		now:   time.Now,
		peers: map[string]PeerRecord{},
	}
}

// SetOnAdvance installs the LastSeen-advanced hook. Set at wiring time.
func (b *StatusBook) SetOnAdvance(fn func(peerId string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onAdvance = fn
}

// Load reads the persisted records; a missing file is an empty book.
func (b *StatusBook) Load() error {
	if b.path == "" {
		return nil
	}
	raw, err := os.ReadFile(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f statusFile
	if err = json.Unmarshal(raw, &f); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, rec := range f.Peers {
		b.peers[id] = rec
	}
	return nil
}

// Seen records liveness evidence at time at. Publisher clocks are
// untrusted: at is clamped to now. Reports whether LastSeen advanced.
func (b *StatusBook) Seen(peerId string, at time.Time) bool {
	now := b.now()
	if at.After(now) {
		at = now
	}
	b.mu.Lock()
	rec := b.peers[peerId]
	if !at.After(rec.LastSeen) {
		b.mu.Unlock()
		return false
	}
	rec.LastSeen = at
	rec.Failures = 0
	b.peers[peerId] = rec
	b.markDirtyLocked()
	fn := b.onAdvance
	b.mu.Unlock()
	if fn != nil {
		fn(peerId)
	}
	return true
}

// Attempt records the outcome of a global dial: success is liveness
// evidence and clears the failure streak; failure extends it.
func (b *StatusBook) Attempt(peerId string, ok bool) {
	now := b.now()
	b.mu.Lock()
	rec := b.peers[peerId]
	rec.LastAttempt = now
	if ok {
		rec.Failures = 0
		if now.After(rec.LastSeen) {
			rec.LastSeen = now
		}
	} else {
		rec.Failures++
	}
	b.peers[peerId] = rec
	b.markDirtyLocked()
	b.mu.Unlock()
}

// Get returns a peer's record.
func (b *StatusBook) Get(peerId string) (PeerRecord, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.peers[peerId]
	return rec, ok
}

// LastSeen returns a peer's LastSeen, zero when unknown.
func (b *StatusBook) LastSeen(peerId string) time.Time {
	rec, _ := b.Get(peerId)
	return rec.LastSeen
}

// Tier classifies a peer now. Unknown peers are disabled: every global
// peer gets a Seen call from its key-value record before it is used.
func (b *StatusBook) Tier(peerId string) Tier {
	rec, ok := b.Get(peerId)
	if !ok || rec.LastSeen.IsZero() {
		return TierDisabled
	}
	return TierFor(b.now().Sub(rec.LastSeen), b.th)
}

// Thresholds returns the configured tier boundaries.
func (b *StatusBook) Thresholds() Thresholds { return b.th }

// Forget drops a peer's record.
func (b *StatusBook) Forget(peerId string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.peers[peerId]; !ok {
		return
	}
	delete(b.peers, peerId)
	b.markDirtyLocked()
}

// markDirtyLocked schedules a debounced save. Callers hold b.mu.
func (b *StatusBook) markDirtyLocked() {
	b.dirty = true
	if b.path == "" || b.closed || b.saveTimer != nil {
		return
	}
	b.saveTimer = time.AfterFunc(statusSaveDelay, func() {
		if err := b.Flush(); err != nil {
			log.Warn("persist peer status", zap.Error(err))
		}
	})
}

// Flush writes the book now (atomic replace). No-op when clean.
func (b *StatusBook) Flush() error {
	b.mu.Lock()
	if b.saveTimer != nil {
		b.saveTimer.Stop()
		b.saveTimer = nil
	}
	if !b.dirty || b.path == "" {
		b.mu.Unlock()
		return nil
	}
	snapshot := statusFile{Peers: make(map[string]PeerRecord, len(b.peers))}
	for id, rec := range b.peers {
		snapshot.Peers[id] = rec
	}
	b.dirty = false
	b.mu.Unlock()

	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	tmp := b.path + ".tmp"
	if err = os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, b.path)
}

// Close flushes pending changes and stops the debounce timer.
func (b *StatusBook) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return b.Flush()
}
