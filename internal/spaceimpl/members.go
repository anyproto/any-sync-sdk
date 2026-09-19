package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// memberPollInterval governs how often the watcher checks AclList.Head().Id
// for changes. Cheap (one RLock + map lookup); 250 ms feels live to a UI
// without burning CPU.
const memberPollInterval = 250 * time.Millisecond

// identityRepoPollInterval governs how often the watcher refreshes
// every member's profile from identityRepo. Network call to the
// coordinator — a low rate is fine; profile updates are rare and
// non-time-critical (display name / icon).
const identityRepoPollInterval = 60 * time.Second

// membersAPI implements space.MembersAPI. List/Get/Me read directly
// off the locally replicated AclList (point-in-time, always current).
// Subscribe and Query are backed by a per-space watcher: it polls
// AclList.Head().Id, diffs the membership snapshot on change, fans
// out MemberEvents to subscribers AND reconciles the materialised
// members collection used by Query.
//
// Watcher lifecycle is sticky: started on the first Subscribe or
// Query call, runs until SDK shutdown (which drains all watchers via
// Service.watchers). This keeps Query reads cheap on subsequent
// access without paying a re-seed cost.
type membersAPI struct {
	s *spaceImpl

	// watcherMu guards watcher start. The collection has its own
	// init via sync.Once so we don't deadlock collection() against
	// ensureWatcher() (the watcher needs the collection during
	// construction).
	watcherMu sync.Mutex
	watcher   *memberWatcher

	collOnce sync.Once
	collVal  anystore.Collection
	collErr  error
}

func newMembersAPI(s *spaceImpl) *membersAPI { return &membersAPI{s: s} }

// collection returns the materialised members collection, opening it
// on first call. Per-space; named "<spaceId>_members" via the SDK
// shared db. Never blocks the watcher lock.
func (m *membersAPI) collection(ctx context.Context) (anystore.Collection, error) {
	m.collOnce.Do(func() {
		collName := m.s.id + "_" + MembersCollection
		coll, err := m.s.parent.db.Collection(ctx, collName)
		if err != nil {
			m.collErr = fmt.Errorf("members: open %s: %w", collName, err)
			return
		}
		m.collVal = coll
	})
	if m.collErr != nil {
		return nil, m.collErr
	}
	return m.collVal, nil
}

// aclList loads the underlying any-sync space and returns its
// AclList. Cheap — the cache returns the same loaded space until TTL
// eviction.
func (m *membersAPI) aclList(ctx context.Context) (list.AclList, error) {
	handle, err := m.s.app.GetSpace(ctx, m.s.id)
	if err != nil {
		return nil, fmt.Errorf("members: load space: %w", err)
	}
	return handle.Inner().Acl(), nil
}

// List returns every account currently visible in the ACL: active
// members, removed tombstones, and pending join requests. Profile
// overrides from identityRepo (cached on the running watcher) are
// applied on top — the same view Subscribe / Query expose.
func (m *membersAPI) List(ctx context.Context) ([]space.Member, error) {
	acl, err := m.aclList(ctx)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	members, _ := collectMembers(acl)
	acl.RUnlock()
	m.applyProfiles(members)
	return members, nil
}

func (m *membersAPI) Get(ctx context.Context, identity string) (space.Member, error) {
	pk, err := decodeIdentity(identity)
	if err != nil {
		return space.Member{}, err
	}
	acl, err := m.aclList(ctx)
	if err != nil {
		return space.Member{}, err
	}
	acl.RLock()
	state := acl.AclState()
	syntheticOwner := syntheticOneToOneOwner(state)
	var found *space.Member
	for _, acc := range state.CurrentAccounts() {
		// The synthetic 1-1 owner is not a real member — a Get on its
		// (unguessable) id resolves to not-found, same as any non-member.
		if syntheticOwner != nil && acc.PubKey.Equals(syntheticOwner) {
			continue
		}
		if acc.PubKey.Equals(pk) {
			val := memberFromAccountState(acc)
			found = &val
			break
		}
	}
	if found == nil {
		// Also check pending join requests so callers can Get a joining
		// identity by their account id.
		if reqs, _ := state.JoinRecords(true); reqs != nil {
			for _, r := range reqs {
				if r.RequestIdentity.Equals(pk) {
					val := memberFromJoinRecord(r)
					found = &val
					break
				}
			}
		}
	}
	acl.RUnlock()
	if found == nil {
		return space.Member{}, space.ErrNotFound
	}
	m.applyProfile(found)
	return *found, nil
}

func (m *membersAPI) Me(ctx context.Context) (space.Member, error) {
	acl, err := m.aclList(ctx)
	if err != nil {
		return space.Member{}, err
	}
	acl.RLock()
	state := acl.AclState()
	me := state.Identity()
	var found *space.Member
	for _, acc := range state.CurrentAccounts() {
		if acc.PubKey.Equals(me) {
			val := memberFromAccountState(acc)
			found = &val
			break
		}
	}
	acl.RUnlock()
	if found == nil {
		return space.Member{}, space.ErrNotFound
	}
	m.applyProfile(found)
	return *found, nil
}

func (m *membersAPI) JoinRequests(ctx context.Context) ([]space.JoinRequestInfo, error) {
	acl, err := m.aclList(ctx)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	reqs, symKeys := collectJoinRequests(acl)
	acl.RUnlock()
	m.resolveJoinRequestProfiles(ctx, reqs, symKeys)
	return reqs, nil
}

// resolveJoinRequestProfiles fills each request's name/icon from the
// requester's identityRepo profile, decrypted with the symkey carried on
// the (decrypted) join record. Profiles are cached in the shared member
// watcher: a cache hit skips the network, a miss does ONE fetch and
// stores the result (so repeat calls — and member views — reuse it). The
// watcher's profile loop refreshes the cache over time. Best-effort: a
// missing key or offline coordinator leaves that request identity-only.
func (m *membersAPI) resolveJoinRequestProfiles(ctx context.Context, reqs []space.JoinRequestInfo, symKeys map[string]string) {
	s := m.s.parent
	if s == nil {
		return
	}
	w := m.ensureWatcher()
	for i := range reqs {
		id := reqs[i].Identity
		if w != nil {
			if p, ok := w.cachedProfile(id); ok {
				applyJoinProfile(&reqs[i], p)
				continue
			}
		}
		enc, ok := symKeys[id]
		if !ok {
			continue
		}
		key, err := space.UnmarshalSymKey(enc)
		if err != nil {
			continue
		}
		prof, ok := s.fetchIdentityProfile(ctx, id, key)
		if !ok {
			continue
		}
		applyJoinProfile(&reqs[i], prof)
		if w != nil {
			w.cacheProfile(id, prof)
		}
		// Write through to the identities directory.
		_ = s.tsp.SetIdentityProfile(ctx, id, prof.Name, prof.Description, prof.IconCID)
	}
}

// dedupStrings merges two id slices into one with duplicates removed.
func dedupStrings(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	for _, s := range b {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// applyJoinProfile overlays non-empty profile fields onto a join request.
func applyJoinProfile(r *space.JoinRequestInfo, p space.AccountMetadata) {
	if p.Name != "" {
		r.Name = p.Name
	}
	if p.Description != "" {
		r.Description = p.Description
	}
	if p.IconCID != "" {
		r.IconCID = p.IconCID
	}
}

func (m *membersAPI) Invites(ctx context.Context) ([]space.InviteInfo, error) {
	// Custody read before the ACL lock — tsp.Get takes its own locks.
	custody, hasCustody := m.s.loadIssuedKey(ctx, techspace.IssuedKeyMember)
	acl, err := m.aclList(ctx)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	invites := acl.AclState().Invites()
	active := selfAclActive(acl.AclState())
	acl.RUnlock()
	out := make([]space.InviteInfo, 0, len(invites))
	for _, inv := range invites {
		info := space.InviteInfo{
			RecordId:   inv.Id,
			Permission: fromAclPermissions(inv.Permissions),
		}
		if active && inv.Type == aclrecordproto.AclInviteType_RequestToJoin {
			info.Key = m.s.sharedInviteKey(ctx, inv.Key)
		}
		if active && inv.Type == aclrecordproto.AclInviteType_RequestToJoin && info.Key == nil && hasCustody && inv.Key != nil && inv.Key.Equals(custody.GetPublic()) {
			info.Key = custody
		}
		out = append(out, info)
	}
	return out, nil
}

// Subscribe attaches a firehose listener. The watcher is started on
// the first Subscribe or Query call and runs until SDK shutdown
// (Service.Close drains all watchers). Detaching the last subscriber
// does NOT stop the watcher — Query callers expect fresh data on
// later reads, so once started the watcher is sticky.
//
// cb runs synchronously from the watcher goroutine; keep work small
// or hand off to your own goroutine.
func (m *membersAPI) Subscribe(cb func(space.MemberEvent)) (cancel func()) {
	if cb == nil {
		return func() {}
	}
	w := m.ensureWatcher()
	if w == nil {
		return func() {}
	}
	return w.subs.Add(cb)
}

// Query returns a chainable query builder over the materialised
// members collection. Starts the watcher (if not already running) so
// the collection is being kept up to date.
func (m *membersAPI) Query() space.Query {
	m.ensureWatcher()
	return newMembersQuery(m)
}

// applyProfile overlays the cached identityRepo profile (if any) on
// a single Member in place. No-op when the watcher hasn't been
// started or has no cached entry yet.
func (m *membersAPI) applyProfile(member *space.Member) {
	if member == nil {
		return
	}
	m.watcherMu.Lock()
	w := m.watcher
	m.watcherMu.Unlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	p, ok := w.profiles[member.Identity]
	w.mu.Unlock()
	if !ok {
		return
	}
	applyProfile(member, p)
}

// cachedProfile returns the watcher's cached identityRepo profile for an
// identity, if one has been fetched. Safe under the watcher lock.
func (w *memberWatcher) cachedProfile(identity string) (space.AccountMetadata, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.profiles[identity]
	return p, ok
}

// cacheProfile stores a freshly fetched profile in the watcher cache so
// later reads (member views and JoinRequests) reuse it instead of
// re-fetching. Marks the snapshot dirty so the next tick applies it.
func (w *memberWatcher) cacheProfile(identity string, p space.AccountMetadata) {
	w.mu.Lock()
	w.profiles[identity] = p
	w.profilesDirty = true
	w.mu.Unlock()
}

// applyProfiles is the slice equivalent of applyProfile.
func (m *membersAPI) applyProfiles(members []space.Member) {
	if len(members) == 0 {
		return
	}
	m.watcherMu.Lock()
	w := m.watcher
	m.watcherMu.Unlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range members {
		if p, ok := w.profiles[members[i].Identity]; ok {
			applyProfile(&members[i], p)
		}
	}
}

// ensureWatcher starts the per-space members watcher if not already
// running. Returns the watcher (or nil if construction failed —
// caller should treat as no-op).
func (m *membersAPI) ensureWatcher() *memberWatcher {
	// Fast path: this handle already resolved its watcher.
	m.watcherMu.Lock()
	if w := m.watcher; w != nil {
		m.watcherMu.Unlock()
		return w
	}
	m.watcherMu.Unlock()

	s := m.s.parent
	if s == nil {
		// No parent Service (test harnesses): fall back to a
		// handle-local watcher with no shared dedup.
		w, err := newMemberWatcher(context.Background(), m)
		if err != nil {
			return nil
		}
		m.setWatcher(w)
		return w
	}

	// Service-level singleton: reuse the per-space watcher if a prior
	// Get/Create/Derive handle already started it.
	s.mu.Lock()
	if w := s.memberWatchers[m.s.id]; w != nil {
		s.mu.Unlock()
		m.setWatcher(w)
		return w
	}
	s.mu.Unlock()

	// Construct outside the lock — newMemberWatcher opens the collection
	// and does the initial ACL seed/reconcile (any-store latency).
	w, err := newMemberWatcher(context.Background(), m)
	if err != nil {
		// If construction fails (e.g. db unavailable), surface lazily
		// — readers see an empty collection, subscribers see no events
		// until the next ensureWatcher call after the issue clears.
		return nil
	}
	s.mu.Lock()
	if existing := s.memberWatchers[m.s.id]; existing != nil {
		// Race: another handle wired concurrently. Drop the loser so we
		// don't leak its goroutines; its seed already ran and is harmless.
		// It also subscribed itself to the ACL kick mux inside
		// newMemberWatcher — detach it, or the stopped watcher (and its
		// snapshot/collection state) is retained by the mux for the
		// space's whole loaded lifetime.
		s.mu.Unlock()
		w.stop()
		s.aclKickRemove(m.s.id, w)
		w = existing
	} else {
		s.memberWatchers[m.s.id] = w
		s.watchers.register(w)
		s.mu.Unlock()
	}
	m.setWatcher(w)
	return w
}

// setWatcher caches the resolved shared watcher on this handle.
func (m *membersAPI) setWatcher(w *memberWatcher) {
	m.watcherMu.Lock()
	m.watcher = w
	m.watcherMu.Unlock()
}

// memberFromAccountState lifts one any-sync AccountState into the public
// Member shape. The ACL holds only a member's metadata symkey, not their
// name/icon — those are overlaid from the identityRepo profile cache by
// applyProfile.
func memberFromAccountState(acc list.AccountState) space.Member {
	return space.Member{
		Identity:   acc.PubKey.Account(),
		Permission: fromAclPermissions(acc.Permissions),
		Status:     fromAclStatus(acc.Status),
	}
}

// memberFromJoinRecord lifts a pending RequestRecord into a Member with
// Status=Joining. Name/icon come from the identityRepo overlay, not the
// record (which carries only the requester's metadata symkey).
func memberFromJoinRecord(r list.RequestRecord) space.Member {
	return space.Member{
		Identity:        r.RequestIdentity.Account(),
		Permission:      space.PermissionNone,
		Status:          space.MemberStatusJoining,
		RequestRecordId: r.RecordId,
	}
}

// syntheticOneToOneOwner returns the ACL's synthetic 1-1 owner key when
// the space is a 1-1, else nil. A 1-1 ACL carries a synthetic owner
// (sharedPk, derived via ECDH from both peers' account keys) purely to
// satisfy any-sync's "every ACL has an owner" invariant — nobody holds
// its private key and it has no identity profile. It is ignored in
// business logic (docs/one-to-one-spaces.md), so member views exclude
// it and surface only the two real writers. Best-effort: if the space is
// a 1-1 but the owner key can't be read, returns nil (no filtering) —
// the raw view is a strictly better failure than a panic. Caller holds
// the AclList read lock.
func syntheticOneToOneOwner(state *list.AclState) crypto.PubKey {
	if !state.IsOneToOne() {
		return nil
	}
	owner, err := state.OwnerPubKey()
	if err != nil {
		return nil
	}
	return owner
}

// collectMembers snapshots the ACL into the union view: active members
// from CurrentAccounts (including removed tombstones) plus pending join
// requests. The second return maps identity → metadata symkey string for
// every member/requester whose symkey we could decrypt — the caller
// caches these so each contact's profile resolves from identityRepo (and
// so the account's other devices learn the key). Caller holds the
// AclList read lock.
func collectMembers(acl list.AclList) ([]space.Member, map[string]string) {
	state := acl.AclState()
	keys := state.Keys()
	accounts := state.CurrentAccounts()
	out := make([]space.Member, 0, len(accounts))
	symKeys := make(map[string]string)
	syntheticOwner := syntheticOneToOneOwner(state)
	for _, acc := range accounts {
		if syntheticOwner != nil && acc.PubKey.Equals(syntheticOwner) {
			continue
		}
		out = append(out, memberFromAccountState(acc))
		if sk := decodeSymKeyMetadata(acc.RequestMetadata, keys, acc.KeyRecordId); sk != "" {
			symKeys[acc.PubKey.Account()] = sk
		}
	}
	// JoinRecords(true) decrypts the request metadata when the caller
	// holds the metadata key (owner / admin); the plaintext is the
	// requester's symkey. JoinRecords(false) leaves it ciphertext (no
	// symkey learned).
	reqs, err := state.JoinRecords(true)
	decrypted := err == nil
	if err != nil {
		reqs, _ = state.JoinRecords(false)
	}
	for _, r := range reqs {
		out = append(out, memberFromJoinRecord(r))
		if decrypted && len(r.RequestMetadata) > 0 {
			symKeys[r.RequestIdentity.Account()] = string(r.RequestMetadata)
		}
	}
	return out, symKeys
}

// collectJoinRequests is the projection over collectMembers limited to
// the Status=Joining rows. The second return maps requester identity →
// metadata symkey string (from the decrypted join record) so the caller
// can resolve each requester's name/icon from identityRepo — the record
// itself no longer carries name/icon.
func collectJoinRequests(acl list.AclList) ([]space.JoinRequestInfo, map[string]string) {
	reqs, err := acl.AclState().JoinRecords(true)
	decrypted := err == nil
	if err != nil {
		reqs, _ = acl.AclState().JoinRecords(false)
	}
	out := make([]space.JoinRequestInfo, 0, len(reqs))
	symKeys := make(map[string]string)
	for _, r := range reqs {
		id := r.RequestIdentity.Account()
		out = append(out, space.JoinRequestInfo{
			RecordId: r.RecordId,
			Identity: id,
		})
		if decrypted && len(r.RequestMetadata) > 0 {
			symKeys[id] = string(r.RequestMetadata)
		}
	}
	return out, symKeys
}

func fromAclStatus(s list.AclStatus) space.MemberStatus {
	switch s {
	case list.StatusJoining:
		return space.MemberStatusJoining
	case list.StatusActive:
		return space.MemberStatusActive
	case list.StatusRemoved:
		return space.MemberStatusRemoved
	case list.StatusDeclined:
		return space.MemberStatusDeclined
	case list.StatusRemoving:
		return space.MemberStatusRemoving
	case list.StatusCanceled:
		return space.MemberStatusCanceled
	default:
		return space.MemberStatusUnknown
	}
}

// memberWatcher is the per-space polling loop. It keeps three things
// in sync:
//
//  1. An in-memory snapshot of the membership view (identity → Member),
//     used to diff against the next AclList state and emit events.
//  2. The set of Subscribe callbacks (firehose).
//  3. The on-disk materialised members collection (drives Query).
//
// Started on the first Subscribe or Query (via membersAPI.ensureWatcher).
// Runs until Service.Close.
type memberWatcher struct {
	api  *membersAPI
	coll anystore.Collection

	subs fanout.Registry[space.MemberEvent]

	mu       sync.Mutex
	headId   string
	snapshot map[string]space.Member

	// profiles caches the latest identityRepo profile per member,
	// keyed by identity (strkey account-address form). Applied as overrides when
	// rebuilding the snapshot from AclList state. Updated by the
	// identityRepo fetcher loop. Empty fields don't override.
	profiles map[string]space.AccountMetadata
	// profilesDirty is set by the fetcher when profiles change so the
	// next regular tick rebuilds the snapshot even when the ACL head
	// hasn't moved.
	profilesDirty bool

	// kickCh is signalled by UpdateAcl (called by syncacl on every ACL
	// record add — local OR pushed from peers) so the watcher tick fires
	// immediately rather than waiting up to memberPollInterval. Buffered
	// 1 so coalesced kicks don't block the syncacl write path.
	kickCh chan struct{}

	// fetchAll / fetchIds (under mu) queue profile fetches for
	// profileLoop, signalled through fetchCh (buffered 1, coalescing).
	// Every identityRepo round-trip runs on that one joined goroutine.
	fetchAll bool
	fetchIds map[string]struct{}
	fetchCh  chan struct{}

	// ctx bounds the loops' coordinator calls and store writes; stop
	// cancels it, so a hung round-trip can't hold stop.
	ctx    context.Context
	cancel context.CancelFunc
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newMemberWatcher(ctx context.Context, api *membersAPI) (*memberWatcher, error) {
	coll, err := api.collection(ctx)
	if err != nil {
		return nil, err
	}
	w := &memberWatcher{
		api:      api,
		coll:     coll,
		snapshot: make(map[string]space.Member),
		profiles: make(map[string]space.AccountMetadata),
		kickCh:   make(chan struct{}, 1),
		fetchIds: make(map[string]struct{}),
		fetchCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	// Seed the snapshot AND reconcile the disk collection synchronously
	// so the first tick after start doesn't fire spurious "added"
	// events for the existing set, and Query reads land fresh data
	// even before the first periodic tick.
	if acl, err := api.aclList(ctx); err == nil {
		acl.RLock()
		w.headId = acl.Head().Id
		members, symKeys := collectMembers(acl)
		acl.RUnlock()
		for _, m := range members {
			w.snapshot[m.Identity] = m
		}
		_ = w.reconcileCollection(ctx, nil, w.snapshot)
		// Seed the identities directory for members present at watcher
		// start. The first tick will early-return (head unchanged), so this
		// work won't otherwise run for them: cache their symkeys and record
		// the space sighting. Their profiles are then resolved by the
		// profileLoop's initial run, which now finds the keys cached.
		for id, sk := range symKeys {
			_ = w.api.s.tsp.SetIdentityMetaKey(ctx, id, sk)
		}
		for id := range w.snapshot {
			_ = w.api.s.tsp.AddIdentitySpace(ctx, id, w.api.s.id)
		}
		// Register for ACL kicks so we tick immediately on every record
		// add. syncacl has a SINGLE AclUpdater slot (last SetAclUpdater
		// wins), shared with the ACL mirror watcher — go through the
		// Service's per-space fan-out when there is one; fall back to
		// claiming the slot directly for parentless test harnesses. The
		// cast inside aclKickFanout is safe — commonspace.Space.Acl()
		// returns syncacl.SyncAcl, and our aclList() forwards that.
		if svc := api.s.parent; svc != nil {
			svc.aclKickFanout(api.s.id, acl).add(w)
		} else if su, ok := acl.(syncacl.SyncAcl); ok {
			su.SetAclUpdater(w)
		}
	}
	w.wg.Add(2)
	go w.loop()
	go w.profileLoop()
	return w, nil
}

func (w *memberWatcher) spaceID() string { return w.api.s.id }

func (w *memberWatcher) stop() {
	select {
	case <-w.stopCh:
		// already stopped
	default:
		close(w.stopCh)
		w.cancel()
	}
	w.wg.Wait()
}

func (w *memberWatcher) loop() {
	defer w.wg.Done()
	t := time.NewTicker(memberPollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			w.tick()
		case <-w.kickCh:
			w.tick()
		}
	}
}

// UpdateAcl satisfies headupdater.AclUpdater. Called by syncacl
// synchronously after every ACL record add — local AcceptRequest /
// CreateInvite as well as pushed records from peers. We coalesce by
// buffered channel so the syncacl write path stays non-blocking.
//
// Without this hook the watcher only ticks on memberPollInterval, so a
// fast local stack can collapse a join+accept into a single observed
// head and emit only Added(Active) — losing the intermediate
// Added(Joining) event subscribers care about.
func (w *memberWatcher) UpdateAcl(_ list.AclList) {
	select {
	case w.kickCh <- struct{}{}:
	default:
	}
}

func (w *memberWatcher) tick() {
	ctx := w.ctx
	acl, err := w.api.aclList(ctx)
	if err != nil {
		return
	}
	acl.RLock()
	head := acl.Head().Id
	w.mu.Lock()
	headChanged := head != w.headId
	dirty := w.profilesDirty
	w.mu.Unlock()
	// Skip the rest of the tick when nothing changed — the
	// profile-loop sets profilesDirty when it pulls fresh data so
	// snapshots get rebuilt with new overrides even if AclList head
	// hasn't moved.
	if !headChanged && !dirty {
		acl.RUnlock()
		return
	}
	current, symKeys := collectMembers(acl)
	meActive := selfAclActive(acl.AclState())
	acl.RUnlock()

	// Self-heal the tech-space row when the owner has accepted our join
	// but the row still says joining or ended (the join controller's
	// flip lost a race, or a legacy device-local marker never flipped),
	// so Service.List / Space.Info stop reporting a member's space as
	// pending while members.Me already reads "active" off the live
	// AclList. The watcher ticks on every ACL record add via
	// SetAclUpdater, so the flip lands promptly after the owner's accept
	// replicates.
	if meActive {
		w.maybeFlipTechSpaceJoining(ctx)
	}

	// Cache each member's metadata symkey into the synced account-scoped
	// store before fetching profiles, so the decrypt path (and the
	// account's other devices) can resolve their identityRepo profile.
	// SetIdentityMetaKey no-ops on an unchanged value. Track identities
	// whose key is NEW so we fetch their profile right away — a member
	// present since watcher start (e.g. the owner, from a fresh joiner's
	// view) isn't a "newcomer", so without this its profile would wait up
	// to identityRepoPollInterval.
	var newKeyIds []string
	for id, sk := range symKeys {
		if cur, ok := w.api.s.tsp.GetIdentityMetaKey(ctx, id); !ok || cur != sk {
			newKeyIds = append(newKeyIds, id)
		}
		_ = w.api.s.tsp.SetIdentityMetaKey(ctx, id, sk)
	}

	w.mu.Lock()
	prev := w.snapshot
	next := make(map[string]space.Member, len(current))
	for _, m := range current {
		applyProfile(&m, w.profiles[m.Identity])
		next[m.Identity] = m
	}
	w.headId = head
	w.profilesDirty = false
	w.snapshot = next
	w.mu.Unlock()

	// Reconcile the on-disk collection first so any Query call that
	// runs concurrently with a tick sees state at-least-as-fresh as
	// the events firing now. Failures are logged-and-continued — the
	// in-memory firehose is independent. Not cancellable: headId has
	// advanced, so a delete dropped by a stop mid-tick is never retried
	// (the restart seed only upserts). Local writes, so stop stays fast.
	_ = w.reconcileCollection(context.WithoutCancel(ctx), prev, next)

	// Emit add / change events for the new view. Track newcomers so we
	// can pull their identityRepo profile right away — without this the
	// join-time fallback name lingers until the next slow profileLoop
	// tick (up to identityRepoPollInterval).
	var newcomers []string
	for id, m := range next {
		old, existed := prev[id]
		if !existed {
			newcomers = append(newcomers, id)
			w.subs.Dispatch(space.MemberEvent{
				Kind:   space.MemberEventAdded,
				Member: m,
			})
			continue
		}
		if old != m {
			oldCopy := old
			w.subs.Dispatch(space.MemberEvent{
				Kind:     space.MemberEventChanged,
				Member:   m,
				Previous: &oldCopy,
			})
		}
	}
	// On a membership change, record the space sighting for every current
	// member in the identities directory (not just newcomers — a member
	// present since watcher start, e.g. the owner from a fresh joiner's
	// view, is never a newcomer). AddIdentitySpace no-ops when the sighting
	// already exists; gated on headChanged so profile-only ticks skip it.
	if headChanged {
		spaceID := w.spaceID()
		for id := range next {
			_ = w.api.s.tsp.AddIdentitySpace(ctx, id, spaceID)
		}
	}
	// Fetch profiles for both newcomers and members whose symkey just
	// became available (deduped).
	if fetch := dedupStrings(newcomers, newKeyIds); len(fetch) > 0 {
		w.requestProfiles(fetch)
	}
	// Emit remove events for identities that disappeared.
	for id, old := range prev {
		if _, stillThere := next[id]; stillThere {
			continue
		}
		// Prune the sighting — this member left the space.
		_ = w.api.s.tsp.RemoveIdentitySpace(ctx, id, w.spaceID())
		oldCopy := old
		w.subs.Dispatch(space.MemberEvent{
			Kind:     space.MemberEventRemoved,
			Member:   old,
			Previous: &oldCopy,
		})
	}
}

// maybeFlipTechSpaceJoining flips the space-index row to active if (and
// only if) it still reads joining or ended — Service.healJoinMembership,
// which reads the current value first so a settled row never burns a
// CRDT change per tick.
//
// Best-effort: any failure (tsp not open, write rejected) is dropped;
// the next tick retries.
func (w *memberWatcher) maybeFlipTechSpaceJoining(ctx context.Context) {
	if w.api.s.tsp == nil || w.api.s.parent == nil {
		return
	}
	w.api.s.parent.healJoinMembership(ctx, w.api.s.id)
}

// reconcileCollection writes the new snapshot to disk: Upsert every
// row in `next`, DeleteId every identity present in `prev` but not
// `next`. Atomic per-row; if any single op fails the rest still try.
//
// On the seed call (constructor) prev is nil — every member in next
// is a fresh upsert. The "compare for equality" optimisation isn't
// done here: any-store Upsert is cheap enough that always-write keeps
// the code simpler than tracking dirty rows.
func (w *memberWatcher) reconcileCollection(
	ctx context.Context,
	prev, next map[string]space.Member,
) error {
	if w.coll == nil {
		return errors.New("members: collection unavailable")
	}
	arena := &anyenc.Arena{}
	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Upserts.
	for id, m := range next {
		// Skip writes for unchanged rows when prev is non-nil — same
		// data on disk, no need to thrash.
		if prev != nil {
			if old, existed := prev[id]; existed && old == m {
				continue
			}
		}
		arena.Reset()
		val := encodeMember(arena, m)
		if err := w.coll.UpsertOne(ctx, val); err != nil {
			keep(fmt.Errorf("members: upsert %s: %w", id, err))
		}
	}
	// Deletions: identities that vanished from next.
	for id := range prev {
		if _, stillThere := next[id]; stillThere {
			continue
		}
		if err := w.coll.DeleteId(ctx, id); err != nil {
			keep(fmt.Errorf("members: delete %s: %w", id, err))
		}
	}
	return firstErr
}

// applyProfile overrides the metadata fields on m with the
// identityRepo profile values when they're non-empty. Empty fields
// keep the join-time fallback. Mutates m in place.
func applyProfile(m *space.Member, p space.AccountMetadata) {
	if p.Name != "" {
		m.Name = p.Name
	}
	if p.Description != "" {
		m.Description = p.Description
	}
	if p.IconCID != "" {
		m.IconCID = p.IconCID
	}
}

// profileLoop is the background goroutine that pulls every member's
// identityRepo profile from the coordinator. Runs once immediately
// after watcher start, then every identityRepoPollInterval.
func (w *memberWatcher) profileLoop() {
	defer w.wg.Done()
	// Seed run — pick up any profile that was published before we
	// started watching this space.
	w.fetchProfilesOnce(w.ctx)
	t := time.NewTicker(identityRepoPollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			w.fetchProfilesOnce(w.ctx)
		case <-w.fetchCh:
			all, ids := w.takeProfileRequests()
			if all {
				w.fetchProfilesOnce(w.ctx)
			} else {
				w.fetchProfilesFor(w.ctx, ids)
			}
		}
	}
}

// requestProfiles queues an identityRepo fetch on profileLoop: the
// given identities, or every current member when ids is nil. Never
// blocks — callers include the ACL tick and Account.UpdateMetadata.
func (w *memberWatcher) requestProfiles(ids []string) {
	w.mu.Lock()
	if ids == nil {
		w.fetchAll = true
	}
	for _, id := range ids {
		w.fetchIds[id] = struct{}{}
	}
	w.mu.Unlock()
	select {
	case w.fetchCh <- struct{}{}:
	default:
	}
}

// takeProfileRequests drains the queue. With all set the ids are moot:
// the tick publishes its snapshot before it requests, so a full fetch
// covers them.
func (w *memberWatcher) takeProfileRequests() (all bool, ids []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	all = w.fetchAll
	ids = make([]string, 0, len(w.fetchIds))
	for id := range w.fetchIds {
		ids = append(ids, id)
	}
	w.fetchAll = false
	w.fetchIds = make(map[string]struct{})
	return all, ids
}

// fetchProfilesOnce pulls every current member's identityRepo profile
// in one DRPC call, verifies signatures, decodes the bytes, and
// merges into w.profiles (keyed on the strkey account-address identity).
// Sets profilesDirty when anything changed so the next tick rebuilds
// the snapshot.
func (w *memberWatcher) fetchProfilesOnce(ctx context.Context) {
	w.mu.Lock()
	identities := make([]string, 0, len(w.snapshot))
	for id := range w.snapshot {
		identities = append(identities, id)
	}
	w.mu.Unlock()
	w.fetchProfilesFor(ctx, identities)
}

// fetchProfilesFor pulls identityRepo profiles for the given identities.
// Same merge semantics as fetchProfilesOnce — sets profilesDirty so the
// next tick rebuilds the snapshot. Used to flip a newly-added member's
// fallback display name to the live profile without waiting up to
// identityRepoPollInterval.
//
// Member identities are already in the strkey account-address form
// identityRepo expects, so the request needs no translation. We only
// decode each address back to a PubKey to verify the record signature.
func (w *memberWatcher) fetchProfilesFor(ctx context.Context, identities []string) {
	app := w.api.s.app
	if app == nil || app.Coordinator() == nil {
		return
	}
	if len(identities) == 0 {
		return
	}
	pubKeys := make(map[string]crypto.PubKey, len(identities))
	for _, id := range identities {
		pk, err := crypto.DecodeAccountAddress(id)
		if err != nil {
			continue
		}
		pubKeys[id] = pk
	}
	if len(pubKeys) == 0 {
		return
	}

	res := w.api.s.parent.identityRepoGet(ctx, identities)
	if len(res) == 0 {
		return
	}

	// Resolve each contact's decryption symkey before taking the lock —
	// the lookup reads the tech-space cache and shouldn't run under w.mu.
	symKeys := make(map[string]crypto.SymKey, len(res))
	for _, dwi := range res {
		symKeys[dwi.Identity] = w.api.s.parent.metadataSymKeyFor(ctx, dwi.Identity)
	}

	w.mu.Lock()
	changed := false
	resolved := make(map[string]space.AccountMetadata)
	for _, dwi := range res {
		pk, ok := pubKeys[dwi.Identity]
		if !ok {
			continue
		}
		profile, ok := decodeIdentityRepoProfile(dwi.Data, pk, symKeys[dwi.Identity])
		if !ok {
			continue
		}
		prev := w.profiles[dwi.Identity]
		if prev != profile {
			w.profiles[dwi.Identity] = profile
			changed = true
			resolved[dwi.Identity] = profile
		}
	}
	if changed {
		w.profilesDirty = true
	}
	w.mu.Unlock()

	// Write changed profiles through to the account-global identities
	// directory (device-local) so the public Identities API and 1-1 list
	// rows see them too. Outside the lock — these are tech-space writes.
	for id, p := range resolved {
		_ = w.api.s.tsp.SetIdentityProfile(ctx, id, p.Name, p.Description, p.IconCID)
	}
}

// decodeIdentityRepoProfile finds the SDK-kind record in data, verifies
// its signature against pk (over the ciphertext), and decrypts the bytes
// with key. Returns (zero, false) on missing record, bad signature, a nil
// key (we don't hold this contact's symkey yet), or decrypt failure.
func decodeIdentityRepoProfile(data []*identityrepoproto.Data, pk crypto.PubKey, key crypto.SymKey) (space.AccountMetadata, bool) {
	for _, d := range data {
		if d == nil || d.Kind != space.IdentityProfileKind {
			continue
		}
		ok, err := pk.Verify(d.Data, d.Signature)
		if err != nil || !ok {
			return space.AccountMetadata{}, false
		}
		return space.DecryptProfile(d.Data, key)
	}
	return space.AccountMetadata{}, false
}

// metadataSymKeyFor resolves the symkey that decrypts identity's
// identityRepo profile: our own is derivable from the account key; a
// contact's comes from the synced account-scoped identityMetaKeys cache
// (populated from a shared space's ACL or a 1-1 invite). Returns nil when
// we don't hold the contact's key yet — the profile then stays
// unresolved until the key arrives.
func (s *Service) metadataSymKeyFor(ctx context.Context, identity string) crypto.SymKey {
	keys := s.app.AccountKeys()
	if keys != nil && identity == keys.SignKey.GetPublic().Account() {
		k, err := space.DeriveAccountMetadataSymKey(keys.SignKey)
		if err != nil {
			return nil
		}
		return k
	}
	enc, ok := s.tsp.GetIdentityMetaKey(ctx, identity)
	if !ok {
		return nil
	}
	k, err := space.UnmarshalSymKey(enc)
	if err != nil {
		return nil
	}
	return k
}

// fetchIdentityProfile pulls one identity's identityRepo profile from the
// coordinator, verifies it, and decrypts it with key. Returns
// (zero, false) when the coordinator is absent/offline, the record is
// missing, or we don't hold the decryption key. Shared by the per-space
// member fetcher and the pending-1-1 name resolver.
func (s *Service) fetchIdentityProfile(ctx context.Context, identity string, key crypto.SymKey) (space.AccountMetadata, bool) {
	if s.app == nil || s.app.Coordinator() == nil || key == nil {
		return space.AccountMetadata{}, false
	}
	pk, err := crypto.DecodeAccountAddress(identity)
	if err != nil {
		return space.AccountMetadata{}, false
	}
	res, err := s.app.Coordinator().IdentityRepoGet(ctx, []string{identity}, []string{space.IdentityProfileKind})
	if err != nil {
		return space.AccountMetadata{}, false
	}
	for _, dwi := range res {
		if dwi.Identity != identity {
			continue
		}
		return decodeIdentityRepoProfile(dwi.Data, pk, key)
	}
	return space.AccountMetadata{}, false
}

// identityRepoMaxBatch is the coordinator's per-request identity cap for
// identityRepo Pull (ErrThresholdReached beyond it). Larger requests are
// split into chunks.
const identityRepoMaxBatch = 350

// identityRepoGet fetches identityRepo profiles for many identities,
// chunked to the coordinator's per-request cap. Best-effort per chunk: a
// failing chunk is skipped, not fatal to the rest.
func (s *Service) identityRepoGet(ctx context.Context, identities []string) []*identityrepoproto.DataWithIdentity {
	if s.app == nil || s.app.Coordinator() == nil {
		return nil
	}
	var out []*identityrepoproto.DataWithIdentity
	for start := 0; start < len(identities); start += identityRepoMaxBatch {
		end := start + identityRepoMaxBatch
		if end > len(identities) {
			end = len(identities)
		}
		res, err := s.app.Coordinator().IdentityRepoGet(ctx, identities[start:end], []string{space.IdentityProfileKind})
		if err != nil {
			continue
		}
		out = append(out, res...)
	}
	return out
}

// ResolveIdentityProfiles batch-resolves every directory identity that
// has a synced symkey but no locally-cached profile yet — the cold-sync
// case: a fresh device receives many symkeys over tech-space sync but
// holds no profiles (those are device-local). One IdentityRepoGet covers
// the whole set instead of one call per identity. Best-effort; run in a
// goroutine on boot.
func (s *Service) ResolveIdentityProfiles(ctx context.Context) {
	if s.app == nil || s.app.Coordinator() == nil {
		return
	}
	type pending struct {
		key crypto.SymKey
		pk  crypto.PubKey
	}
	todo := make(map[string]pending)
	ids := make([]string, 0)
	for _, r := range s.tsp.ListIdentities(ctx) {
		if r.SymKey == "" || r.Name != "" {
			continue // no key, or already resolved
		}
		key, err := space.UnmarshalSymKey(r.SymKey)
		if err != nil {
			continue
		}
		pk, err := crypto.DecodeAccountAddress(r.Identity)
		if err != nil {
			continue
		}
		todo[r.Identity] = pending{key: key, pk: pk}
		ids = append(ids, r.Identity)
	}
	if len(ids) == 0 {
		return
	}
	for _, dwi := range s.identityRepoGet(ctx, ids) {
		p, ok := todo[dwi.Identity]
		if !ok {
			continue
		}
		prof, ok := decodeIdentityRepoProfile(dwi.Data, p.pk, p.key)
		if !ok || (prof.Name == "" && prof.Description == "" && prof.IconCID == "") {
			continue
		}
		_ = s.tsp.SetIdentityProfile(ctx, dwi.Identity, prof.Name, prof.Description, prof.IconCID)
	}
}
