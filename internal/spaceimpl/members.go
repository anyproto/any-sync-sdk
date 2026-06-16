package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/util/crypto"

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
	members := collectMembers(acl)
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
	keys := state.Keys()
	var found *space.Member
	for _, acc := range state.CurrentAccounts() {
		if acc.PubKey.Equals(pk) {
			val := memberFromAccountState(acc, keys)
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
	keys := state.Keys()
	me := state.Identity()
	var found *space.Member
	for _, acc := range state.CurrentAccounts() {
		if acc.PubKey.Equals(me) {
			val := memberFromAccountState(acc, keys)
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
	defer acl.RUnlock()
	return collectJoinRequests(acl), nil
}

func (m *membersAPI) Invites(ctx context.Context) ([]space.InviteInfo, error) {
	acl, err := m.aclList(ctx)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	defer acl.RUnlock()
	invites := acl.AclState().Invites()
	out := make([]space.InviteInfo, 0, len(invites))
	for _, inv := range invites {
		out = append(out, space.InviteInfo{
			RecordId:   inv.Id,
			Permission: fromAclPermissions(inv.Permissions),
		})
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
	id := w.add(cb)
	return func() {
		w.remove(id)
	}
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
		s.mu.Unlock()
		w.stop()
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

// memberFromAccountState lifts one any-sync AccountState into the
// public Member shape, decrypting the request metadata using the
// keys from the keyRecordId on the account state. Decryption falls
// back to raw bytes if the caller doesn't hold the matching key
// (e.g. a non-admin reading another member's record) — those
// bytes won't decode into our flat AccountMetadata format, so the
// fields stay empty.
func memberFromAccountState(acc list.AccountState, keys map[string]list.AclKeys) space.Member {
	md := decodeAccountMetadata(acc.RequestMetadata, keys, acc.KeyRecordId)
	return space.Member{
		Identity:    acc.PubKey.Account(),
		Permission:  fromAclPermissions(acc.Permissions),
		Status:      fromAclStatus(acc.Status),
		Name:        md.Name,
		Description: md.Description,
		IconCID:     md.IconCID,
	}
}

// memberFromJoinRecord lifts a pending RequestRecord into a Member
// with Status=Joining. JoinRecords(true) on the AclState already
// decrypts in-place when the caller has the metadata key, so the
// bytes here are plaintext on the owner/admin side.
func memberFromJoinRecord(r list.RequestRecord) space.Member {
	md := decodeMetadata(r.RequestMetadata)
	return space.Member{
		Identity:        r.RequestIdentity.Account(),
		Permission:      space.PermissionNone,
		Status:          space.MemberStatusJoining,
		Name:            md.Name,
		Description:     md.Description,
		IconCID:         md.IconCID,
		RequestRecordId: r.RecordId,
	}
}

// collectMembers snapshots the ACL into the union view: active
// members from CurrentAccounts (including removed tombstones) plus
// pending join requests. Caller holds the AclList read lock.
func collectMembers(acl list.AclList) []space.Member {
	state := acl.AclState()
	keys := state.Keys()
	accounts := state.CurrentAccounts()
	out := make([]space.Member, 0, len(accounts))
	for _, acc := range accounts {
		out = append(out, memberFromAccountState(acc, keys))
	}
	// JoinRecords(true) decrypts metadata when the caller has the
	// metadata key (owner / admin); fall back to ciphertext otherwise.
	reqs, err := state.JoinRecords(true)
	if err != nil {
		reqs, _ = state.JoinRecords(false)
	}
	for _, r := range reqs {
		out = append(out, memberFromJoinRecord(r))
	}
	return out
}

// collectJoinRequests is the projection over collectMembers limited
// to the Status=Joining rows.
func collectJoinRequests(acl list.AclList) []space.JoinRequestInfo {
	reqs, err := acl.AclState().JoinRecords(true)
	if err != nil {
		reqs, _ = acl.AclState().JoinRecords(false)
	}
	out := make([]space.JoinRequestInfo, 0, len(reqs))
	for _, r := range reqs {
		md := decodeMetadata(r.RequestMetadata)
		out = append(out, space.JoinRequestInfo{
			RecordId:    r.RecordId,
			Identity:    r.RequestIdentity.Account(),
			Name:        md.Name,
			Description: md.Description,
			IconCID:     md.IconCID,
		})
	}
	return out
}

// decodeAccountMetadata decrypts the request-metadata blob attached
// to an account state, if the caller holds the metadata key for the
// matching keyRecordId. Falls back silently to empty metadata when
// decryption fails — readers without the key see no name/icon, which
// is correct (they shouldn't be able to anyway).
func decodeAccountMetadata(raw []byte, keys map[string]list.AclKeys, keyRecordId string) space.AccountMetadata {
	if len(raw) == 0 {
		return space.AccountMetadata{}
	}
	k, ok := keys[keyRecordId]
	if !ok || k.MetadataPrivKey == nil {
		return space.AccountMetadata{}
	}
	plain, err := k.MetadataPrivKey.Decrypt(raw)
	if err != nil {
		return space.AccountMetadata{}
	}
	return decodeMetadata(plain)
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
// Runs until Service.Close. Subscribers are stored in a map keyed by
// an int counter for O(1) remove.
type memberWatcher struct {
	api  *membersAPI
	coll anystore.Collection

	mu       sync.Mutex
	subs     map[int]func(space.MemberEvent)
	nextID   int
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
		subs:     make(map[int]func(space.MemberEvent)),
		snapshot: make(map[string]space.Member),
		profiles: make(map[string]space.AccountMetadata),
		kickCh:   make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
	}
	// Seed the snapshot AND reconcile the disk collection synchronously
	// so the first tick after start doesn't fire spurious "added"
	// events for the existing set, and Query reads land fresh data
	// even before the first periodic tick.
	if acl, err := api.aclList(ctx); err == nil {
		acl.RLock()
		w.headId = acl.Head().Id
		members := collectMembers(acl)
		acl.RUnlock()
		for _, m := range members {
			w.snapshot[m.Identity] = m
		}
		_ = w.reconcileCollection(ctx, nil, w.snapshot)
		// Register as the syncacl AclUpdater so we tick immediately on
		// every record add. The cast is safe — commonspace.Space.Acl()
		// returns syncacl.SyncAcl, and our aclList() forwards that.
		if su, ok := acl.(syncacl.SyncAcl); ok {
			su.SetAclUpdater(w)
		}
	}
	w.wg.Add(2)
	go w.loop()
	go w.profileLoop()
	return w, nil
}

// add registers cb and returns its slot id.
func (w *memberWatcher) add(cb func(space.MemberEvent)) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := w.nextID
	w.nextID++
	w.subs[id] = cb
	return id
}

// remove detaches the subscriber. The watcher does NOT stop on the
// last remove — it stays running until SDK shutdown so subsequent
// Query calls keep seeing fresh data.
func (w *memberWatcher) remove(id int) {
	w.mu.Lock()
	delete(w.subs, id)
	w.mu.Unlock()
}

func (w *memberWatcher) spaceID() string { return w.api.s.id }

func (w *memberWatcher) stop() {
	select {
	case <-w.stopCh:
		// already stopped
	default:
		close(w.stopCh)
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
	ctx := context.Background()
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
	current := collectMembers(acl)
	state := acl.AclState()
	meActive := false
	if me := state.Identity(); me != nil {
		for _, acc := range state.CurrentAccounts() {
			if acc.PubKey.Equals(me) {
				meActive = acc.Status == list.StatusActive
				break
			}
		}
	}
	acl.RUnlock()

	// Self-heal the tech-space LocalStatus when the owner has accepted
	// our join. Without this the index keeps LocalStatus="joining"
	// forever (Service.Join wrote it; nothing else flips it), so
	// Service.List / Space.Info return Status=Joining while members.Me
	// already reads "active" off the live AclList. The watcher ticks
	// on every ACL record add via SetAclUpdater, so the flip lands
	// promptly after the owner's accept replicates.
	if meActive {
		w.maybeFlipTechSpaceJoining(ctx)
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
	subs := make([]func(space.MemberEvent), 0, len(w.subs))
	for _, cb := range w.subs {
		subs = append(subs, cb)
	}
	w.mu.Unlock()

	// Reconcile the on-disk collection first so any Query call that
	// runs concurrently with a tick sees state at-least-as-fresh as
	// the events firing now. Failures are logged-and-continued — the
	// in-memory firehose is independent.
	_ = w.reconcileCollection(ctx, prev, next)

	// Emit add / change events for the new view. Track newcomers so we
	// can pull their identityRepo profile right away — without this the
	// join-time fallback name lingers until the next slow profileLoop
	// tick (up to identityRepoPollInterval).
	var newcomers []string
	for id, m := range next {
		old, existed := prev[id]
		if !existed {
			newcomers = append(newcomers, id)
			fanout(subs, space.MemberEvent{
				Kind:   space.MemberEventAdded,
				Member: m,
			})
			continue
		}
		if old != m {
			oldCopy := old
			fanout(subs, space.MemberEvent{
				Kind:     space.MemberEventChanged,
				Member:   m,
				Previous: &oldCopy,
			})
		}
	}
	if len(newcomers) > 0 {
		go w.fetchProfilesFor(context.Background(), newcomers)
	}
	// Emit remove events for identities that disappeared.
	for id, old := range prev {
		if _, stillThere := next[id]; stillThere {
			continue
		}
		oldCopy := old
		fanout(subs, space.MemberEvent{
			Kind:     space.MemberEventRemoved,
			Member:   old,
			Previous: &oldCopy,
		})
	}
}

// maybeFlipTechSpaceJoining writes LocalStatus="active" to the
// space-index record if (and only if) the cached value is still
// "joining". Cheap no-op once the row reaches "active" — we read the
// current value first to avoid burning a CRDT change every tick.
//
// Best-effort: any failure (tsp not open, write rejected) is dropped;
// the next tick retries.
func (w *memberWatcher) maybeFlipTechSpaceJoining(ctx context.Context) {
	tsp := w.api.s.tsp
	if tsp == nil {
		return
	}
	rec, ok := tsp.Get(ctx, w.api.s.id)
	if !ok {
		return
	}
	if rec.LocalStatus != joiningLocalStatus {
		return
	}
	if _, err := tsp.SetLocalStatus(ctx, w.api.s.id, techspace.StatusActive); err != nil {
		// Swallow; transient failures heal on the next tick.
		_ = err
	}
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

// fanout invokes each subscriber synchronously. A panic in one cb
// would propagate; subscribers are expected to be well-behaved
// (do small work or hand off). Wrapping in recover felt heavier
// than the cost of a misbehaving caller learning fast.
func fanout(subs []func(space.MemberEvent), ev space.MemberEvent) {
	for _, cb := range subs {
		cb(ev)
	}
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
	w.fetchProfilesOnce(context.Background())
	t := time.NewTicker(identityRepoPollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			w.fetchProfilesOnce(context.Background())
		}
	}
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

	res, err := app.Coordinator().IdentityRepoGet(ctx, identities, []string{space.IdentityProfileKind})
	if err != nil {
		// Network errors are best-effort; the next tick will retry.
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	changed := false
	for _, dwi := range res {
		pk, ok := pubKeys[dwi.Identity]
		if !ok {
			continue
		}
		profile, ok := decodeIdentityRepoProfile(dwi.Data, pk)
		if !ok {
			continue
		}
		prev := w.profiles[dwi.Identity]
		if prev != profile {
			w.profiles[dwi.Identity] = profile
			changed = true
		}
	}
	if changed {
		w.profilesDirty = true
	}
}

// decodeIdentityRepoProfile finds the SDK-kind record in data,
// verifies its signature against pk, and decodes the bytes. Returns
// (zero, false) on missing record, bad signature, or parse failure.
func decodeIdentityRepoProfile(data []*identityrepoproto.Data, pk crypto.PubKey) (space.AccountMetadata, bool) {
	for _, d := range data {
		if d == nil || d.Kind != space.IdentityProfileKind {
			continue
		}
		ok, err := pk.Verify(d.Data, d.Signature)
		if err != nil || !ok {
			return space.AccountMetadata{}, false
		}
		return space.DecodeAccountMetadata(d.Data), true
	}
	return space.AccountMetadata{}, false
}
