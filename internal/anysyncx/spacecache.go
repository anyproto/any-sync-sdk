package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
)

// Space cache TTL is disabled: once a commonspace.Space is loaded it
// stays loaded for the SDK lifetime. Loaded spaces own only headsync
// + syncacl + a small set of tree handles per active object — the
// per-space memory floor is bounded and well below what auto-eviction
// was designed to claw back. Keeping spaces resident lets headsync /
// syncacl stay subscribed at all times so a peer that wakes up offline
// receives ACL updates and pushed changes without anyone touching the
// space first. Eviction also broke the inverse case: if no caller
// referenced a space for spaceLoaderTTL after boot, periodic headsync
// never fired at all (the per-space components only start on first
// app.GetSpace), so an idle peer would silently fall behind.
//
// Passing 0 to either WithTTL or WithGCPeriod disables the ocache
// ticker entirely (see app/ocache/ocache.go: `if c.ttl != 0 && c.gc != 0`).
const (
	spaceLoaderTTL = 0
	spaceLoaderGC  = 0
)

// SpaceHandle is the handle returned by App.GetSpace. Callers operate
// on it for the duration of a logical operation; the underlying
// commonspace.Space is reachable via Inner. The cache TTL releases
// idle spaces in the background — callers don't refcount manually.
type SpaceHandle interface {
	Id() string
	Inner() commonspace.Space
	// SyncHeads forces an immediate head-sync (diff) round on this
	// space, rather than waiting for the periodic timer. Blocks until
	// the round completes and returns its error verbatim.
	SyncHeads(ctx context.Context) error
}

// spaceWrapper implements ocache.Object and SpaceHandle. Holds the
// commonspace.Space and tags itself for sync-handler register /
// unregister on load / close.
type spaceWrapper struct {
	id  string
	cs  commonspace.Space
	app *App
}

func (s *spaceWrapper) Id() string                  { return s.id }
func (s *spaceWrapper) Inner() commonspace.Space    { return s.cs }

func (s *spaceWrapper) SyncHeads(ctx context.Context) error { return s.cs.SyncHeads(ctx) }

// Close unregisters this space from the inbound sync handler before
// closing the commonspace. Called by ocache on Remove/shutdown and
// after a successful TryClose.
func (s *spaceWrapper) Close() error {
	s.app.sync.UnregisterSpace(s.id)
	return s.cs.Close()
}

// TryClose forwards to commonspace.Space — it knows whether the space
// has unfinished sync work and refuses to close while busy.
func (s *spaceWrapper) TryClose(ttl time.Duration) (bool, error) {
	closed, err := s.cs.TryClose(ttl)
	if closed {
		s.app.sync.UnregisterSpace(s.id)
	}
	return closed, err
}

// newSpaceCache builds the cache. The loader fully wires a space —
// NewSpace, Init, register — so callers reach a ready Space straight
// from Get.
func (a *App) newSpaceCache() ocache.OCache {
	return ocache.New(
		a.loadSpaceForCache,
		ocache.WithTTL(spaceLoaderTTL),
		ocache.WithGCPeriod(spaceLoaderGC),
	)
}

// loadSpaceForCache is the ocache LoadFunc. Independent of "is this a
// regular space or the tech space" — both flow through here. Storage
// must already exist (caller is expected to Create / Derive first if
// the space is new).
func (a *App) loadSpaceForCache(ctx context.Context, id string) (ocache.Object, error) {
	// The Tracker is constructed lazily and reused across reloads of
	// the same spaceId; we wire it as commonspace.Deps.SyncStatus so
	// any-sync dispatches HeadsChange / ObjectReceive / HeadsApply
	// straight into it.
	tracker := a.syncStatus.For(id)

	cs, err := a.spaceService.NewSpace(ctx, id, commonspace.Deps{
		SyncStatus: tracker,
		TreeSyncer: a.newTreeSyncerForSpace(id),
	})
	if err != nil {
		if errors.Is(err, spacestorage.ErrSpaceStorageMissing) {
			return nil, fmt.Errorf("anysyncx: space %s not found locally: %w", id, err)
		}
		return nil, fmt.Errorf("anysyncx: NewSpace %s: %w", id, err)
	}
	if err := cs.Init(ctx); err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("anysyncx: Init %s: %w", id, err)
	}
	a.sync.RegisterSpace(id, cs)

	// Register system trees the rollup must ignore. ACL fires only
	// HeadsReceive (which the tracker no-ops anyway), but the settings
	// tree is a regular synctree that fires HeadsChange / HeadsApply —
	// excluding it keeps the Pending count tied to user-visible trees.
	// The spaceIndex object id is registered separately by the space
	// layer when ensureSpaceIndexWiring derives it.
	if acl := cs.Acl(); acl != nil {
		tracker.AddExcluded(acl.Id())
	}
	if st := cs.Storage(); st != nil {
		if state := st.StateStorage(); state != nil {
			tracker.AddExcluded(state.SettingsId())
		}
	}

	// Hash cache: seed from current state and subscribe to future
	// changes. The observer is owned by the StateStorage and lives
	// for the duration of the SpaceStorage instance — survives our
	// spaceWrapper.Close (any-sync re-creates StateStorage on the
	// next NewSpace).
	if state, stErr := cs.Storage().StateStorage().GetState(ctx); stErr == nil && state.NewHash != "" {
		a.headCache.Set(id, state.NewHash)
	}
	cs.Storage().StateStorage().SetObserver(a.headCache.observerFor(id))

	return &spaceWrapper{id: id, cs: cs, app: a}, nil
}

// GetSpace returns a handle for the given space id, loading it through
// the cache if necessary. The handle is valid for the duration of the
// operation; the cache TTL evicts idle spaces in the background.
func (a *App) GetSpace(ctx context.Context, id string) (SpaceHandle, error) {
	if a.spaceCache == nil {
		return nil, errors.New("anysyncx: space cache not initialised")
	}
	v, err := a.spaceCache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return v.(SpaceHandle), nil
}

// SyncHeads loads the space (through the cache) and forces an immediate
// head-sync (diff) round on it. Used to converge on demand instead of
// waiting for the periodic headsync timer.
func (a *App) SyncHeads(ctx context.Context, id string) error {
	h, err := a.GetSpace(ctx, id)
	if err != nil {
		return err
	}
	return h.SyncHeads(ctx)
}

// EvictSpace forces immediate close + remove from cache. Used by
// space-delete paths to drop the in-memory state right away rather
// than waiting for TTL.
func (a *App) EvictSpace(ctx context.Context, id string) error {
	if a.spaceCache == nil {
		return nil
	}
	_, err := a.spaceCache.Remove(ctx, id)
	return err
}
