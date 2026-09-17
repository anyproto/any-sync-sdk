package spaceimpl

import (
	"context"
	"sync"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// One-to-one key exchange. A 1-1 ACL is immutable and carries no
// per-writer metadata, and the inbox invite delivers the initiator's
// identity metadata symkey one way only. So each participant publishes
// its own symkey as a row of the `identityKeys` dataset on the space's
// derived spaceIndex object (row id = own account identity; the
// handler admits a row only from that identity), and a watcher folds
// the other participant's row into the identities directory, where the
// profile resolvers pick it up. The space read key is held by exactly
// the two participants, so the audience is the same as the invite's.

var oneToOneKeysLog = logger.NewNamed("sdk.onetoone.keys")

// isOneToOne reports whether spaceId's tech-space row is a one-to-one
// space. Keys on every marker the row can carry: the header type is
// back-filled after the first load and a row registered before the
// header was readable has none yet, while the peer identity is written
// on every 1-1 row from the start. False for an unknown row.
func (s *Service) isOneToOne(ctx context.Context, spaceId string) bool {
	rec, ok := s.tsp.Get(ctx, spaceId)
	return ok && (rec.Type == space.SpaceTypeOneToOne || rec.SpaceType == space.SpaceTypeOneToOne || rec.OneToOnePeer != "")
}

// oneToOnePeerOf returns the other participant's identity off the
// tech-space row, "" when unknown.
func (s *Service) oneToOnePeerOf(ctx context.Context, spaceId string) string {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return ""
	}
	return rec.OneToOnePeer
}

// publishOneToOneKey writes this account's identity metadata symkey to
// the space's identityKeys row when the space is a one-to-one and the
// row is absent or stale. Idempotent: the key is deterministic, so
// every device of the account writes the same bytes and a present,
// equal row is left alone. Best-effort — runs from the post-load seed
// goroutine; a failure is retried by the next load.
func (s *spaceImpl) publishOneToOneKey(ctx context.Context) {
	if s.techIndexId != "" || !s.parent.isOneToOne(ctx, s.id) {
		return
	}
	// One publish per space at a time: concurrent loads of the same
	// space each run a seed goroutine, and two of them seeing the row
	// absent would write two identical changes.
	if !s.parent.beginOneToOnePublish(s.id) {
		return
	}
	defer s.parent.endOneToOnePublish(s.id)
	keys := s.app.AccountKeys()
	if keys == nil {
		return
	}
	me := keys.SignKey.GetPublic().Account()
	want, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
		oneToOneKeysLog.Warn("derive own identity key", zap.String("spaceId", s.id), zap.Error(err))
		return
	}
	obj, err := s.bundles.indexObj(ctx)
	if err != nil {
		oneToOneKeysLog.Debug("load spaceIndex object", zap.String("spaceId", s.id), zap.Error(err))
		return
	}
	if spaceindex.IdentityKeyOf(obj.Controller().Get(ctx, spaceindex.IdentityKeysDataset, me)) == string(want) {
		return
	}
	dataVersion, err := s.store.DataVersion(spaceindex.IdentityKeysDataset)
	if err != nil {
		oneToOneKeysLog.Warn("identityKeys data version", zap.String("spaceId", s.id), zap.Error(err))
		return
	}
	arena := &anyenc.Arena{}
	res, err := s.localWriteRetry(ctx, obj, obj.Id(), crdt.Change{
		Dataset:     spaceindex.IdentityKeysDataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     me,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{spaceindex.FieldIdentityKeySymKey}, Payload: arena.NewString(string(want))}},
		}},
	})
	if err != nil {
		oneToOneKeysLog.Warn("publish identity key", zap.String("spaceId", s.id), zap.Error(err))
		return
	}
	if len(res.Rejections) > 0 {
		oneToOneKeysLog.Warn("publish identity key rejected", zap.String("spaceId", s.id), zap.Error(res.Rejections[0].Err))
	}
}

// beginOneToOnePublish claims the per-space publish slot; false when
// another goroutine holds it.
func (s *Service) beginOneToOnePublish(spaceId string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.oneToOnePublishing[spaceId]; busy {
		return false
	}
	s.oneToOnePublishing[spaceId] = struct{}{}
	return true
}

func (s *Service) endOneToOnePublish(spaceId string) {
	s.mu.Lock()
	delete(s.oneToOnePublishing, spaceId)
	s.mu.Unlock()
}

// oneToOneKeysWatcher folds the peer's identityKeys row of one
// one-to-one space into the identities directory. Same shape as
// spaceIndexWatcher: a sub on (spaceIndexObjectId, identityKeys) with
// no window, a one-shot reconcile on start for state that landed before
// the sub, then a reconcile per event batch. Best-effort: a sub closed
// by mailbox overflow ends the loop, logged, and the watcher is wired
// again when the space is next loaded after an offload or a restart.
// Overflow takes more events than a two-row dataset produces.
type oneToOneKeysWatcher struct {
	parent   *Service
	store    *spaceobjects.Store
	spaceId  string
	objectId string
	self     string
	peer     string
	sub      *subscribe.Sub
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newOneToOneKeysWatcher(ctx context.Context, parent *Service, store *spaceobjects.Store, spaceId, spaceIndexObjectId string) *oneToOneKeysWatcher {
	sub, err := store.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{ObjectId: spaceIndexObjectId, Dataset: spaceindex.IdentityKeysDataset},
	}, func(yield func(id string, doc *anyenc.Value)) error { return nil })
	if err != nil {
		oneToOneKeysLog.Warn("subscribe identityKeys", zap.String("spaceId", spaceId), zap.Error(err))
	}
	w := &oneToOneKeysWatcher{
		parent:   parent,
		store:    store,
		spaceId:  spaceId,
		objectId: spaceIndexObjectId,
		peer:     parent.oneToOnePeerOf(ctx, spaceId),
		sub:      sub,
		stopCh:   make(chan struct{}),
	}
	if keys := parent.app.AccountKeys(); keys != nil {
		w.self = keys.SignKey.GetPublic().Account()
	}
	w.reconcileOnce(ctx)
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *oneToOneKeysWatcher) spaceID() string { return w.spaceId }

func (w *oneToOneKeysWatcher) stop() {
	w.stopOnce.Do(func() {
		if w.sub != nil {
			_ = w.sub.Close()
		}
		close(w.stopCh)
	})
	w.wg.Wait()
}

func (w *oneToOneKeysWatcher) loop() {
	defer w.wg.Done()
	if w.sub == nil {
		return
	}
	mb := w.sub.Events()
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}
		if _, err := mb.Wait(context.Background()); err != nil {
			select {
			case <-w.stopCh:
			default:
				oneToOneKeysLog.Warn("identityKeys sub closed", zap.String("spaceId", w.spaceId), zap.Error(err))
			}
			return
		}
		// Coalesce: the directory state is whatever the rows say now,
		// so one pass covers every event since the last Wait.
		w.reconcileOnce(context.Background())
	}
}

// reconcileOnce reads the peer's identityKeys row off the spaceIndex
// object, caches its key in the identities directory when it is new,
// and kicks the peer-name resolution (a coordinator round-trip, so in
// the background like every other caller) while the directory holds no
// profile for the peer. Only the row keyed by the other participant
// counts: the directory is account-wide, so a row under any other
// identity is ignored rather than cached. Silent on a spaceIndex tree
// not present locally yet — the sub fires once it arrives.
func (w *oneToOneKeysWatcher) reconcileOnce(ctx context.Context) {
	if w.peer == "" {
		w.peer = w.parent.oneToOnePeerOf(ctx, w.spaceId)
		if w.peer == "" {
			return
		}
	}
	obj, err := w.store.Get(ctx, w.objectId)
	if err != nil {
		return
	}
	symKey := spaceindex.IdentityKeyOf(obj.Controller().Get(ctx, spaceindex.IdentityKeysDataset, w.peer))
	if symKey == "" {
		return
	}
	dir, _ := w.parent.tsp.GetIdentity(ctx, w.peer)
	cache, resolve := peerKeyAction(symKey, dir)
	if cache {
		if err := w.parent.tsp.SetIdentityMetaKey(ctx, w.peer, symKey); err != nil {
			oneToOneKeysLog.Warn("cache peer identity key", zap.String("spaceId", w.spaceId), zap.Error(err))
			return
		}
	}
	if resolve {
		go w.parent.resolveOneToOnePeerName(context.Background(), w.peer)
	}
}

// peerKeyAction is what a reconcile owes for the peer's published key
// given the peer's directory row: cache the key when it differs, resolve
// the profile while none is cached. A cached key says nothing about the
// profile — the inbox invite and the synced directory both deliver the
// key with no resolve of their own, and every other resolve kick may
// have run before the key arrived.
func peerKeyAction(rowKey string, dir techspace.IdentityRecord) (cache, resolve bool) {
	cache = dir.SymKey != rowKey
	return cache, cache || dir.Name == ""
}

var _ spaceScoped = (*oneToOneKeysWatcher)(nil)
