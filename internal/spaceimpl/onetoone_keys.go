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
// space. False for an unknown row.
func (s *Service) isOneToOne(ctx context.Context, spaceId string) bool {
	rec, ok := s.tsp.Get(ctx, spaceId)
	return ok && rec.Type == space.SpaceTypeOneToOne
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
	keys := s.app.AccountKeys()
	if keys == nil {
		return
	}
	me := keys.SignKey.GetPublic().Account()
	want, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
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

// oneToOneKeysWatcher folds the peer's identityKeys row of one
// one-to-one space into the identities directory. Same shape as
// spaceIndexWatcher: an unbounded sub on (spaceIndexObjectId,
// identityKeys), a one-shot reconcile on start for state that landed
// before the sub, then a reconcile per event batch. Best-effort: on
// sub overflow the loop exits and the next load re-wires it.
type oneToOneKeysWatcher struct {
	parent   *Service
	store    *spaceobjects.Store
	spaceId  string
	objectId string
	self     string
	sub      *subscribe.Sub
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newOneToOneKeysWatcher(ctx context.Context, parent *Service, store *spaceobjects.Store, spaceId, spaceIndexObjectId string) *oneToOneKeysWatcher {
	sub, _ := store.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{ObjectId: spaceIndexObjectId, Dataset: spaceindex.IdentityKeysDataset},
	}, func(yield func(id string, doc *anyenc.Value)) error { return nil })
	w := &oneToOneKeysWatcher{
		parent:   parent,
		store:    store,
		spaceId:  spaceId,
		objectId: spaceIndexObjectId,
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
			return // ErrClosed on stop / overflow / drift
		}
		// Coalesce: the directory state is whatever the rows say now,
		// so one pass covers every event since the last Wait.
		w.reconcileOnce(context.Background())
	}
}

// reconcileOnce reads every identityKeys row of the spaceIndex object
// and caches each foreign key in the identities directory, kicking the
// peer-name resolution for a key that is new. Silent on a spaceIndex
// tree not present locally yet — the sub fires once it arrives.
func (w *oneToOneKeysWatcher) reconcileOnce(ctx context.Context) {
	obj, err := w.store.Get(ctx, w.objectId)
	if err != nil {
		return
	}
	for _, row := range obj.Controller().Records(ctx, spaceindex.IdentityKeysDataset) {
		id := row.GetString(crdt.IdField)
		symKey := spaceindex.IdentityKeyOf(row)
		if id == "" || id == w.self || symKey == "" {
			continue
		}
		if cur, ok := w.parent.tsp.GetIdentityMetaKey(ctx, id); ok && cur == symKey {
			continue
		}
		if err := w.parent.tsp.SetIdentityMetaKey(ctx, id, symKey); err != nil {
			oneToOneKeysLog.Warn("cache peer identity key", zap.String("spaceId", w.spaceId), zap.Error(err))
			continue
		}
		w.parent.resolveOneToOnePeerName(ctx, id)
	}
}

var _ spaceScoped = (*oneToOneKeysWatcher)(nil)
