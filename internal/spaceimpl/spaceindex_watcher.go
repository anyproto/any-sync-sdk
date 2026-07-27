package spaceimpl

import (
	"context"
	"errors"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// spaceIndexWatcher mirrors the in-space `spaceIndex` derived
// object's converged property state into the per-account tech-space
// row, via the space.Indexer seam. One watcher per loaded spaceId.
//
// Lifecycle:
//   - Register an unbounded sub on (spaceIndexObjectId,
//     properties.Dataset) via the per-space subscribe engine.
//   - On start, do a one-shot read of the current projected row and
//     forward it to indexer.OnSpaceMetadataUpdated. Closes the
//     cold-start gap: live updates deliver events going forward, but
//     state that landed before we subscribed (e.g. a tree pulled via
//     background sync before the SDK booted this watcher) needs this
//     reconciliation to propagate.
//   - Loop until stop(): read events from the sub's mailbox, decode
//     the four fields off the post-apply row, forward to the indexer.
//
// Overflow tolerance: the per-sub mailbox is bounded. On overflow the
// engine closes the sub with ErrSubscriptionOverflow; the watcher
// returns from the loop. A higher layer can resubscribe; for v1 we
// log-and-exit since the watcher is best-effort.
type spaceIndexWatcher struct {
	store    *spaceobjects.Store
	indexer  space.Indexer
	spaceId  string
	objectId string
	sub      *subscribe.Sub
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// newSpaceIndexWatcher constructs the watcher, subscribes, fires the
// initial mirror, and starts the loop goroutine. Returns the watcher
// ready for watcherRegistry.register; the caller still owns the
// stop() lifecycle.
func newSpaceIndexWatcher(ctx context.Context, store *spaceobjects.Store, indexer space.Indexer, spaceId, spaceIndexObjectId string) *spaceIndexWatcher {
	// Register a sub scoped to this object's `properties` dataset.
	// Empty filter / sort / limit — the watcher doesn't care about
	// per-event content, only that something changed; it always
	// re-reads the projected row.
	sub, _ := store.SubEngine().Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{
			Shared:   false,
			ObjectId: spaceIndexObjectId,
			Dataset:  properties.Dataset,
		},
	}, func(yield func(id string, doc *anyenc.Value)) error { return nil })
	w := &spaceIndexWatcher{
		store:    store,
		indexer:  indexer,
		spaceId:  spaceId,
		objectId: spaceIndexObjectId,
		sub:      sub,
		stopCh:   make(chan struct{}),
	}
	// Initial reconcile — fire-and-forget. Any read failure is swallowed
	// (the next apply event re-fires the mirror).
	w.reconcileOnce(ctx)
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *spaceIndexWatcher) spaceID() string { return w.spaceId }

func (w *spaceIndexWatcher) stop() {
	w.stopOnce.Do(func() {
		if w.sub != nil {
			_ = w.sub.Close()
		}
		close(w.stopCh)
	})
	w.wg.Wait()
}

func (w *spaceIndexWatcher) loop() {
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
		events, err := mb.Wait(context.Background())
		if err != nil {
			return // ErrClosed on stop / overflow / drift
		}
		// Coalesce — when many events arrive between Wait calls the
		// mirror state is whatever the latest row says, so a single
		// reconcile pass covers them all.
		_ = events
		w.reconcileOnce(context.Background())
	}
}

// reconcileOnce reads the spaceIndex row off the per-space `objects`
// collection, decodes the four fields, and forwards to the indexer.
// No-op on absent / empty rows (joiner before owner's seed lands).
func (w *spaceIndexWatcher) reconcileOnce(ctx context.Context) {
	meta, ok := readSpaceIndexRow(ctx, w.store, w.objectId, w.spaceId)
	if !ok {
		return
	}
	_ = w.indexer.OnSpaceMetadataUpdated(ctx, w.spaceId, meta)
}

// readSpaceIndexRow loads the spaceIndex object's projected row from
// the per-space `objects` collection and decodes the four spaceIndex
// fields into a space.SpaceInfo (Id + Name/Description/IconCID/SpaceType).
// Returns (zero, false) when the row is absent, tombstoned, or holds
// no spaceIndex fields — none of which should trigger a mirror write.
func readSpaceIndexRow(ctx context.Context, store *spaceobjects.Store, spaceIndexObjectId, spaceId string) (space.SpaceInfo, bool) {
	coll, err := store.SharedObjects(ctx)
	if err != nil {
		return space.SpaceInfo{}, false
	}
	doc, err := coll.FindId(ctx, spaceIndexObjectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return space.SpaceInfo{}, false
		}
		return space.SpaceInfo{}, false
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return space.SpaceInfo{}, false
	}
	// Clone off the doc buffer — FindId reuses it; the indexer call may
	// outlive the iterator window.
	row := anyencx.Clone(v)
	siNs := row.Get(spaceindex.TypeId)
	if siNs == nil {
		// Row exists (some other writer touched it) but no spaceIndex
		// namespace yet — nothing to mirror.
		return space.SpaceInfo{}, false
	}
	return space.SpaceInfo{
		Id:          spaceId,
		Name:        getString(siNs, spaceindex.FieldName),
		Description: getString(siNs, spaceindex.FieldDescription),
		IconCID:     getString(siNs, spaceindex.FieldIcon),
		// The in-space spaceIndex.spaceType is the app-level tag; mirror
		// it into SpaceInfo.SpaceType (the tech-space FieldSpaceType
		// column). Header Type is pinned in the tech-space record and is
		// not touched by the metadata mirror.
		SpaceType: getString(siNs, spaceindex.FieldSpaceType),
	}, true
}

// getString is a nil-safe accessor that returns "" for missing or
// non-string values. anyenc.Value.GetString already does the
// non-string case; we only need the nil guard.
func getString(v *anyenc.Value, key string) string {
	if v == nil {
		return ""
	}
	return v.GetString(key)
}

// Compile-time check: spaceIndexWatcher satisfies stopper so it can
// register with watcherRegistry alongside memberWatcher.
var _ stopper = (*spaceIndexWatcher)(nil)
