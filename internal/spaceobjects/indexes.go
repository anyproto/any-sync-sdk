package spaceobjects

import (
	"context"

	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Declared indexes of shared datasets. A per-object collection gets its
// declared indexes from the registration when a controller opens it. A
// shared dataset has one collection per space, read without any
// controller, so the store keeps its indexes equal to the catalog's: a
// worker reconciles them when the catalog changes and at open. A build
// scans the collection inside one write transaction, so every writer
// of the database waits for it; it never runs on the apply path.

// IndexBuildPhase is the state an IndexBuild reports.
type IndexBuildPhase int

const (
	IndexBuildStarted IndexBuildPhase = iota
	IndexBuildDone
	IndexBuildFailed
)

// IndexBuild reports the build of a shared dataset's missing declared
// indexes: Started before the scan, then Done or Failed (Err set).
type IndexBuild struct {
	Dataset string
	Phase   IndexBuildPhase
	Err     error
}

// SubscribeIndexBuilds registers cb on the index-build feed. A build in
// flight is reported to cb at once as Started, so its terminal report
// never arrives alone. cb runs on the index worker, or here for that
// first report — keep it small or hand off, and do not subscribe from
// it. The returned cancel is idempotent.
func (s *Store) SubscribeIndexBuilds(cb func(IndexBuild)) (cancel func()) {
	if cb == nil {
		return func() {}
	}
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	cancel = s.indexBuilds.Add(cb)
	if s.building != "" {
		cb(IndexBuild{Dataset: s.building, Phase: IndexBuildStarted})
	}
	return cancel
}

// reportBuild dispatches one build report. buildMu orders it against a
// subscription being added.
func (s *Store) reportBuild(ev IndexBuild) {
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	if ev.Phase == IndexBuildStarted {
		s.building = ev.Dataset
	} else {
		s.building = ""
	}
	s.indexBuilds.Dispatch(ev)
}

// StartIndexSync lets the index worker run and asks for a first pass,
// which builds what a previous run left missing. Until it is called a
// store indexes nothing: one handed back closed during teardown never
// starts background work.
func (s *Store) StartIndexSync() {
	s.indexMu.Lock()
	s.indexStarted = true
	s.indexMu.Unlock()
	s.kickIndexSync()
}

// kickIndexSync asks the index worker for a pass over the space's
// shared datasets, starting the worker on first use. Never blocks. A
// space without shared datasets starts no worker.
func (s *Store) kickIndexSync() {
	if s.catalog == nil || s.db == nil || len(s.keyedDatasets()) == 0 {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if s.indexClosed || !s.indexStarted {
		return
	}
	if s.indexKick == nil {
		s.indexKick = make(chan struct{}, 1)
		s.indexDone = make(chan struct{})
		var ctx context.Context
		ctx, s.indexCancel = context.WithCancel(context.Background())
		go s.indexLoop(ctx)
	}
	select {
	case s.indexKick <- struct{}{}:
	default:
	}
}

// holdIndexSync keeps the worker off a type's datasets while the type
// object replays: each replayed definition change refreshes the
// catalog, and a dataset whose index records have not replayed yet
// would lose its indexes to a pass in between. releaseIndexSync ends
// the hold and asks for the pass.
func (s *Store) holdIndexSync(typeId string) {
	s.indexMu.Lock()
	if s.indexHeld == nil {
		s.indexHeld = make(map[string]struct{})
	}
	s.indexHeld[typeId] = struct{}{}
	s.indexMu.Unlock()
}

func (s *Store) releaseIndexSync(typeId string) {
	s.indexMu.Lock()
	_, held := s.indexHeld[typeId]
	delete(s.indexHeld, typeId)
	s.indexMu.Unlock()
	if held {
		s.kickIndexSync()
	}
}

func (s *Store) indexSyncHeld(typeId string) bool {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	_, held := s.indexHeld[typeId]
	return held
}

// stopIndexSync stops the index worker and waits for a build in flight:
// Close must not return while the worker still writes the database.
func (s *Store) stopIndexSync() {
	s.indexMu.Lock()
	s.indexClosed = true
	cancel, done := s.indexCancel, s.indexDone
	s.indexMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (s *Store) indexLoop(ctx context.Context) {
	defer close(s.indexDone)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.indexKick:
		}
		for _, dataset := range s.keyedDatasets() {
			if ctx.Err() != nil {
				return
			}
			if err := s.syncKeyedIndexes(ctx, dataset); err != nil && ctx.Err() == nil {
				storeLog.Warn("shared dataset: index sync",
					zap.String("spaceId", s.spaceId), zap.String("dataset", dataset), zap.Error(err))
			}
		}
	}
}

// syncKeyedIndexes makes the declared indexes of a shared dataset's
// collection equal to the catalog's: drops the ones no definition
// names any more, builds the missing ones.
func (s *Store) syncKeyedIndexes(ctx context.Context, dataset string) error {
	ds, ok := s.catalog.lookup(dataset)
	if !ok || !ds.Shared || s.indexSyncHeld(ds.TypeId) {
		return nil
	}
	coll, err := s.keyedCollection(ctx, dataset)
	if err != nil {
		return err
	}
	want := ds.StoreIndexes()
	if err := crdt.DropStaleIndexes(ctx, coll, want, schema.IndexStorePrefix); err != nil {
		return err
	}
	missing := crdt.MissingIndexes(coll, want)
	if len(missing) == 0 {
		return nil
	}
	s.reportBuild(IndexBuild{Dataset: dataset, Phase: IndexBuildStarted})
	if err := coll.EnsureIndex(ctx, missing...); err != nil {
		s.reportBuild(IndexBuild{Dataset: dataset, Phase: IndexBuildFailed, Err: err})
		return err
	}
	s.reportBuild(IndexBuild{Dataset: dataset, Phase: IndexBuildDone})
	return nil
}

// KeyedIndexReady reports whether the shared dataset's collection holds
// the any-store index named storeName. False for a dataset that is not
// shared or a collection that cannot be opened.
func (s *Store) KeyedIndexReady(ctx context.Context, dataset, storeName string) bool {
	coll, ok, err := s.KeyedCollection(ctx, dataset)
	if err != nil || !ok {
		return false
	}
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Name == storeName {
			return true
		}
	}
	return false
}
