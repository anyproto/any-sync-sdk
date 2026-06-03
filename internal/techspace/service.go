package techspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/space"
)

// Service runs the account-private tech space — derived from the
// account key, owner-only ACL — and exposes typed access to the
// space-index dataset.
//
// Loads the underlying any-sync space lazily through anysyncx's
// space cache. The CRDT controller stays loaded for the life of the
// SDK (cheap, DB-backed); the any-sync side evicts on TTL like any
// other space. Each write rebinds an *object.Object to the freshly
// loaded tree before issuing tree.AddContent.
type Service struct {
	app *anysyncx.App
	db  anystore.DB

	mu sync.Mutex
	// open is read lock-free from many methods (incl. background
	// goroutines like the spaceIndex lazy-seed) and flipped to false by
	// Close — atomic to keep those concurrent accesses race-free.
	open    atomic.Bool
	spaceId string
	indexId string
	ctrl    *crdt.Controller
	alloc   *object.VersionAllocator

	// bindMu serializes bindIndexObject calls. Each call constructs a
	// fresh *object.Object and runs the synctree afterBuild → Rebuild
	// → replayLocked cycle against the shared Controller. With two
	// different *Object instances reading/writing Controller.MaxAddSeq
	// concurrently (one from a writer like SetSpaceMetadata, another
	// from sync service's HandleHeadUpdate routing through GetTree),
	// the unprotected uint64 access races. Caller-driven serialization
	// keeps the existing fresh-Object pattern (per the cold-sync
	// rationale in GetTree's docstring) without the race.
	bindMu sync.Mutex
}

// TechSpaceType is the on-the-wire SpaceType stamped into the
// tech-space's space header. Mirrors anytype-heart's
// spacedomain.SpaceTypeTech and is the value the any-sync-coordinator
// recognises for tech spaces (see any-sync-coordinator
// spacestatus/changeverifier.go).
const TechSpaceType = "anytype.techspace"

// New returns a Service ready for Open.
func New(app *anysyncx.App, db anystore.DB) *Service {
	return &Service{app: app, db: db}
}

// Open derives the tech-space id, ensures storage exists, and
// computes the space-index object id. The space itself is loaded via
// the cache as needed (and on the first call to do an initial cold
// restore so subscribed callers see persisted state immediately).
func (s *Service) Open(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open.Load() {
		return nil
	}

	keys := s.app.AccountKeys()
	if keys == nil {
		return errors.New("techspace: anysyncx app has no account keys")
	}

	// SpaceType for the tech space matches the any-sync-coordinator's
	// allow-list (see spacestatus/changeverifier.go in
	// any-sync-coordinator). The library default
	// spacepayloads.SpaceReserved ("any-sync.space") is rejected by
	// the coordinator — it only knows the anytype.* family.
	spaceCfg := spacepayloads.SpaceDerivePayload{
		SigningKey: keys.SignKey,
		MasterKey:  keys.SignKey,
		SpaceType:  TechSpaceType,
	}
	spaceId, err := s.app.SpaceService().DeriveId(ctx, spaceCfg)
	if err != nil {
		return fmt.Errorf("techspace: derive id: %w", err)
	}
	s.spaceId = spaceId

	if !s.app.SpaceExists(spaceId) {
		if _, err := s.app.SpaceService().DeriveSpace(ctx, spaceCfg); err != nil {
			return fmt.Errorf("techspace: derive: %w", err)
		}
	}

	// Load the space once at boot so we can compute the index objectId
	// (depends on the space's keys/state) and run cold restore. After
	// this returns, the space is in cache and may TTL-evict if idle.
	handle, err := s.app.GetSpace(ctx, spaceId)
	if err != nil {
		return fmt.Errorf("techspace: get space: %w", err)
	}
	cs := handle.Inner()

	derivePayload := objecttree.ObjectTreeDerivePayload{
		ChangePayload: []byte(SpaceIndexDeriveSeed),
		SpaceId:       spaceId,
		IsEncrypted:   true,
	}
	storagePayload, err := cs.TreeBuilder().DeriveTree(ctx, derivePayload)
	if err != nil {
		return fmt.Errorf("techspace: derive index tree payload: %w", err)
	}
	s.indexId = storagePayload.RootRawChange.Id

	collName := s.indexId + "_" + SpaceIndexDataset
	if _, err := s.db.Collection(ctx, collName); err != nil {
		return fmt.Errorf("techspace: open index collection: %w", err)
	}

	ctrl, err := crdt.NewController(ctx, s.indexId, s.db,
		crdt.HandlerReg{Name: SpaceIndexDataset, Handler: SpaceIndexHandler{}},
		crdt.HandlerReg{Name: ProfileDataset, Handler: ProfileHandler{}},
	)
	if err != nil {
		return fmt.Errorf("techspace: new controller: %w", err)
	}
	s.ctrl = ctrl
	s.alloc = object.NewVersionAllocator("")

	// First-time PutTree (creates) or BuildTree (already created on a
	// prior boot). Either way we run ColdRestore against the freshly
	// bound tree.
	obj, err := s.bindIndexObject(ctx, cs, storagePayload)
	if err != nil {
		return err
	}
	if err := obj.ColdRestore(ctx); err != nil {
		return fmt.Errorf("techspace: cold restore: %w", err)
	}

	s.open.Store(true)
	return nil
}

// bindIndexObject wires a fresh *object.Object to the index tree under
// the given (loaded) commonspace.Space. The Object is short-lived —
// callers use it for a single LocalWrite or ColdRestore and let it go.
// On subsequent calls under a different (re-loaded) Space, a new
// Object is built; both share the same controller and allocator.
func (s *Service) bindIndexObject(ctx context.Context, cs interface {
	TreeBuilder() objecttreebuilder.TreeBuilder
}, storagePayload treestorage.TreeStorageCreatePayload) (*object.Object, error) {
	keys := s.app.AccountKeys()
	return object.New(object.Config{
		SpaceId:    s.spaceId,
		SignKey:    keys.SignKey,
		Controller: s.ctrl,
		Allocator:  s.alloc,
	}, func(listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
		tree, err := cs.TreeBuilder().PutTree(ctx, storagePayload, listener)
		if err == nil {
			return tree, nil
		}
		if !errors.Is(err, treestorage.ErrTreeExists) {
			return nil, fmt.Errorf("techspace: put index tree: %w", err)
		}
		tree, err = cs.TreeBuilder().BuildTree(ctx, s.indexId, objecttreebuilder.BuildTreeOpts{Listener: listener})
		if err != nil {
			return nil, fmt.Errorf("techspace: build index tree: %w", err)
		}
		return tree, nil
	})
}

// indexObject loads the tech space (via cache), rebinds an Object to
// the index tree, and returns it. Each write is a separate Get →
// rebind cycle; the cache TTL governs when the underlying space tears
// down.
//
// Caller must hold s.bindMu — see Service.bindMu docstring. The lock
// covers BOTH the bind (which runs the synctree afterBuild → Rebuild
// → replayLocked cycle against the shared Controller) AND the
// follow-up Controller mutation (LocalWrite), so two callers don't
// interleave on the Controller's MaxAddSeq watermark.
func (s *Service) indexObject(ctx context.Context) (*object.Object, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("techspace: get space: %w", err)
	}
	derivePayload := objecttree.ObjectTreeDerivePayload{
		ChangePayload: []byte(SpaceIndexDeriveSeed),
		SpaceId:       s.spaceId,
		IsEncrypted:   true,
	}
	storagePayload, err := handle.Inner().TreeBuilder().DeriveTree(ctx, derivePayload)
	if err != nil {
		return nil, fmt.Errorf("techspace: derive index tree payload: %w", err)
	}
	return s.bindIndexObject(ctx, handle.Inner(), storagePayload)
}

// SpaceId returns the tech-space id once Open has run.
func (s *Service) SpaceId() string { return s.spaceId }

// IndexObjectId returns the space-index object id once Open has run.
func (s *Service) IndexObjectId() string { return s.indexId }

// Add writes a new space-index record.
func (s *Service) Add(ctx context.Context, rec SpaceIndexRecord) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if rec.Id == "" {
		return object.WriteResult{}, errors.New("techspace: SpaceIndexRecord.Id required")
	}

	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	obj, err := s.indexObject(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}

	arena := &anyenc.Arena{}
	payload := rec.EncodeCreate(arena)

	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{
			{
				Id:     rec.Id,
				Upsert: true,
				Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
			},
		},
	}
	return obj.LocalWrite(ctx, change)
}

// SetSpaceMetadata is the idempotent overwrite of the row's
// name / description / icon fields — driven by the per-space
// spaceIndex watcher whenever the in-space spaceIndex object's
// converged state changes. `type` is intentionally left untouched
// (pinned on first write by SpaceIndexHandler.BeforeModify); status
// fields stay under their own setters.
//
// The write is a single multi-field $set so all three columns land
// under one VersionId. No-op when every field equals the empty
// string (rare — the spaceIndex object can carry an empty mirror
// state right after Create, before the initial property write has
// applied).
func (s *Service) SetSpaceMetadata(ctx context.Context, spaceId, name, description, iconCID, spaceType string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	obj, err := s.indexObject(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set(FieldName, arena.NewString(name))
	payload.Set(FieldDescription, arena.NewString(description))
	payload.Set(FieldIcon, arena.NewString(iconCID))
	payload.Set(FieldSpaceType, arena.NewString(spaceType))
	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:  spaceId,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	}
	return obj.LocalWrite(ctx, change)
}

// SetLocalStatus updates the localStatus field of an existing record.
func (s *Service) SetLocalStatus(ctx context.Context, spaceId, status string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	obj, err := s.indexObject(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}

	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{
			{
				Id: spaceId,
				Ops: []crdt.Op{{
					Type:    crdt.OpSet,
					Path:    []string{FieldLocalStatus},
					Payload: arena.NewString(status),
				}},
			},
		},
	}
	return obj.LocalWrite(ctx, change)
}

// Get returns the current state of one space-index record. Reads
// straight off the controller — no space load needed.
func (s *Service) Get(ctx context.Context, spaceId string) (SpaceIndexRecord, bool) {
	if !s.open.Load() {
		return SpaceIndexRecord{}, false
	}
	v := s.ctrl.Get(ctx, SpaceIndexDataset, spaceId)
	if v == nil {
		return SpaceIndexRecord{}, false
	}
	return DecodeSpaceIndexRecord(v), true
}

// List returns every live space-index record.
func (s *Service) List(ctx context.Context) []SpaceIndexRecord {
	if !s.open.Load() {
		return nil
	}
	rows := s.ctrl.Records(ctx, SpaceIndexDataset)
	out := make([]SpaceIndexRecord, 0, len(rows))
	for _, v := range rows {
		out = append(out, DecodeSpaceIndexRecord(v))
	}
	return out
}

// GetProfile returns the locally-stored profile (the source-of-truth
// the SDK pushes to identityRepo on boot). The second return is true
// when a profile has ever been written; false on a fresh device that
// hasn't seen UpdateMetadata yet.
func (s *Service) GetProfile(ctx context.Context) (ProfileRecord, bool) {
	if !s.open.Load() {
		return ProfileRecord{}, false
	}
	v := s.ctrl.Get(ctx, ProfileDataset, ProfileSelfId)
	if v == nil {
		return ProfileRecord{}, false
	}
	return DecodeProfileRecord(v), true
}

// SetProfile writes (or replaces) the local profile row. Called by
// Account.UpdateMetadata to persist the value before pushing it to
// identityRepo, so a subsequent boot can republish without the user
// re-calling UpdateMetadata.
func (s *Service) SetProfile(ctx context.Context, rec ProfileRecord) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	obj, err := s.indexObject(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     ProfileDataset,
		DataVersion: ProfileHandlerVersion,
		Records: []crdt.RecordChange{
			{
				Id:     ProfileSelfId,
				Upsert: true,
				Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: rec.encodeUpsert(arena)}},
			},
		},
	}
	_, err = obj.LocalWrite(ctx, change)
	return err
}

// OnSpaceCreated satisfies space.Indexer. Adds a fresh record to the
// space-index dataset with the supplied metadata. The existing
// spaceimpl.Service.Create still calls Add() directly for the eager
// caller-round-trip seed; this method exists for future callers that
// route everything through the Indexer seam.
func (s *Service) OnSpaceCreated(ctx context.Context, spaceId string, meta space.SpaceInfo) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	if spaceId == "" {
		return errors.New("techspace: OnSpaceCreated: empty spaceId")
	}
	if _, ok := s.Get(ctx, spaceId); ok {
		return nil // idempotent — row already exists
	}
	_, err := s.Add(ctx, SpaceIndexRecord{
		Id:           spaceId,
		Type:         meta.Type,
		SpaceType:    meta.SpaceType,
		Name:         meta.Name,
		Description:  meta.Description,
		IconCID:      meta.IconCID,
		LocalStatus:  StatusActive,
		RemoteStatus: StatusActive,
	})
	return err
}

// OnSpaceDeleted satisfies space.Indexer. Flips the row to
// localStatus=deleted; the record is never physically removed.
func (s *Service) OnSpaceDeleted(ctx context.Context, spaceId string) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	_, err := s.SetLocalStatus(ctx, spaceId, StatusDeleted)
	return err
}

// OnSpaceMetadataUpdated satisfies space.Indexer. Idempotent
// overwrite of the row's name / description / icon — the mirror
// from the in-space spaceIndex derived object into this device's
// tech-space row. Skipped when:
//
//   - The row is absent (joiner without a tech-space record yet, or
//     a peer that hasn't completed Join).
//   - All three fields already equal what we would write. This is
//     the common case for the initial seed (Service.Create's
//     tsp.Add wrote the row first; the in-space spaceIndex apply
//     then triggers the watcher to re-write the SAME values).
//     Dedup avoids a pointless second CRDT change on the tech-space
//     tree, which would otherwise race with caller-initiated writes
//     like SetLocalStatus on the apply lock.
func (s *Service) OnSpaceMetadataUpdated(ctx context.Context, spaceId string, meta space.SpaceInfo) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	rec, ok := s.Get(ctx, spaceId)
	if !ok {
		return nil
	}
	if rec.Name == meta.Name && rec.Description == meta.Description && rec.IconCID == meta.IconCID && rec.SpaceType == meta.SpaceType {
		return nil
	}
	_, err := s.SetSpaceMetadata(ctx, spaceId, meta.Name, meta.Description, meta.IconCID, meta.SpaceType)
	return err
}

// Compile-time check that the Service satisfies space.Indexer.
var _ space.Indexer = (*Service)(nil)

// Close marks the service inactive. Underlying space cleanup happens
// via the App's space cache on its own schedule.
func (s *Service) Close(_ context.Context) error {
	s.open.Store(false)
	return nil
}

// SpaceRegistry adapter — the tech-space owns one tree (the index).
// Anything else routes back through the app's broader SpaceRegistry
// (set later when regular spaces gain their own one).

var ErrSpaceRegistryUnknown = errors.New("techspace: unknown (spaceId, treeId)")

// GetTree resolves the index tree with our CRDT controller's listener
// bound. Anything else under the tech space is unknown.
//
// Why a fresh bindIndexObject per call: ocache may TTL-evict the
// underlying commonspace.Space between calls, which closes the
// previous tree handle. Each call rebinds the listener onto the
// currently-loaded tree so the synctree's AddRawChangesFromPeer path
// fires Update on us — without that the on-disk tree fills up but
// our space-index controller stays empty (the cold-sync regression
// surfaced by TestE2E_ColdSyncSameKey).
func (s *Service) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	if spaceId != s.spaceId || treeId != s.indexId {
		return nil, ErrSpaceRegistryUnknown
	}
	s.bindMu.Lock()
	defer s.bindMu.Unlock()
	obj, err := s.indexObject(ctx)
	if err != nil {
		return nil, err
	}
	tree := obj.Tree()
	if tree == nil {
		return nil, fmt.Errorf("techspace: index tree not bound after indexObject")
	}
	return tree, nil
}

func (s *Service) PutTree(_ context.Context, _ string, _ treestorage.TreeStorageCreatePayload) error {
	return ErrSpaceRegistryUnknown
}

func (s *Service) MarkTreeDeleted(_ context.Context, _, _ string) error { return nil }

func (s *Service) DeleteTree(_ context.Context, _, _ string) error {
	return ErrSpaceRegistryUnknown
}
