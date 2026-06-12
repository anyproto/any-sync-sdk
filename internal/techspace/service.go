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
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"

	"github.com/anyproto/any-sync-sdk/internal/accountvalues"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
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

	// open is read lock-free from many methods (incl. background
	// goroutines like the spaceIndex lazy-seed) and flipped to false by
	// Close — atomic to keep those concurrent accesses race-free.
	open    atomic.Bool
	spaceId string
	indexId string

	// avIds caches targetSpaceId → derived account-values carrier
	// object id (see accountvalues.go). Guarded by avMu.
	avMu  sync.Mutex
	avIds map[string]string

	// store backs the single index object. It is a "raw" spaceobjects
	// Store (custom handlers, gate disabled) so the index inherits the
	// regular-space sync + subscription machinery — ocache-resident
	// object, SetDeferredUpdater(true), ColdRestore-on-load, and the
	// subscribe.Engine — without the type/properties model. The index
	// object's spaces/profile datasets live at <indexId>_spaces /
	// <indexId>_profile, exactly as the previous bespoke controller
	// wrote them.
	store *spaceobjects.Store
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

// SyncHeads forces an immediate head-sync round on the tech space, so
// remotely-added or -removed spaces land in the local index without
// waiting for the periodic headsync timer. No-op before Open.
func (s *Service) SyncHeads(ctx context.Context) error {
	if !s.open.Load() || s.spaceId == "" {
		return nil
	}
	return s.app.SyncHeads(ctx, s.spaceId)
}

// Open derives the tech-space id, ensures storage exists, and
// computes the space-index object id. The space itself is loaded via
// the cache as needed (and on the first call to do an initial cold
// restore so subscribed callers see persisted state immediately).
func (s *Service) Open(ctx context.Context) error {
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

	s.store = spaceobjects.NewStoreWithConfig(spaceobjects.StoreConfig{
		App:     s.app,
		DB:      s.db,
		SignKey: keys.SignKey,
		SpaceId: s.spaceId,
		Alloc:   object.NewVersionAllocator(""),
		Handlers: []crdt.HandlerReg{
			{Name: SpaceIndexDataset, Handler: SpaceIndexHandler{}, Schema: SpaceIndexSchema()},
			{Name: ProfileDataset, Handler: ProfileHandler{}, Schema: ProfileSchema()},
			// Account-values carrier (one derived object per target
			// space) — see accountvalues.go. Dynamic: carrier records
			// carry free-form typeId heads at the target rows' paths.
			{Name: accountvalues.Dataset, Handler: crdt.DefaultHandler{}, Schema: accountvalues.Schema()},
		},
		DisableGate: true,
		DataVersions: map[string]string{
			SpaceIndexDataset:     HandlerVersion,
			ProfileDataset:        ProfileHandlerVersion,
			accountvalues.Dataset: accountvalues.HandlerVersion,
		},
	})

	// Derive the single index object through the Store. Store.Derive
	// computes the deterministic id, then runs PutTree (first boot) /
	// BuildTree (subsequent) + SetDeferredUpdater + ColdRestore
	// atomically, and keeps the object resident in ocache so inbound
	// head-sync changes project live via its listener. The id is the
	// same SpaceIndexDeriveSeed-derived id the bespoke path used.
	obj, err := s.store.Derive(ctx, spaceobjects.DeriveOpts{ChangePayload: []byte(SpaceIndexDeriveSeed)})
	if err != nil {
		return fmt.Errorf("techspace: derive index object: %w", err)
	}
	s.indexId = obj.Id()

	s.open.Store(true)
	return nil
}

// indexObj returns the resident index *object.Object via the Store's
// ocache. Cheap on a cache hit; on a TTL-evicted miss the Store
// rebuilds the tree with its listener + ColdRestore atomically. The
// MaxAddSeq watermark is guarded by the single resident object's
// tree.Lock and ocache's per-id load lock — no caller-side mutex.
func (s *Service) indexObj(ctx context.Context) (*object.Object, error) {
	return s.store.Get(ctx, s.indexId)
}

// SpaceId returns the tech-space id once Open has run.
func (s *Service) SpaceId() string { return s.spaceId }

// IndexObjectId returns the space-index object id once Open has run.
func (s *Service) IndexObjectId() string { return s.indexId }

// SubEngine exposes the index object's live-query engine so the space
// layer can back space.Service.Subscribe over the spaces dataset.
func (s *Service) SubEngine() *subscribe.Engine { return s.store.SubEngine() }

// Store exposes the tech-space object Store so the space layer can build
// generic Query/Subscribe over the system datasets (spaces, profile,
// future system objects) by object id.
func (s *Service) Store() *spaceobjects.Store { return s.store }

// Add writes a new space-index record.
func (s *Service) Add(ctx context.Context, rec SpaceIndexRecord) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if rec.Id == "" {
		return object.WriteResult{}, errors.New("techspace: SpaceIndexRecord.Id required")
	}

	obj, err := s.indexObj(ctx)
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
	obj, err := s.indexObj(ctx)
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

// SetLocalStatus updates the device-local localStatus field via the
// local-set path: it does NOT enter the DAG and never syncs to other
// devices (each device owns its own value). Use for per-device
// lifecycle — active / joining / offloaded.
func (s *Service) SetLocalStatus(ctx context.Context, spaceId, status string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
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
	return obj.LocalSet(ctx, change)
}

// SetRemoteStatus updates the SYNCED remoteStatus field — account-wide
// state that propagates to every device. Used for account-wide delete
// (status=StatusDeleted); the handler keeps deleted terminal.
func (s *Service) SetRemoteStatus(ctx context.Context, spaceId, status string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
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
					Path:    []string{FieldRemoteStatus},
					Payload: arena.NewString(status),
				}},
			},
		},
	}
	return obj.LocalWrite(ctx, change)
}

// Get returns the current state of one space-index record. Reads off
// the resident index object's controller; inbound changes are kept
// current by the object's deferred-updater listener (no drain needed).
func (s *Service) Get(ctx context.Context, spaceId string) (SpaceIndexRecord, bool) {
	if !s.open.Load() {
		return SpaceIndexRecord{}, false
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return SpaceIndexRecord{}, false
	}
	v := obj.Controller().Get(ctx, SpaceIndexDataset, spaceId)
	if v == nil {
		return SpaceIndexRecord{}, false
	}
	return DecodeSpaceIndexRecord(v), true
}

// List returns every live space-index record. Inbound head-sync
// changes are projected live by the resident index object's
// deferred-updater listener, so a plain controller read is current —
// the old read-time drain is gone.
func (s *Service) List(ctx context.Context) []SpaceIndexRecord {
	if !s.open.Load() {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return nil
	}
	rows := obj.Controller().Records(ctx, SpaceIndexDataset)
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
	obj, err := s.indexObj(ctx)
	if err != nil {
		return ProfileRecord{}, false
	}
	v := obj.Controller().Get(ctx, ProfileDataset, ProfileSelfId)
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
	obj, err := s.indexObj(ctx)
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

// OnSpaceDeleted satisfies space.Indexer. Flips the row to the synced
// remoteStatus=deleted (account-wide — propagates to every device); the
// record is never physically removed.
func (s *Service) OnSpaceDeleted(ctx context.Context, spaceId string) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	_, err := s.SetRemoteStatus(ctx, spaceId, StatusDeleted)
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

// Close marks the service inactive and tears down the index Store
// (which closes the resident object + the subscribe engine). Underlying
// space cleanup happens via the App's space cache on its own schedule.
func (s *Service) Close(_ context.Context) error {
	s.open.Store(false)
	if s.store != nil {
		_ = s.store.Close()
	}
	return nil
}

// SpaceRegistry adapter — the tech-space owns one tree (the index).
// Anything else routes back through the app's broader SpaceRegistry
// (set later when regular spaces gain their own one).

var ErrSpaceRegistryUnknown = errors.New("techspace: unknown (spaceId, treeId)")

// GetTree resolves the index tree via the Store's resident object.
// Anything else under the tech space is unknown. The Store returns the
// listener-bound, deferred-updater, cold-restored object (reloading it
// if ocache evicted), so the synctree's AddRawChangesFromPeer path
// fires Update → replayLocked and inbound index changes project live —
// the cold-sync path that TestE2E_ColdSyncSameKey covers.
func (s *Service) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	if spaceId != s.spaceId || treeId != s.indexId {
		return nil, ErrSpaceRegistryUnknown
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return nil, err
	}
	tree := obj.Tree()
	if tree == nil {
		return nil, fmt.Errorf("techspace: index tree not bound")
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
