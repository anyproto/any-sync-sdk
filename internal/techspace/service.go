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
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/util/crypto"

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
// tech-space's space header. The `any` product's own value — distinct
// from anytype-heart's "anytype.techspace", so an SDK account never
// shares a tech space with a heart client, whatever its derivation
// index. Headers carrying it declare fileproto v2 (coordinator-
// enforced; see spacestatus/changeverifier.go there).
const TechSpaceType = "any.techspace"

// deriveCfg builds the tech-space derive payload. The type and
// fileproto version feed the derived space id (pinned by
// TestDeriveCfg_Stable). The library default
// spacepayloads.SpaceReserved ("any-sync.space") is rejected by the
// coordinator.
func deriveCfg(signKey crypto.PrivKey) spacepayloads.SpaceDerivePayload {
	return spacepayloads.SpaceDerivePayload{
		SigningKey:       signKey,
		MasterKey:        signKey,
		SpaceType:        TechSpaceType,
		FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
	}
}

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

	spaceCfg := deriveCfg(keys.SignKey)
	spaceId, err := s.app.SpaceService().DeriveId(ctx, spaceCfg)
	if err != nil {
		return fmt.Errorf("techspace: derive id: %w", err)
	}
	s.spaceId = spaceId

	// Headless: the tech space is a private registry of tracked foreign
	// spaces, strictly local to this device — never announced to sync
	// nodes, never pushed, never coordinator-signed. Marked BEFORE the
	// first load below (the peer manager is chosen at NewSpace time).
	if s.app.Headless() {
		s.app.MarkSpaceLocalOnly(spaceId)
	}

	// Boot-serial only (sdk.Open, before any API is exposed), so this
	// exists-check + out-of-band create cannot race itself. Do NOT copy
	// this shape into a concurrent context: CreateSpaceStorage is
	// concurrency-safe only under the space cache's per-id load — a
	// concurrent deterministic-id create must ride the cache load
	// instead (see spaceimpl's ctxWithCreatePayload).
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
			{Name: InboxCursorDataset, Handler: InboxCursorHandler{}, Schema: InboxCursorSchema()},
			{Name: IdentitiesDataset, Handler: IdentitiesHandler{}, Schema: IdentitiesSchema(), Indexes: IdentitiesIndexes()},
			{Name: DevicesDataset, Handler: DevicesHandler{}, Schema: DevicesSchema()},
			// Account-values carrier (one derived object per target
			// space) — see accountvalues.go. Dynamic: carrier records
			// carry free-form typeId heads at the target rows' paths.
			{Name: accountvalues.Dataset, Handler: crdt.DefaultHandler{}, Schema: accountvalues.Schema()},
		},
		DisableGate: true,
		DataVersions: map[string]string{
			SpaceIndexDataset:     HandlerVersion,
			ProfileDataset:        ProfileHandlerVersion,
			InboxCursorDataset:    InboxCursorHandlerVersion,
			IdentitiesDataset:     IdentitiesHandlerVersion,
			DevicesDataset:        DevicesHandlerVersion,
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

// setRowField is the shared scaffold behind the per-field setters:
// one OpSet at [field] on spaceId's row, payload built on a fresh
// arena. local selects LocalSet (device-local, never enters the DAG)
// vs LocalWrite (synced, account-wide).
func (s *Service) setRowField(ctx context.Context, spaceId, field string, local bool, payload func(a *anyenc.Arena) *anyenc.Value) (object.WriteResult, error) {
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
		Records: []crdt.RecordChange{{
			Id: spaceId,
			Ops: []crdt.Op{{
				Type:    crdt.OpSet,
				Path:    []string{field},
				Payload: payload(arena),
			}},
		}},
	}
	if local {
		return obj.LocalSet(ctx, change)
	}
	return obj.LocalWrite(ctx, change)
}

// SetLocalStatus updates the device-local localStatus field via the
// local-set path: it does NOT enter the DAG and never syncs to other
// devices (each device owns its own value). Use for per-device
// lifecycle — active / joining / offloaded.
func (s *Service) SetLocalStatus(ctx context.Context, spaceId, status string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldLocalStatus, true, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(status)
	})
}

// SetType backfills the on-wire header type onto a row created with an
// unknown type (join/track register rows before the space's header is
// readable). Synced write; the handler's set-once rule makes it a no-op
// race loser everywhere the value is already non-empty.
func (s *Service) SetType(ctx context.Context, spaceId, typ string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldType, false, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(typ)
	})
}

// SetPushKeys mirrors the space's derived push-notification key
// material onto its row via the local-set path (FieldPushKeys,
// device-local; never enters the DAG — every device derives the same
// values from the same converged ACL). The whole `push` object is
// replaced in one op: encKey/encKeyId always rotate together and
// spaceKey never changes, so per-subfield merging buys nothing.
// Caller (the ACL mirror watcher) is responsible for the row-exists
// check and for skipping no-op writes.
func (s *Service) SetPushKeys(ctx context.Context, spaceId string, keys space.PushKeys) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldPushKeys, true, func(a *anyenc.Arena) *anyenc.Value {
		payload := a.NewObject()
		payload.Set(PushKeySpaceKey, a.NewString(keys.SpaceKey))
		payload.Set(PushKeyEncKey, a.NewString(keys.EncKey))
		payload.Set(PushKeyEncKeyId, a.NewString(keys.EncKeyId))
		return payload
	})
}

// SetOwnRole mirrors this account's own ACL permission onto the
// space's row via the local-set path (FieldOwnRole, device-local;
// never enters the DAG — every device derives the same role from the
// same converged ACL). Stored as the canonical wire label
// (space.Permission.String). Caller (the ACL mirror watcher) is
// responsible for the row-exists check and for skipping no-op writes.
func (s *Service) SetOwnRole(ctx context.Context, spaceId string, role space.Permission) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldOwnRole, true, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(role.String())
	})
}

// SetAclHeadId records the ACL head id from RequestJoin on the joining
// row via the local-set path (device-local; never enters the DAG). Read
// back by the joiner-side post-acceptance waiter to detect a decline.
func (s *Service) SetAclHeadId(ctx context.Context, spaceId, aclHeadId string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldAclHeadId, true, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(aclHeadId)
	})
}

// SetOneToOneInviteState writes the device-local 1-1 invite-send marker
// (FieldOneToOneInviteState) via the local-set path — never enters the
// DAG. Pass "toSend" to flag a pending notification, "" to clear it once
// the coordinator confirms delivery.
func (s *Service) SetOneToOneInviteState(ctx context.Context, spaceId, state string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldOneToOneInviteState, true, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(state)
	})
}

// AddInviteNotify queues identity into the device-local direct-add send
// outbox on spaceId's row ($addToSet on FieldInviteNotifyPending; no-op
// when already queued). Local-set path — never enters the DAG.
func (s *Service) AddInviteNotify(ctx context.Context, spaceId, identity string) error {
	if spaceId == "" || identity == "" {
		return nil
	}
	if rec, ok := s.Get(ctx, spaceId); ok {
		for _, id := range rec.InviteNotifyPending {
			if id == identity {
				return nil
			}
		}
	}
	return s.inviteNotifyOp(ctx, spaceId, identity, crdt.OpAddToSet)
}

// ClearInviteNotify removes identity from spaceId's send outbox ($pull)
// once the coordinator confirms delivery — or when the entry is
// permanently undeliverable. Local-set path.
func (s *Service) ClearInviteNotify(ctx context.Context, spaceId, identity string) error {
	if spaceId == "" || identity == "" {
		return nil
	}
	return s.inviteNotifyOp(ctx, spaceId, identity, crdt.OpPull)
}

func (s *Service) inviteNotifyOp(ctx context.Context, spaceId, identity string, op crdt.OpType) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{
			{Id: spaceId, Ops: []crdt.Op{{Type: op, Path: []string{FieldInviteNotifyPending}, Payload: arena.NewString(identity)}}},
		},
	}
	_, err = obj.LocalSet(ctx, change)
	return err
}

// SetRemoteStatus updates the SYNCED remoteStatus field — account-wide
// state that propagates to every device. Used for account-wide delete
// (status=StatusDeleted); the handler keeps deleted terminal.
func (s *Service) SetRemoteStatus(ctx context.Context, spaceId, status string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldRemoteStatus, false, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(status)
	})
}

// SetIssuedInviteKey writes (or clears, with "") the SYNCED custody of
// one issued invite key kind (IssuedKeyMember / IssuedKeyGuest) at
// [issuedInviteKeys, kind] — every device of the issuing account can
// then re-show or revoke the same invite. Per-path so kinds written on
// different devices merge instead of clobbering. Written / cleared by
// the ACL invite mint and revoke paths.
func (s *Service) SetIssuedInviteKey(ctx context.Context, spaceId, kind, encodedKey string) (object.WriteResult, error) {
	if kind != IssuedKeyMember && kind != IssuedKeyGuest {
		return object.WriteResult{}, fmt.Errorf("techspace: unknown issued-key kind %q", kind)
	}
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	arena := &anyenc.Arena{}
	op := crdt.Op{Type: crdt.OpUnset, Path: []string{FieldIssuedInviteKeys, kind}}
	if encodedKey != "" {
		op = crdt.Op{Type: crdt.OpSet, Path: op.Path, Payload: arena.NewString(encodedKey)}
	}
	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records:     []crdt.RecordChange{{Id: spaceId, Ops: []crdt.Op{op}}},
	}
	return obj.LocalWrite(ctx, change)
}

// SetGuestKey updates the SYNCED guest-mode key on an existing row —
// the re-join path after the owner rotated the guest key (JoinGuest
// with a fresh invite). Row creation writes the field via EncodeCreate.
func (s *Service) SetGuestKey(ctx context.Context, spaceId, encodedKey string) (object.WriteResult, error) {
	return s.setRowField(ctx, spaceId, FieldGuestKey, false, func(a *anyenc.Arena) *anyenc.Value {
		return a.NewString(encodedKey)
	})
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

// GetIdentity returns the full identities-directory record for an
// account identity, or (zero, false) when we've never encountered it.
func (s *Service) GetIdentity(ctx context.Context, identity string) (IdentityRecord, bool) {
	if !s.open.Load() || identity == "" {
		return IdentityRecord{}, false
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return IdentityRecord{}, false
	}
	v := obj.Controller().Get(ctx, IdentitiesDataset, identity)
	if v == nil {
		return IdentityRecord{}, false
	}
	return DecodeIdentityRecord(v), true
}

// ListIdentities returns every identities-directory row.
func (s *Service) ListIdentities(ctx context.Context) []IdentityRecord {
	if !s.open.Load() {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return nil
	}
	rows := obj.Controller().Records(ctx, IdentitiesDataset)
	out := make([]IdentityRecord, 0, len(rows))
	for _, v := range rows {
		out = append(out, DecodeIdentityRecord(v))
	}
	return out
}

// GetIdentityMetaKey returns the cached metadata symkey (string form, per
// space.MarshalSymKey) for a contact identity, or "" + false when this
// account has never received it. Account-scoped and SYNCED, so a key
// learned on any device or in any space is available here.
func (s *Service) GetIdentityMetaKey(ctx context.Context, identity string) (string, bool) {
	rec, ok := s.GetIdentity(ctx, identity)
	if !ok || rec.SymKey == "" {
		return "", false
	}
	return rec.SymKey, true
}

// SetIdentityMetaKey caches a contact's metadata symkey (string form).
// SYNCED (LocalWrite → DAG) so the account's other devices can decrypt
// that contact's profile too. Write-once in practice (the contact's
// deterministic key never changes); a no-op when already stored.
func (s *Service) SetIdentityMetaKey(ctx context.Context, identity, symKey string) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	if identity == "" || symKey == "" {
		return nil
	}
	if cur, ok := s.GetIdentityMetaKey(ctx, identity); ok && cur == symKey {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     IdentitiesDataset,
		DataVersion: IdentitiesHandlerVersion,
		Records: []crdt.RecordChange{
			{
				Id:     identity,
				Upsert: true,
				Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{FieldIdentitySymKey}, Payload: arena.NewString(symKey)}},
			},
		},
	}
	_, err = obj.LocalWrite(ctx, change)
	return err
}

// SetIdentityProfile caches a contact's resolved identityRepo profile.
// DEVICE-LOCAL (LocalSet → never synced): each device re-resolves from
// the synced symkey + identityRepo. No-op when unchanged.
func (s *Service) SetIdentityProfile(ctx context.Context, identity, name, description, iconCID string) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	if identity == "" {
		return nil
	}
	if rec, ok := s.GetIdentity(ctx, identity); ok &&
		rec.Name == name && rec.Description == description && rec.IconCID == iconCID {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set(FieldIdentityName, arena.NewString(name))
	payload.Set(FieldIdentityDescription, arena.NewString(description))
	payload.Set(FieldIdentityIcon, arena.NewString(iconCID))
	change := crdt.Change{
		Dataset:     IdentitiesDataset,
		DataVersion: IdentitiesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: identity, Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}}},
		},
	}
	_, err = obj.LocalSet(ctx, change)
	return err
}

// AddIdentitySpace records that we've seen identity in spaceId.
// DEVICE-LOCAL ($addToSet on the spaceIds set). No-op if already present.
func (s *Service) AddIdentitySpace(ctx context.Context, identity, spaceId string) error {
	if identity == "" || spaceId == "" {
		return nil
	}
	if rec, ok := s.GetIdentity(ctx, identity); ok {
		for _, id := range rec.SpaceIds {
			if id == spaceId {
				return nil
			}
		}
	}
	return s.identitySpaceOp(ctx, identity, spaceId, crdt.OpAddToSet)
}

// RemoveIdentitySpace drops spaceId from identity's sightings ($pull).
// DEVICE-LOCAL. Used when a single member leaves a space.
func (s *Service) RemoveIdentitySpace(ctx context.Context, identity, spaceId string) error {
	if identity == "" || spaceId == "" {
		return nil
	}
	return s.identitySpaceOp(ctx, identity, spaceId, crdt.OpPull)
}

func (s *Service) identitySpaceOp(ctx context.Context, identity, spaceId string, op crdt.OpType) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     IdentitiesDataset,
		DataVersion: IdentitiesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: identity, Upsert: true, Ops: []crdt.Op{{Type: op, Path: []string{FieldIdentitySpaceIds}, Payload: arena.NewString(spaceId)}}},
		},
	}
	_, err = obj.LocalSet(ctx, change)
	return err
}

// RemoveSpaceFromIdentities drops spaceId from every identity's sightings
// — used when the account leaves/offloads a space so spaceIds keeps
// reflecting live memberships. DEVICE-LOCAL.
func (s *Service) RemoveSpaceFromIdentities(ctx context.Context, spaceId string) error {
	if spaceId == "" {
		return nil
	}
	for _, rec := range s.ListIdentities(ctx) {
		for _, id := range rec.SpaceIds {
			if id == spaceId {
				_ = s.RemoveIdentitySpace(ctx, rec.Identity, spaceId)
				break
			}
		}
	}
	return nil
}

// GetInboxCursor returns the SYNCED account-wide coordinator-inbox read
// position (ObjectID-hex offset), or "" when nothing has been processed
// yet (or the value hasn't synced to this device). Account-scoped: the
// inbox notifier seeds from it so a fresh device skips already-processed
// history.
func (s *Service) GetInboxCursor(ctx context.Context) string {
	if !s.open.Load() {
		return ""
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return ""
	}
	return inboxCursorOffset(obj.Controller().Get(ctx, InboxCursorDataset, InboxCursorSelfId))
}

// SetInboxCursor advances the synced inbox read position to offset.
// SYNCED (LocalWrite → enters the DAG, propagates to the account's other
// devices). Monotonic-forward: a no-op when offset is not lexically
// greater than the current value (ObjectID-hex compares in coordinator
// order), so a stale write from a lagging device can't rewind the
// account-wide cursor. Best-effort against the rare concurrent-write race
// — a regression only costs a harmless, deduplicated re-fetch.
func (s *Service) SetInboxCursor(ctx context.Context, offset string) error {
	if !s.open.Load() {
		return errors.New("techspace: service not open")
	}
	if offset == "" || offset <= s.GetInboxCursor(ctx) {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	change := crdt.Change{
		Dataset:     InboxCursorDataset,
		DataVersion: InboxCursorHandlerVersion,
		Records: []crdt.RecordChange{
			{
				Id:     InboxCursorSelfId,
				Upsert: true,
				Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{FieldInboxCursorOffset}, Payload: arena.NewString(offset)}},
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

// SpaceRegistry adapter — the tech-space hosts the index object plus
// one account-values carrier object per target space, all served by
// the same raw Store. Every tech-space tree routes through it so the
// listener-bound, deferred-updater, cold-restored object is what the
// tree syncer touches.

var ErrSpaceRegistryUnknown = errors.New("techspace: unknown (spaceId, treeId)")

// GetTree resolves any tech-space tree via the Store. The Store
// returns the listener-bound, deferred-updater, cold-restored object
// (reloading it if ocache evicted), so the synctree's
// AddRawChangesFromPeer path fires Update → replayLocked and inbound
// changes project live — for the index object (the cold-sync path
// TestE2E_ColdSyncSameKey covers) AND the account-values carriers
// (TestE2E_AccountScopeSync): restricting this to the index id used to
// silently skip carrier trees in every headsync round, so account
// values never crossed devices.
//
// Accepting arbitrary tree ids is safe: the tech space is owner-only —
// every tree in it is this account's, and the raw Store registers the
// full tech handler set (spaces/profile/account_values) on every
// controller. An id any-sync can't resolve fails inside Store.Get and
// the syncer skips it.
func (s *Service) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	if spaceId != s.spaceId {
		return nil, ErrSpaceRegistryUnknown
	}
	obj, err := s.store.Get(ctx, treeId)
	if err != nil {
		return nil, err
	}
	tree := obj.Tree()
	if tree == nil {
		return nil, fmt.Errorf("techspace: tree %s not bound", treeId)
	}
	return tree, nil
}

// PutTree binds a remote-delivered tech-space tree payload — a carrier
// created by another of the account's devices that this device hasn't
// derived yet. (The index object is always derived locally at Open, so
// it never arrives this way, but accepting it is harmless: Derive and
// PutTree converge on the same deterministic tree.)
func (s *Service) PutTree(ctx context.Context, spaceId string, payload treestorage.TreeStorageCreatePayload) error {
	if spaceId != s.spaceId {
		return ErrSpaceRegistryUnknown
	}
	_, err := s.store.PutTreeFromPayload(ctx, payload)
	return err
}

// MarkTreeDeleted is the soft-delete hook fired when the settings tree
// announces a deletion — for the tech space that means a carrier
// object dropped by another of the account's devices. Same contract as
// the regular-space registry: drop the cached object so reads/mirrors
// stop touching it; the storage delete follows via DeleteTree.
// Unconditional like the regular path — dropping the index object
// would merely force a reload on next use.
func (s *Service) MarkTreeDeleted(_ context.Context, spaceId, treeId string) error {
	if spaceId == s.spaceId && s.store != nil {
		s.store.Drop(treeId)
	}
	return nil
}

// DeleteTree handles the deletion-manager's per-tree cleanup — fired
// for carrier objects dropped on space leave/delete. Same contract as
// the regular-space registry, with ONE deliberate exception: the index
// tree is refused. It is the account's space list — storage-deleting
// it is unrecoverable locally (the deterministic re-derive hits the
// deleted-storage mark) and no SDK path ever legitimately requests it,
// so a request can only be a bug we'd rather surface than obey.
func (s *Service) DeleteTree(ctx context.Context, spaceId, treeId string) error {
	if spaceId != s.spaceId {
		return ErrSpaceRegistryUnknown
	}
	if treeId == s.indexId {
		return fmt.Errorf("techspace: refusing to delete the index tree")
	}
	return s.store.DeleteTree(ctx, treeId)
}
