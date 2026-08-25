package spaceimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// spaceImpl is the per-space handle returned by Service.Create / Get.
//
// Holds a reference to the per-space spaceobjects.Store. Sub-APIs
// (Objects, Types, Properties) are constructed lazily and memoised on
// first access — they're cheap structs that delegate back to the
// store for actual work.
type spaceImpl struct {
	id     string
	app    *anysyncx.App
	tsp    *techspace.Service
	store  *spaceobjects.Store
	parent *Service
	// techIndexId is set on the inner impl of the tech-space handle:
	// the resident tech index object stands in for the per-space
	// spaceIndex object (no derive, no index watcher wiring).
	techIndexId string

	objects    *objectService
	types      *typesAPI
	properties *propertiesAPI
	acl        *aclAPI
	members    *membersAPI
	bundles    *bundlesAPI
}

func newSpace(id string, app *anysyncx.App, tsp *techspace.Service, store *spaceobjects.Store, parent *Service) *spaceImpl {
	s := &spaceImpl{id: id, app: app, tsp: tsp, store: store, parent: parent}
	s.objects = newObjectService(s)
	s.types = newTypesAPI(s)
	s.properties = newPropertiesAPI(s)
	s.acl = newACLAPI(s)
	s.members = newMembersAPI(s)
	s.bundles = newBundlesAPI(s)
	return s
}

func (s *spaceImpl) Id() string { return s.id }

// indexObjectId resolves this space's spaceIndex object: the tech
// index on the tech handle's inner impl, otherwise the per-space
// derived object (which also wires the index watcher and mirrors —
// never for the tech id).
func (s *spaceImpl) indexObjectId(ctx context.Context) (string, error) {
	if s.techIndexId != "" {
		return s.techIndexId, nil
	}
	return s.parent.spaceIndexObjectIdFor(ctx, s.id)
}

// Info reads the space-index snapshot.
//
// LocalStatus="joining" is fact-checked against the live AclList: if
// the local identity is already StatusActive in the ACL (owner accepted,
// records replicated locally) we return StatusActive without waiting
// for the tech-space record to flip. The self-heal write that updates
// the cached tech-space row lives in the members watcher
// (memberWatcher.tick), so the fix here covers the read path even when
// the watcher hasn't been started yet (e.g. caller hits Info() before
// any Members().Subscribe / Query).
func (s *spaceImpl) Info() space.SpaceInfo {
	ctx := context.Background()
	rec, ok := s.tsp.Get(ctx, s.id)
	if !ok {
		return space.SpaceInfo{Id: s.id}
	}
	info := s.parent.recordToInfo(ctx, rec)
	if info.Status == space.StatusJoining && s.localIdentityActive(ctx) {
		info.Status = space.StatusActive
		// Fire-and-forget self-heal so the next List() / Get() reads
		// the right cached status without a live AclList round-trip.
		// Members watcher does the same flip when running; this path
		// covers callers that never start the watcher.
		go s.healJoiningStatus()
	}
	return info
}

// healJoiningStatus writes LocalStatus="active" if the cached row
// still says "joining". Idempotent guard via the tsp Get; intended to
// be called from a goroutine with no return path. Failures are
// dropped — the next Info() / watcher tick retries.
func (s *spaceImpl) healJoiningStatus() {
	ctx := context.Background()
	rec, ok := s.tsp.Get(ctx, s.id)
	if !ok || rec.LocalStatus != joiningLocalStatus {
		return
	}
	_, _ = s.tsp.SetLocalStatus(ctx, s.id, techspace.StatusActive)
}

// localIdentityActive returns true when the live AclList places our
// account in StatusActive. Used by Info() to override a stale
// LocalStatus="joining" left over from before the owner's accept
// landed. Best-effort: any failure to load the space / read the ACL
// returns false (caller falls back to the cached status).
func (s *spaceImpl) localIdentityActive(ctx context.Context) bool {
	handle, err := s.app.GetSpace(ctx, s.id)
	if err != nil {
		return false
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return false
	}
	acl.RLock()
	defer acl.RUnlock()
	return selfAclActive(acl.AclState())
}

// writeGate rejects user mutations on a read-only space (guest-mode
// row, or an ACL role without write permission) with ErrReadOnlySpace.
// Reads the store's cached gate — seeded at store build, maintained by
// the ACL mirror — for entry points that mutate outside the store's
// DAG-write funnel (tree deletion, file-node uploads).
func (s *spaceImpl) writeGate(_ context.Context) error {
	return s.store.CheckWrite()
}

// canWrite reports whether this account currently has write permission
// in the space ACL. Used to gate durability takeover — only a writer may
// upload a downloaded file to the file nodes. Best-effort: any load/read
// failure returns false (we simply don't take over).
func (s *spaceImpl) canWrite(ctx context.Context) bool {
	handle, err := s.app.GetSpace(ctx, s.id)
	if err != nil {
		return false
	}
	acl := handle.Inner().Acl()
	if acl == nil {
		return false
	}
	acl.RLock()
	defer acl.RUnlock()
	state := acl.AclState()
	return state.Permissions(state.Identity()).CanWrite()
}

func (s *spaceImpl) Objects() space.ObjectService    { return s.objects }
func (s *spaceImpl) Types() space.TypesAPI           { return s.types }
func (s *spaceImpl) Properties() space.PropertiesAPI { return s.properties }

func (s *spaceImpl) ACL() space.ACL            { return s.acl }
func (s *spaceImpl) Members() space.MembersAPI { return s.members }
func (s *spaceImpl) Bundles() space.BundlesAPI { return s.bundles }

// SyncHeads forces an immediate head-sync (diff) round on this space
// instead of waiting for the periodic timer. Blocks until the round
// completes and returns its error verbatim.
func (s *spaceImpl) SyncHeads(ctx context.Context) error {
	return s.app.SyncHeads(ctx, s.id)
}

// TreeHeads reads the space frontier straight from any-sync's
// headstorage: one entry per live (non-deleted) tree — materialized
// trees and heads-only selective-sync stubs alike, system trees
// (settings) included. See space.Space.TreeHeads.
func (s *spaceImpl) TreeHeads(ctx context.Context) ([]space.TreeHeads, error) {
	handle, err := s.app.GetSpace(ctx, s.id)
	if err != nil {
		return nil, err
	}
	hs := handle.Inner().Storage().HeadStorage()
	var out []space.TreeHeads
	if err = hs.IterateEntries(ctx, headstorage.IterOpts{}, func(e headstorage.HeadsEntry) (bool, error) {
		out = append(out, space.TreeHeads{
			TreeId: e.Id,
			Heads:  slices.Clone(e.Heads),
		})
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("spaceimpl: iterate heads: %w", err)
	}
	return out, nil
}

// SyncStatus returns the per-space sync-status accessor backed by the
// account-level syncstatus.Service held on anysyncx.App. The accessor
// is a thin pointer wrapper — safe to construct on every call (no
// hidden state) but we memoise on the spaceImpl to avoid extra
// allocations when middleware polls.
func (s *spaceImpl) SyncStatus() space.SyncStatusAPI {
	return newSyncStatusAPI(s.app.SyncStatus(), s.id)
}

// Debug returns the per-space diagnostic surface. Constructed on
// every call; no hidden state. See space.DebugAPI.
func (s *spaceImpl) Debug() space.DebugAPI {
	return newDebugAPI(s)
}

// Changes returns the change-index surface (live feed + "changed since
// N" query) for consumer-side incremental indexers. Constructed on
// every call; the state lives on the spaceobjects.Store. See
// space.ChangeIndexAPI.
func (s *spaceImpl) Changes() space.ChangeIndexAPI {
	return newChangeIndexAPI(s)
}

// History returns the version-history surface for this space. See
// space.HistoryAPI and docs/version-history-proposal.md.
func (s *spaceImpl) History() space.HistoryAPI {
	return newHistoryAPI(s)
}

// ReadState exposes the read/unread tracking surface —
// space.ReadStateAPI.
func (s *spaceImpl) ReadState() space.ReadStateAPI {
	return newReadStateAPI(s)
}

// PubSub returns the ephemeral pub/sub surface for this space.
// Constructed on every call; the subscriptions live on the app-level
// engine. See space.PubSubAPI.
func (s *spaceImpl) PubSub() space.PubSubAPI {
	return NewPubSubAPI(s.app, s.id)
}

// Query builds a chainable read query against (objectId, dataset).
// The query is single-shot; call Space.Query() again per read.
func (s *spaceImpl) Query(objectId, dataset string) space.Query {
	return newQuery(s.store, objectId, dataset)
}

// QueryObjects builds a chainable query against the per-space
// `objects` collection — one row per regular object's property
// values, keyed by objectId.
func (s *spaceImpl) QueryObjects() space.Query {
	return newSharedQuery(s.store)
}

// Aggregate builds an aggregation pipeline against (objectId,
// dataset). Single-shot; call Space.Aggregate() again per read.
func (s *spaceImpl) Aggregate(objectId, dataset string, pipeline any) space.Agg {
	return newAgg(s.store, objectId, dataset, pipeline)
}

// AggregateObjects builds an aggregation pipeline against the
// per-space `objects` collection.
func (s *spaceImpl) AggregateObjects(pipeline any) space.Agg {
	return newSharedAgg(s.store, pipeline)
}

// Datasets returns the JSON-Schema description of every dataset in this
// space (discovery). See toDatasetSchemas.
func (s *spaceImpl) Datasets() []space.DatasetSchema {
	return toDatasetSchemas(s.store.Schemas())
}

// checkPublicDataset rejects writes to SDK-internal datasets through
// the public Modify/ModifyMany/Delete/Upsert surface. The payloads
// dataset is written only by the SDK's files layer (its change shapes
// are fixed and its object class ships changes unencrypted).
func checkPublicDataset(dataset string) error {
	// bundles: registry writes go through the typed BundlesAPI only — a
	// raw Modify could assert an arbitrary winner (passing the handler's
	// claim invariant) and turn the genuine root into a deletable
	// "loser", and a raw Delete would be signed into the DAG before the
	// apply-time rejection.
	if dataset == payloads.Dataset || dataset == spaceindex.BundlesDataset {
		return fmt.Errorf("spaceimpl: dataset %q is SDK-internal", dataset)
	}
	return nil
}

// checkDatasetMembership enforces the unified ownership invariant for
// type-owned datasets: an object may only hold a type's dataset if it
// implements that type (any.types ∋ owner). No-op for built-in / unknown
// datasets (DatasetOwner returns false) — property-namespace membership
// is enforced separately by SystemPropertiesHandler.PreValidate. Local
// write-time only; inbound apply stays read-tolerant.
func (s *spaceImpl) checkDatasetMembership(ctx context.Context, objectId, dataset string) error {
	owner, ok := s.store.DatasetOwner(dataset)
	if !ok {
		return nil
	}
	types, err := s.store.ObjectTypes(ctx, objectId)
	if err != nil {
		return err
	}
	for _, t := range types {
		if t == owner {
			return nil
		}
	}
	return &properties.ValidationError{
		Reason: properties.ReasonTypeNotImplemented,
		TypeId: owner,
		Types:  types,
	}
}

// localWriteRetry runs a LocalWrite, retrying once with a fresh
// resolve when the object was evicted between Get and the write — the
// lazy schema-refresh Drop (EnsureDatasetRegistered / drain) closes
// resident handles, and an in-flight caller must not surface that
// transient as a user error.
func (s *spaceImpl) localWriteRetry(ctx context.Context, obj *object.Object, objectId string, ch crdt.Change) (object.WriteResult, error) {
	res, err := obj.LocalWrite(ctx, ch)
	if !errors.Is(err, object.ErrClosed) {
		return res, err
	}
	obj, gerr := s.store.Get(ctx, objectId)
	if gerr != nil {
		return object.WriteResult{}, gerr
	}
	return obj.LocalWrite(ctx, ch)
}

// localSetRetry is localWriteRetry for the LocalSet route.
func (s *spaceImpl) localSetRetry(ctx context.Context, obj *object.Object, objectId string, ch crdt.Change) (object.WriteResult, error) {
	res, err := obj.LocalSet(ctx, ch)
	if !errors.Is(err, object.ErrClosed) {
		return res, err
	}
	obj, gerr := s.store.Get(ctx, objectId)
	if gerr != nil {
		return object.WriteResult{}, gerr
	}
	return obj.LocalSet(ctx, ch)
}

// Modify resolves the target object via the per-space store, builds a
// crdt.Change from the public batch, and submits it on the route the
// batch's Scope selects: the Object's local-write (DAG) path for
// synced (the default), Object.LocalSet for local. Returns the
// bundled identifiers.
func (s *spaceImpl) Modify(ctx context.Context, batch space.ModifyBatch) (space.ModifyResult, error) {
	if batch.ObjectId == "" {
		return space.ModifyResult{}, errors.New("spaceimpl: ObjectId required")
	}
	if batch.Dataset == "" {
		return space.ModifyResult{}, errors.New("spaceimpl: Dataset required")
	}
	if err := checkPublicDataset(batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}
	switch batch.Scope {
	case 0, space.ScopeSynced:
		// The DAG route below.
	case space.ScopeLocal:
		return s.modifyLocal(ctx, batch)
	default:
		return space.ModifyResult{}, fmt.Errorf("spaceimpl: Modify: scope %s is not writable via Modify (synced and local only)", batch.Scope)
	}
	dataVersion, err := s.store.DataVersionFor(ctx, batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
	if err := s.checkDatasetMembership(ctx, batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}

	s.store.EnsureDatasetRegistered(ctx, batch.ObjectId, batch.Dataset)
	obj, err := s.store.Get(ctx, batch.ObjectId)
	if err != nil {
		return space.ModifyResult{}, err
	}

	change, err := buildChange(batch, dataVersion)
	if err != nil {
		return space.ModifyResult{}, err
	}

	res, err := s.localWriteRetry(ctx, obj, batch.ObjectId, change)
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// modifyLocal is the ScopeLocal route of Modify: the batch is
// materialised straight into the object's rows via Object.LocalSet —
// no DAG change, nothing syncs; VersionIds come from the local lexid
// allocator and the write flows through Query/Subscribe like any
// apply (the techspace localStatus / identities pattern, opened to
// public datasets).
//
// Strict by construction: local fields annotate records that already
// exist on the synced route, so record creation is refused up front
// (explicit ids, no Upsert) rather than minting a local-only record
// no other device would ever see. Scope enforcement itself lives in
// the apply layer (classifyFieldWrite, route=local): ops targeting
// fields the schema doesn't declare ScopeLocal come back in
// ModifyResult.Rejections, exactly like handler rejections on the
// synced route, as does a strict-mode miss on an absent record
// (ErrStrictSkipAbsent).
func (s *spaceImpl) modifyLocal(ctx context.Context, batch space.ModifyBatch) (space.ModifyResult, error) {
	// The shared objects dataset is DynamicScopeByKey: the apply layer
	// exempts its undeclared heads from the route check and relies on
	// the WRITER to enforce per-property scope + kind — which for the
	// local route is PropertiesAPI.Set (validateRoutedPatch). Letting a
	// generic local batch through here would bypass that validation
	// and write synced property paths into the local version domain.
	if batch.Dataset == properties.Dataset {
		return space.ModifyResult{}, fmt.Errorf("spaceimpl: Modify: local-scope writes to the %s dataset go through Properties().Set (per-property scope enforcement)", properties.Dataset)
	}
	if len(batch.TraceIds) > 0 {
		return space.ModifyResult{}, errors.New("spaceimpl: Modify: TraceIds ride the any-sync change and are not supported on the local scope")
	}
	for i := range batch.Records {
		if batch.Records[i].Id == "" {
			return space.ModifyResult{}, fmt.Errorf("spaceimpl: Modify: record %d: local-scope writes require explicit record ids", i)
		}
		if batch.Records[i].Upsert {
			return space.ModifyResult{}, fmt.Errorf("spaceimpl: Modify: record %d: local-scope writes cannot create records (Upsert unsupported)", i)
		}
	}
	dataVersion, err := s.store.DataVersionFor(ctx, batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
	if err := s.checkDatasetMembership(ctx, batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}

	s.store.EnsureDatasetRegistered(ctx, batch.ObjectId, batch.Dataset)
	obj, err := s.store.Get(ctx, batch.ObjectId)
	if err != nil {
		return space.ModifyResult{}, err
	}

	change, err := buildChange(batch, dataVersion)
	if err != nil {
		return space.ModifyResult{}, err
	}

	res, err := s.localSetRetry(ctx, obj, batch.ObjectId, change)
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// ModifyMany pre-validates every batch up-front (against the
// target object's controller) and only proceeds with the actual
// AddContent + apply pipeline if all pass. A validation error on
// any batch fails the whole call without touching any-sync.
//
// All batches must target the same ObjectId. Datasets may differ.
// Each batch produces one any-sync DAG entry.
func (s *spaceImpl) ModifyMany(ctx context.Context, batches []space.ModifyBatch) ([]space.ModifyResult, error) {
	if len(batches) == 0 {
		return nil, errors.New("spaceimpl: ModifyMany: empty batches")
	}
	objectId := batches[0].ObjectId
	if objectId == "" {
		return nil, errors.New("spaceimpl: ModifyMany: ObjectId required")
	}
	for i := 1; i < len(batches); i++ {
		if batches[i].ObjectId != objectId {
			return nil, fmt.Errorf("spaceimpl: ModifyMany: batch %d ObjectId %q differs from batch 0 %q (cross-object batches not supported)",
				i, batches[i].ObjectId, objectId)
		}
	}
	for i := range batches {
		if batches[i].Scope != 0 && batches[i].Scope != space.ScopeSynced {
			return nil, fmt.Errorf("spaceimpl: ModifyMany: batch %d: scope %s not supported (synced only) — issue scoped batches through Modify",
				i, batches[i].Scope)
		}
	}

	for i := range batches {
		s.store.EnsureDatasetRegistered(ctx, objectId, batches[i].Dataset)
	}
	obj, err := s.store.Get(ctx, objectId)
	if err != nil {
		return nil, err
	}

	// Pre-validation pass: build all crdt.Changes and run
	// Controller.ValidateChange on each. Aggregate failures so the
	// caller can see every problem in one shot.
	changes := make([]crdt.Change, len(batches))
	var validationErrs []error
	for i, b := range batches {
		if err := checkPublicDataset(b.Dataset); err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("batch %d: %w", i, err))
			continue
		}
		dataVersion, err := s.store.DataVersionFor(ctx, b.Dataset)
		if err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("batch %d: %w", i, err))
			continue
		}
		ch, err := buildChange(b, dataVersion)
		if err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("batch %d: build: %w", i, err))
			continue
		}
		if err := s.checkDatasetMembership(ctx, objectId, b.Dataset); err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("batch %d: %w", i, err))
			continue
		}
		if err := obj.Controller().ValidateChange(ch); err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("batch %d: validate: %w", i, err))
			continue
		}
		changes[i] = ch
	}
	if len(validationErrs) > 0 {
		return nil, errors.Join(validationErrs...)
	}

	// All valid — apply each. Per-op rejections at apply time
	// still surface in each ModifyResult.Rejections.
	out := make([]space.ModifyResult, 0, len(changes))
	for i := range changes {
		res, err := s.localWriteRetry(ctx, obj, objectId, changes[i])
		if err != nil {
			return nil, fmt.Errorf("spaceimpl: ModifyMany: batch %d write: %w", i, err)
		}
		out = append(out, modifyResultFromWrite(res))
	}
	return out, nil
}

// Delete produces sticky tombstones for the listed record ids.
// Implemented as a Modify with one delete op per record.
func (s *spaceImpl) Delete(ctx context.Context, batch space.DeleteBatch) (space.ModifyResult, error) {
	if len(batch.RecordIds) == 0 {
		return space.ModifyResult{}, errors.New("spaceimpl: DeleteBatch.RecordIds empty")
	}
	if err := checkPublicDataset(batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}
	dataVersion, err := s.store.DataVersionFor(ctx, batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
	s.store.EnsureDatasetRegistered(ctx, batch.ObjectId, batch.Dataset)
	obj, err := s.store.Get(ctx, batch.ObjectId)
	if err != nil {
		return space.ModifyResult{}, err
	}

	records := make([]crdt.RecordChange, len(batch.RecordIds))
	for i, id := range batch.RecordIds {
		records[i] = crdt.RecordChange{
			Id:  id,
			Ops: []crdt.Op{{Type: crdt.OpDelete}},
		}
	}
	change := crdt.Change{
		Dataset:     batch.Dataset,
		DataVersion: dataVersion,
		TraceIds:    batch.TraceIds,
		Records:     records,
	}
	res, err := s.localWriteRetry(ctx, obj, batch.ObjectId, change)
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// buildChange converts a public space.ModifyBatch into the internal
// crdt.Change representation. Op payloads (caller-supplied `any`)
// land on a fresh anyenc arena owned by the change — the encoder
// runs inside LocalWrite before the arena goes out of scope.
func buildChange(batch space.ModifyBatch, dataVersion string) (crdt.Change, error) {
	arena := &anyenc.Arena{}
	records := make([]crdt.RecordChange, len(batch.Records))
	for i := range batch.Records {
		rec, err := buildRecord(arena, &batch.Records[i])
		if err != nil {
			return crdt.Change{}, fmt.Errorf("record %d: %w", i, err)
		}
		records[i] = rec
	}
	return crdt.Change{
		Dataset:     batch.Dataset,
		DataVersion: dataVersion,
		TraceIds:    batch.TraceIds,
		Records:     records,
	}, nil
}

func buildRecord(a *anyenc.Arena, rec *space.RecordModify) (crdt.RecordChange, error) {
	ops := make([]crdt.Op, len(rec.Ops))
	for i := range rec.Ops {
		op, err := buildOp(a, &rec.Ops[i])
		if err != nil {
			return crdt.RecordChange{}, fmt.Errorf("op %d: %w", i, err)
		}
		ops[i] = op
	}
	return crdt.RecordChange{
		Id:     rec.Id,
		Upsert: rec.Upsert,
		Ops:    ops,
	}, nil
}

func buildOp(a *anyenc.Arena, op *space.Op) (crdt.Op, error) {
	out := crdt.Op{Type: crdt.OpType(op.Type)}
	if op.Path != "" {
		out.Path = splitPath(op.Path)
	}
	if op.Value != nil {
		v, err := goToAnyenc(a, op.Value)
		if err != nil {
			return crdt.Op{}, fmt.Errorf("value: %w", err)
		}
		out.Payload = v
	}
	return out, nil
}

// splitPath breaks "a.b.c" into ["a","b","c"]. Empty input returns
// nil — the multi-field $set/$unset shape uses an empty Path.
func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	out := []string{}
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '.' {
			out = append(out, p[start:i])
			start = i + 1
		}
	}
	out = append(out, p[start:])
	return out
}

// jsonParsers backs goToAnyencJSON. A parser is held only for the
// duration of one conversion; NewFromFastJson copies every byte onto
// the arena, so nothing aliases its buffer afterwards.
var jsonParsers fastjson.ParserPool

// goToAnyenc converts a Go value into an anyenc.Value on the given
// arena. One rule on every write path: a value means what its JSON
// form means, and the Extended-JSON wrappers ({"$date": …},
// {"$binary": …}, {"$oid": …}, {"$vector": …}) are typed values — a
// record holds the same instant whether it was written from a Go map,
// a parsed HTTP body, or named in a query filter literal.
//
//   - *fastjson.Value (HTTP / JSON callers parse the body once with a
//     pooled parser) is decoded by anyenc.Arena.NewFromFastJson, which
//     owns the wrapper rule. *anyenc.Value is taken verbatim, wrapper-
//     shaped or not. Both are accepted at any depth of map[string]any
//     / []any.
//   - Go-native typed values: time.Time is a datetime (millisecond
//     precision), []byte is binary — the anyenc types the $date and
//     $binary wrappers decode to.
//   - nil, bool, string, int, int64, float64, json.Number, []string,
//     []any, []map[string]any and map[string]any are built directly:
//     numbers keep their bits and keys go in sorted order, so the
//     encoding is deterministic and matches the fastjson route byte
//     for byte (fastjson itself rounds a few exponent-form floats). A
//     nil slice or map of these kinds is an empty container. Nesting
//     deeper than fastjson.MaxDepth levels — a cycle included — is an
//     error, as on the fastjson route.
//   - A wrapper-shaped map (single key $date / $binary / $oid /
//     $vector with a payload that is not an object or a parsed value)
//     and every other Go value (structs, other slices and maps, other
//     numeric kinds) go through json.Marshal and NewFromFastJson;
//     inside those a time.Time or []byte takes the string form
//     encoding/json gives it and a nil is null. Values encoding/json
//     rejects (NaN, ±Inf, channels) return its error.
func goToAnyenc(a *anyenc.Arena, v any) (*anyenc.Value, error) {
	return goToAnyencDepth(a, v, 0)
}

func goToAnyencDepth(a *anyenc.Arena, v any, depth int) (*anyenc.Value, error) {
	if depth > fastjson.MaxDepth {
		return nil, fmt.Errorf("nesting deeper than %d levels", fastjson.MaxDepth)
	}
	switch x := v.(type) {
	case nil:
		return a.NewNull(), nil
	case *fastjson.Value:
		if x == nil {
			return a.NewNull(), nil
		}
		return a.NewFromFastJson(x), nil
	case *anyenc.Value:
		if x == nil {
			return a.NewNull(), nil
		}
		return x, nil
	case bool:
		return a.NewBool(x), nil
	case string:
		return a.NewString(x), nil
	case int:
		return a.NewNumberInt(x), nil
	case int64:
		return a.NewNumberFloat64(float64(x)), nil
	case float64:
		return newFiniteNumber(a, x)
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return nil, err
		}
		return newFiniteNumber(a, f)
	case time.Time:
		return a.NewDateTime(x), nil
	case []byte:
		return a.NewBinary(x), nil
	case []string:
		arr := a.NewArray()
		for i, s := range x {
			arr.SetArrayItem(i, a.NewString(s))
		}
		return arr, nil
	case []any:
		return goSliceToAnyenc(a, x, depth+1)
	case []map[string]any:
		return goSliceToAnyenc(a, x, depth+1)
	case map[string]any:
		if len(x) == 1 {
			for k, payload := range x {
				if isExtJSONWrapper(k, payload) {
					return goToAnyencJSON(a, x)
				}
			}
		}
		var keyBuf [16]string // stack-resident for the common small object
		keys := keyBuf[:0]
		for k := range x {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		obj := a.NewObject()
		for _, k := range keys {
			ev, err := goToAnyencDepth(a, x[k], depth+1)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", k, err)
			}
			obj.Set(k, ev)
		}
		return obj, nil
	default:
		return goToAnyencJSON(a, v)
	}
}

func goSliceToAnyenc[E any](a *anyenc.Arena, x []E, depth int) (*anyenc.Value, error) {
	arr := a.NewArray()
	for i, e := range x {
		ev, err := goToAnyencDepth(a, e, depth)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
		arr.SetArrayItem(i, ev)
	}
	return arr, nil
}

// isExtJSONWrapper reports whether a single-key map is one of the
// wrapper shapes anyenc decodes. A payload that is itself an object or
// a parsed value can never be well-formed, so that map is walked
// natively and keeps nested Go types and pass-through values.
func isExtJSONWrapper(key string, payload any) bool {
	switch key {
	case "$date", "$binary", "$oid", "$vector":
	default:
		return false
	}
	switch payload.(type) {
	case map[string]any, []map[string]any, *anyenc.Value, *fastjson.Value:
		return false
	}
	return true
}

func newFiniteNumber(a *anyenc.Arena, f float64) (*anyenc.Value, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("json: unsupported value: %v", f)
	}
	return a.NewNumberFloat64(f), nil
}

// goToAnyencJSON is the JSON route: json.Marshal, then the decoder the
// *fastjson.Value path uses.
func goToAnyencJSON(a *anyenc.Arena, v any) (*anyenc.Value, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	p := jsonParsers.Get()
	defer jsonParsers.Put(p)
	jv, err := p.ParseBytes(raw)
	if err != nil {
		return nil, err
	}
	return a.NewFromFastJson(jv), nil
}

// parseCondition is query.ParseCondition over a caller-supplied
// filter, with a Go map literal converted by goToAnyenc first: a
// time.Time or []byte in it is typed the way the record holds it and
// a {"$date": …} literal is an instant. JSON text, marshaled anyenc
// bytes, parsed values and prebuilt filters reach the parser
// untouched. An empty condition ({}, a nil map) matches everything.
func parseCondition(filter any) (query.Filter, error) {
	cond := filter
	switch filter.(type) {
	case nil, string, []byte, *fastjson.Value, *anyenc.Value, query.Filter:
	default:
		v, err := goToAnyenc(&anyenc.Arena{}, filter)
		if err != nil {
			return nil, err
		}
		cond = v
	}
	parsed, err := query.ParseCondition(cond)
	if err != nil {
		return nil, err
	}
	if parsed == nil {
		return query.All{}, nil
	}
	return parsed, nil
}
