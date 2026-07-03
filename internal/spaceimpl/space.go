package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
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

	objects    *objectService
	types      *typesAPI
	properties *propertiesAPI
	acl        *aclAPI
	members    *membersAPI
}

func newSpace(id string, app *anysyncx.App, tsp *techspace.Service, store *spaceobjects.Store, parent *Service) *spaceImpl {
	s := &spaceImpl{id: id, app: app, tsp: tsp, store: store, parent: parent}
	s.objects = newObjectService(s)
	s.types = newTypesAPI(s)
	s.properties = newPropertiesAPI(s)
	s.acl = newACLAPI(s)
	s.members = newMembersAPI(s)
	return s
}

func (s *spaceImpl) Id() string { return s.id }

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
	state := acl.AclState()
	me := state.Identity()
	for _, acc := range state.CurrentAccounts() {
		if acc.PubKey.Equals(me) {
			return acc.Status == list.StatusActive
		}
	}
	return false
}

func (s *spaceImpl) Objects() space.ObjectService    { return s.objects }
func (s *spaceImpl) Types() space.TypesAPI           { return s.types }
func (s *spaceImpl) Properties() space.PropertiesAPI { return s.properties }

func (s *spaceImpl) ACL() space.ACL            { return s.acl }
func (s *spaceImpl) Members() space.MembersAPI { return s.members }

// SyncHeads forces an immediate head-sync (diff) round on this space
// instead of waiting for the periodic timer. Blocks until the round
// completes and returns its error verbatim.
func (s *spaceImpl) SyncHeads(ctx context.Context) error {
	return s.app.SyncHeads(ctx, s.id)
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

// ReadState exposes the read/unread tracking surface —
// space.ReadStateAPI.
func (s *spaceImpl) ReadState() space.ReadStateAPI {
	return newReadStateAPI(s)
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
	switch batch.Scope {
	case 0, space.ScopeSynced:
		// The DAG route below.
	case space.ScopeLocal:
		return s.modifyLocal(ctx, batch)
	default:
		return space.ModifyResult{}, fmt.Errorf("spaceimpl: Modify: scope %s is not writable via Modify (synced and local only)", batch.Scope)
	}
	dataVersion, err := s.store.DataVersion(batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
	if err := s.checkDatasetMembership(ctx, batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}

	obj, err := s.store.Get(ctx, batch.ObjectId)
	if err != nil {
		return space.ModifyResult{}, err
	}

	change, err := buildChange(batch, dataVersion)
	if err != nil {
		return space.ModifyResult{}, err
	}

	res, err := obj.LocalWrite(ctx, change)
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
	dataVersion, err := s.store.DataVersion(batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
	if err := s.checkDatasetMembership(ctx, batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}

	obj, err := s.store.Get(ctx, batch.ObjectId)
	if err != nil {
		return space.ModifyResult{}, err
	}

	change, err := buildChange(batch, dataVersion)
	if err != nil {
		return space.ModifyResult{}, err
	}

	res, err := obj.LocalSet(ctx, change)
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
		dataVersion, err := s.store.DataVersion(b.Dataset)
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
		res, err := obj.LocalWrite(ctx, changes[i])
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
	dataVersion, err := s.store.DataVersion(batch.Dataset)
	if err != nil {
		return space.ModifyResult{}, err
	}
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
	res, err := obj.LocalWrite(ctx, change)
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

// goToAnyenc converts a Go value into an anyenc.Value on the given
// arena. Accepts:
//
//   - Native Go types (string, bool, ints, float64, []byte, []any,
//     []string, map[string]any) for in-process callers.
//   - *fastjson.Value for HTTP / JSON callers — they parse the
//     request body once with a pooled fastjson.Parser, then hand the
//     parsed values straight through. anyenc.Arena.NewFromFastJson
//     does the conversion in one walk on our arena, no Go-native
//     intermediate.
//   - *anyenc.Value passes through unchanged (already on the right
//     arena, or cross-arena — caller's responsibility).
func goToAnyenc(a *anyenc.Arena, v any) (*anyenc.Value, error) {
	switch x := v.(type) {
	case nil:
		return a.NewNull(), nil
	case *fastjson.Value:
		if x == nil {
			return a.NewNull(), nil
		}
		return a.NewFromFastJson(x), nil
	case *anyenc.Value:
		return x, nil
	case bool:
		if x {
			return a.NewTrue(), nil
		}
		return a.NewFalse(), nil
	case string:
		return a.NewString(x), nil
	case int:
		return a.NewNumberInt(x), nil
	case int64:
		return a.NewNumberInt(int(x)), nil
	case float64:
		return a.NewNumberFloat64(x), nil
	case []byte:
		return a.NewBinary(x), nil
	case []any:
		arr := a.NewArray()
		for i, e := range x {
			ev, err := goToAnyenc(a, e)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			arr.SetArrayItem(i, ev)
		}
		return arr, nil
	case []string:
		arr := a.NewArray()
		for i, e := range x {
			arr.SetArrayItem(i, a.NewString(e))
		}
		return arr, nil
	case map[string]any:
		obj := a.NewObject()
		for k, vv := range x {
			ev, err := goToAnyenc(a, vv)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", k, err)
			}
			obj.Set(k, ev)
		}
		return obj, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", v)
	}
}
