package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

// spaceIndexTypesArray builds the any.types value for the spaceIndex
// object: the single built-in spaceIndex type it implements. Declared
// on seed so the write-time pre-flight admits the spaceIndex.*
// namespace.
func spaceIndexTypesArray(arena *anyenc.Arena) *anyenc.Value {
	arr := arena.NewArray()
	arr.SetArrayItem(0, arena.NewString(spaceindex.TypeId))
	return arr
}

// seedSpaceIndexOnCreate is the owner-side initial write of the
// in-space spaceIndex object. Runs once during Service.Create after
// the space header lands and ensureSpaceIndexWiring has cached the
// objectId. Materialises the spaceIndex tree (deterministic derive)
// and stamps the four properties from CreateRequest in one multi-
// field $set. The watcher's apply-time mirror then idempotently
// overwrites the tech-space row this device just seeded via
// tsp.Add — that round-trip is cheap (same DB) and keeps the two
// sides eventually identical.
func (s *Service) seedSpaceIndexOnCreate(ctx context.Context, store *spaceobjects.Store, spaceId string, req space.CreateRequest, normalizedSpaceType string) error {
	// Derive the spaceIndex object — idempotent on the any-sync side
	// (a second Create with the same seed returns the existing tree).
	// ChangeType intentionally left empty: any-sync's DeriveTree
	// hashes the full payload (including ChangeType) into the tree id,
	// so it must match what ensureSpaceIndexWiring derives with.
	obj, err := store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
	})
	if err != nil {
		return fmt.Errorf("derive spaceIndex tree: %w", err)
	}

	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	// Always set all four keys, even when empty — keeps the row's
	// `spaceIndex.*` namespace present on disk so subsequent reads
	// distinguish "seeded with blanks" from "never seeded".
	// Declare the type this object implements so the write-time schema
	// pre-flight admits the spaceIndex.* namespace (any.types
	// membership). Same change as the values — collectTypeAdditions
	// picks it up.
	payload.Set("any.types", spaceIndexTypesArray(arena))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldName, arena.NewString(req.Name))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldDescription, arena.NewString(req.Description))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldIcon, arena.NewString(req.IconCID))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldSpaceType, arena.NewString(normalizedSpaceType))

	dataVersion, err := store.DataVersion(properties.Dataset)
	if err != nil {
		return fmt.Errorf("dataVersion: %w", err)
	}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     obj.Id(),
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
	if err != nil {
		return fmt.Errorf("write initial properties: %w", err)
	}
	return nil
}

// maybeLazySeedSpaceIndex is the post-load reconciliation that covers
// pre-existing spaces (created before this code shipped) and any
// space where the initial owner-side seed never completed (crash
// after CreateSpace but before LocalWrite). Runs in a background
// goroutine spawned by Service.Get / Derive / OneToOne.
//
// Strictly owner-gated: non-owners do nothing — they wait for the
// owner's eventual write to propagate via the normal sync path.
// Writing from a non-owner would be authoritative-looking but
// stale (the joiner's local tech-space row holds whatever the
// joiner typed locally, not the truth-of-the-space), so we skip.
//
// Best-effort: every step that fails is silently dropped. The
// background goroutine has no return path, and the next load
// retries the whole sequence. Logging is deferred — none of the
// failure modes here are actionable from a single space load.
func (s *spaceImpl) maybeLazySeedSpaceIndex(ctx context.Context) {
	if !s.localIdentityIsOwner(ctx) {
		return
	}

	objectId, err := s.parent.spaceIndexObjectIdFor(ctx, s.id)
	if err != nil || objectId == "" {
		return
	}

	// Already seeded? The row's spaceIndex namespace exists once
	// LocalWrite has applied — even with empty strings (seed-with-
	// blanks is still a seed). We only fill in when the namespace is
	// entirely missing.
	if spaceIndexHasNamespace(ctx, s.store, objectId) {
		return
	}

	// The local copy has no spaceIndex namespace — but that does NOT mean
	// the object is absent network-wide. A second device of the same
	// account (every device is an owner) loading a space another device
	// created races here: the spaceIndex object exists and may already be
	// renamed, it just hasn't synced to this device yet. Re-seeding from
	// THIS device's stale tech-space row would derive the same object id
	// with a fresh OrderId and win LWW, reverting the real name on every
	// device (the "rename never converges" flake). So sync first and
	// re-check; only seed when the object is genuinely absent everywhere
	// (a legacy pre-spaceIndex space, or an owner that crashed after
	// CreateSpace but before the initial seed LocalWrite). If the sync
	// fails we can't prove absence — skip and let the next load retry,
	// rather than risk the clobber.
	if err := s.app.SyncHeads(ctx, s.id); err != nil {
		return
	}
	if spaceIndexHasNamespace(ctx, s.store, objectId) {
		return
	}

	// Pull the tech-space row to source the initial values. This is
	// the legacy migration path — the tech-space row is what the
	// owner created with via the pre-spaceIndex code.
	rec, ok := s.tsp.Get(ctx, s.id)
	if !ok {
		return
	}

	obj, err := s.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
	})
	if err != nil {
		return
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set("any.types", spaceIndexTypesArray(arena))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldName, arena.NewString(rec.Name))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldDescription, arena.NewString(rec.Description))
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldIcon, arena.NewString(rec.IconCID))
	// spaceIndex.spaceType carries the app-level tag (rec.SpaceType), not
	// the on-wire header type. Fall back to rec.Type for legacy rows
	// written before the SpaceType column existed.
	payload.Set(spaceindex.TypeId+"."+spaceindex.FieldSpaceType, arena.NewString(lazySpaceTypeTag(rec)))

	dataVersion, err := s.store.DataVersion(properties.Dataset)
	if err != nil {
		return
	}
	_, _ = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     obj.Id(),
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
}

// lazySpaceTypeTag resolves the app-level spaceType to seed into the
// in-space spaceIndex object. Prefers the SpaceType column; falls back to
// the header type (Type) for legacy rows written before SpaceType existed.
func lazySpaceTypeTag(rec techspace.SpaceIndexRecord) string {
	if rec.SpaceType != "" {
		return rec.SpaceType
	}
	return rec.Type
}

// SetMetadata writes the spaceIndex object's properties for the
// caller-supplied fields. Non-nil pointers are written (including
// empty strings); nil pointers leave the existing value alone. The
// resulting LocalWrite carries one multi-field $set so all three
// columns land under one VersionId.
//
// No permission gate in v1 — non-writers are rejected by any-sync
// ACL on the apply path at peers. See PROMPT.md § "Open questions".
func (s *spaceImpl) SetMetadata(ctx context.Context, req space.SetMetadataRequest) error {
	if req.Name == nil && req.Description == nil && req.IconCID == nil {
		return errors.New("spaceimpl: SetMetadata: at least one field required")
	}
	objectId, err := s.indexObjectId(ctx)
	if err != nil {
		return err
	}
	obj, err := s.store.Get(ctx, objectId)
	if err != nil {
		return fmt.Errorf("spaceimpl: SetMetadata: load spaceIndex object: %w", err)
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	if req.Name != nil {
		payload.Set(spaceindex.TypeId+"."+spaceindex.FieldName, arena.NewString(*req.Name))
	}
	if req.Description != nil {
		payload.Set(spaceindex.TypeId+"."+spaceindex.FieldDescription, arena.NewString(*req.Description))
	}
	if req.IconCID != nil {
		payload.Set(spaceindex.TypeId+"."+spaceindex.FieldIcon, arena.NewString(*req.IconCID))
	}
	dataVersion, err := s.store.DataVersion(properties.Dataset)
	if err != nil {
		return err
	}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
	if err != nil {
		return fmt.Errorf("spaceimpl: SetMetadata: write: %w", err)
	}
	return nil
}

// SpaceIndexObjectId returns the deterministic spaceIndex object id
// for this space. Returns "" only when the underlying derive failed
// to compute (rare — usually a programming error if hit).
func (s *spaceImpl) SpaceIndexObjectId() string {
	// Prefer the cached value; fall back to an on-demand derive.
	id, err := s.indexObjectId(context.Background())
	if err != nil {
		return ""
	}
	return id
}

// WaitIndexSynced blocks until the local view of the spaceIndex is
// trustworthy, via either exit:
//
//   - Fast path (offline-capable): the seeded metadata row is already
//     projected locally — regular spaces get it at Create.
//   - Converged path: a clean head-sync round with the sync-status
//     rollup at Synced. Covers spaces that never seed metadata (1-1 /
//     nameless derived spaces): after a converged round the network's
//     current index state is local — including "the index tree exists
//     nowhere yet", a valid converged answer (callers then read an
//     empty registry), NOT a wait-forever.
//
// Read-only: the tree is loaded (Store.Get runs ColdRestore, so
// pre-boot head-synced state projects too), never derived — a wait
// primitive must not create trees, least of all on read-only/guest
// spaces where the write gate would refuse the same effect.
func (s *spaceImpl) WaitIndexSynced(ctx context.Context) error {
	objectId, err := s.indexObjectId(ctx)
	if err != nil {
		return err
	}
	var lastErr error
	seeded := func(ctx context.Context) bool {
		if _, gerr := s.store.Get(ctx, objectId); gerr != nil {
			lastErr = gerr // tree not arrived yet, or a real failure — keep waiting, report on timeout
			return false
		}
		return spaceIndexHasNamespace(ctx, s.store, objectId)
	}
	if seeded(ctx) {
		return nil
	}
	backoff := time.Second
	for {
		serr := s.app.SyncHeads(ctx, s.id)
		if serr != nil {
			lastErr = serr // offline round — keep waiting; the caller's ctx is the only deadline
		}
		if seeded(ctx) {
			return nil
		}
		// Converged exit: a clean round + Synced rollup means the local
		// index state IS the network's (a nil round alone is not proof —
		// any-sync swallows per-peer failures; the rollup flips Synced
		// only on HeadsApply from a responsible peer). Absent tree after
		// convergence = index exists nowhere = done.
		if serr == nil && s.app.SyncStatus().Status(s.id).State == space.SyncStateSynced {
			_, gerr := s.store.Get(ctx, objectId)
			if gerr == nil || errors.Is(gerr, treestorage.ErrUnknownTreeId) {
				return nil
			}
			lastErr = gerr
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w (last attempt: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// spaceIndexHasNamespace returns true when the per-space `objects`
// collection holds a row at spaceIndexObjectId carrying a
// `record.spaceIndex` namespace (i.e. at least one prior LocalWrite
// has applied — empty fields still count). Absent row / tombstone /
// no namespace returns false.
func spaceIndexHasNamespace(ctx context.Context, store *spaceobjects.Store, spaceIndexObjectId string) bool {
	coll, err := store.SharedObjects(ctx)
	if err != nil {
		return false
	}
	doc, err := coll.FindId(ctx, spaceIndexObjectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false
		}
		return false
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return false
	}
	return v.Get(spaceindex.TypeId) != nil
}

// localIdentityIsOwner reports whether the local account holds the
// Owner permission on this space's ACL. Best-effort: any failure to
// load the space / read the ACL returns false (caller falls back to
// the safer no-op path).
//
// Parallel to localIdentityActive in space.go — kept here so the
// spaceIndex code is grouped on disk.
func (s *spaceImpl) localIdentityIsOwner(ctx context.Context) bool {
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
			return acc.Permissions == list.AclPermissionsOwner
		}
	}
	return false
}
