// Bundles registry — the typed API over the `bundles`
// dataset on the in-space spaceIndex object. Ensure is adopt-or-
// install; concurrent installs from different devices converge by
// CRDT (LWW winner + add-only claim set) and losers are resolved
// explicitly by the caller after merging. See space/bundles.go for
// the public contract and docs/bundles.md for the design.

package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

type bundlesAPI struct {
	parent *spaceImpl
	// mu serializes local Ensure calls (read-then-write). Cross-device
	// races are the CRDT's job; this only stops one process from
	// installing the same bundle twice in parallel.
	mu sync.Mutex
}

func newBundlesAPI(parent *spaceImpl) *bundlesAPI {
	return &bundlesAPI{parent: parent}
}

// indexObj returns the resident spaceIndex object, materializing the
// derived tree when absent (idempotent — every deriver mints the same
// tree; the owner's changes merge in when sync delivers them).
func (b *bundlesAPI) indexObj(ctx context.Context) (*object.Object, error) {
	return b.parent.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
	})
}

func (b *bundlesAPI) Ensure(ctx context.Context, req space.EnsureBundleRequest) (space.Bundle, error) {
	if req.Id == "" {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	if req.NewRoot == nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: NewRoot required", space.ErrBundleBadRequest)
	}
	if err := b.parent.writeGate(ctx); err != nil {
		return space.Bundle{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	obj, err := b.indexObj(ctx)
	if err != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: load spaceIndex object: %w", err)
	}

	// Adopt path: a live record with a winner means the bundle is
	// installed — locally-known state; the winner may still flip when
	// a concurrent remote install syncs in (callers re-read after
	// sync, see the public contract).
	if row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, req.Id); liveBundleRow(row) {
		bd := decodeBundleRecord(row)
		if bd.RootId != "" {
			b.fillLosers(ctx, &bd)
			return bd, nil
		}
	}

	rootId, err := req.NewRoot(ctx)
	if err != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: NewRoot: %w", err)
	}
	if rootId == "" {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: NewRoot returned an empty id", space.ErrBundleBadRequest)
	}
	// A derived root would make a losing install unresolvable —
	// derived trees cannot be deleted. Best-effort check: status
	// unknown (tree not present locally) passes.
	if derived, present, derr := b.parent.store.TreeIsDerived(ctx, rootId); derr == nil && present && derived {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: root %q is a derived object — bundle roots must be created, not derived", space.ErrBundleBadRequest, rootId)
	}
	// Stamp the root with the bundle name so its tree always carries a
	// non-root change: any-sync's head-sync diff skips root-only trees,
	// and an unsyncable loser root could never be resolved from another
	// device. Best-effort — an app that already wrote into the root is
	// covered either way.
	b.stampRootName(ctx, rootId, req)

	arena := &anyenc.Arena{}
	var ops []crdt.Op
	if req.Name != "" {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleName}, Payload: arena.NewString(req.Name)})
	}
	ops = append(ops,
		crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: arena.NewString(rootId)},
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: arena.NewString(rootId)},
	)
	dataVersion, err := b.parent.store.DataVersion(spaceindex.BundlesDataset)
	if err != nil {
		return space.Bundle{}, err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     spaceindex.BundlesDataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{
			{Id: req.Id, Upsert: true, Ops: ops},
		},
	})
	if err != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: write: %w", err)
	}
	if len(res.Rejections) > 0 {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: write rejected: %w", res.Rejections[0].Err)
	}

	// Return the applied state, not the input — an inbound install may
	// have landed between the read above and this write's apply.
	row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, req.Id)
	if !liveBundleRow(row) {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: record %q absent after write", req.Id)
	}
	bd := decodeBundleRecord(row)
	b.fillLosers(ctx, &bd)
	return bd, nil
}

func (b *bundlesAPI) Get(ctx context.Context, bundleId string) (space.Bundle, error) {
	if bundleId == "" {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		return space.Bundle{}, err
	}
	row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, bundleId)
	if !liveBundleRow(row) {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleUnknown, bundleId)
	}
	bd := decodeBundleRecord(row)
	b.fillLosers(ctx, &bd)
	return bd, nil
}

func (b *bundlesAPI) List(ctx context.Context) ([]space.Bundle, error) {
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		if errors.Is(err, space.ErrBundleUnknown) {
			return nil, nil
		}
		return nil, err
	}
	rows := obj.Controller().Records(ctx, spaceindex.BundlesDataset)
	out := make([]space.Bundle, 0, len(rows))
	for _, v := range rows {
		bd := decodeBundleRecord(v)
		b.fillLosers(ctx, &bd)
		out = append(out, bd)
	}
	return out, nil
}

func (b *bundlesAPI) ResolveLoser(ctx context.Context, bundleId, loserRootId string) error {
	if bundleId == "" || loserRootId == "" {
		return fmt.Errorf("spaceimpl: %w: bundle id and loser root id required", space.ErrBundleBadRequest)
	}
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		return err
	}
	row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, bundleId)
	if !liveBundleRow(row) {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleUnknown, bundleId)
	}
	bd := decodeBundleRecord(row)
	if bd.RootId == "" || loserRootId == bd.RootId || !containsString(bd.Roots, loserRootId) {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleNotLoser, loserRootId)
	}
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil // already resolved
	}
	err = b.parent.objects.Delete(ctx, loserRootId)
	if err == nil {
		return nil
	}
	// Another resolver may have raced us — re-check before surfacing.
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil
	}
	// The common cause is the loser's tree not having synced to this
	// device yet (deletion needs the local head entry). The Ensure-time
	// name stamp keeps every root tree syncable, so a retry after sync
	// succeeds.
	return fmt.Errorf("spaceimpl: bundles: delete loser %q (retry after its tree syncs): %w", loserRootId, err)
}

// stampRootName writes `any.name` on a freshly created bundle root.
// Load-bearing despite looking cosmetic: it guarantees the root tree
// carries a non-root change (see the Ensure call site). Best-effort —
// on failure the install proceeds; an app write into the root covers
// the same need.
func (b *bundlesAPI) stampRootName(ctx context.Context, rootId string, req space.EnsureBundleRequest) {
	obj, err := b.parent.store.Get(ctx, rootId)
	if err != nil {
		return
	}
	name := req.Name
	if name == "" {
		name = req.Id
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set("any.name", arena.NewString(name))
	dataVersion, err := b.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return
	}
	_, _ = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     rootId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
}

// readIndexObj is the read-path loader: no derive, no tree creation.
// A missing spaceIndex tree (legacy space never seeded, joiner before
// first sync) reads as "no registry" — mapped to ErrBundleUnknown so
// callers distinguish it from infrastructure errors.
func (b *bundlesAPI) readIndexObj(ctx context.Context) (*object.Object, error) {
	objectId, err := b.parent.parent.spaceIndexObjectIdFor(ctx, b.parent.id)
	if err != nil {
		return nil, err
	}
	obj, err := b.parent.store.Get(ctx, objectId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: bundles: %w: spaceIndex object not available: %v", space.ErrBundleUnknown, err)
	}
	return obj, nil
}

// fillLosers computes the live conflict set: every claimed root except
// the winner whose tree is not already deleted. A root with unknown
// deletion status (not present locally, or a read error) counts as
// live — ResolveLoser re-validates and tree deletion is idempotent.
func (b *bundlesAPI) fillLosers(ctx context.Context, bd *space.Bundle) {
	if bd.RootId == "" {
		return
	}
	for _, r := range bd.Roots {
		if r == bd.RootId {
			continue
		}
		if deleted, err := b.parent.store.TreeDeleted(ctx, r); err == nil && deleted {
			continue
		}
		bd.Losers = append(bd.Losers, r)
	}
}

// liveBundleRow reports whether v is a live bundles row — present and
// not a tombstone (the handler rejects deletes, but inbound changes
// from future writers are gated tolerantly, so keep the check).
func liveBundleRow(v *anyenc.Value) bool {
	return v != nil && v.Get(crdt.DeletedAtField) == nil
}

// decodeBundleRecord lifts a bundles row (as returned by
// Controller.Get / Records) into the public space.Bundle. Losers is
// left empty — fillLosers owns it.
func decodeBundleRecord(v *anyenc.Value) space.Bundle {
	if v == nil {
		return space.Bundle{}
	}
	bd := space.Bundle{
		Id:     v.GetString(crdt.IdField),
		Name:   v.GetString(spaceindex.FieldBundleName),
		RootId: v.GetString(spaceindex.FieldBundleRootId),
	}
	if arr := v.Get(spaceindex.FieldBundleRoots); arr != nil && arr.Type() == anyenc.TypeArray {
		items, _ := arr.Array()
		for _, it := range items {
			if it.Type() == anyenc.TypeString {
				bd.Roots = append(bd.Roots, string(it.GetStringBytes()))
			}
		}
	}
	return bd
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
