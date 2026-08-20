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
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	"github.com/anyproto/any-sync-sdk/space"
)

type bundlesAPI struct {
	parent *spaceImpl
	// locks serialize local Ensure calls per bundle id (read-then-
	// write). Cross-device races are the CRDT's job; this only stops
	// one process from installing the same bundle twice in parallel.
	// Per-id — NewRoot runs under its own bundle's lock only, so a
	// composite install may Ensure OTHER bundles from inside NewRoot
	// and a slow NewRoot doesn't stall unrelated installs. Same-id
	// reentry from NewRoot still self-deadlocks: don't.
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newBundlesAPI(parent *spaceImpl) *bundlesAPI {
	return &bundlesAPI{parent: parent, locks: map[string]*sync.Mutex{}}
}

func (b *bundlesAPI) lockFor(bundleId string) *sync.Mutex {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.locks[bundleId]
	if l == nil {
		l = &sync.Mutex{}
		b.locks[bundleId] = l
	}
	return l
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
	l := b.lockFor(req.Id)
	l.Lock()
	defer l.Unlock()

	obj, err := b.indexObj(ctx)
	if err != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: load spaceIndex object: %w", err)
	}

	// Adopt path: a live record whose winner's tree is not deleted
	// means the bundle is installed — locally-known state; the winner
	// may still flip when a concurrent remote install syncs in
	// (callers re-read after sync, see the public contract). A DELETED
	// winner reads as uninstalled — otherwise the bundle id would be
	// permanently wedged (record deletes are rejected, rootId has no
	// unset, ResolveLoser refuses the winner) — and falls through to a
	// fresh install.
	if row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, req.Id); liveBundleRow(row) {
		bd := decodeBundleRecord(row)
		if bd.RootId != "" && !b.winnerDeleted(ctx, &bd) {
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
	// derived trees cannot be deleted. Fail closed: the honest path
	// (Objects().Create in this space) materializes storage
	// synchronously, so an absent entry or a read error means the input
	// is wrong, and letting it through would plant a permanently
	// unresolvable Losers entry on every device if it loses a race.
	derived, present, derr := b.parent.store.TreeIsDerived(ctx, rootId)
	if derr != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: verify root %q: %w", rootId, derr)
	}
	if !present {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: root %q has no local tree — create it with Objects().Create in this space", space.ErrBundleBadRequest, rootId)
	}
	if derived {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: root %q is a derived object — bundle roots must be created, not derived", space.ErrBundleBadRequest, rootId)
	}
	// Stamp the root with the bundle name so its tree always carries a
	// non-root change: any-sync's head-sync diff skips root-only trees,
	// and an unsyncable loser root could never be resolved from another
	// device. Must succeed BEFORE the registering write — registering
	// an unstamped (possibly forever-unsyncable) root would plant a
	// loser no other device can ever resolve.
	if err := b.stampRootName(ctx, rootId, req); err != nil {
		return space.Bundle{}, fmt.Errorf("spaceimpl: bundles: stamp root %q: %w", rootId, err)
	}

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
	// A deleted winner = uninstalled (matches Ensure's adopt gate) —
	// reporting the dead RootId as live would send callers deriving
	// children from a tombstoned parent.
	if bd.RootId == "" || b.winnerDeleted(ctx, &bd) {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: %q (no live install)", space.ErrBundleUnknown, bundleId)
	}
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
		if bd.RootId == "" || b.winnerDeleted(ctx, &bd) {
			continue // uninstalled — same gate as Get
		}
		b.fillLosers(ctx, &bd)
		out = append(out, bd)
	}
	return out, nil
}

// winnerDeleted reports whether the record's winning root tree is
// deleted. Unknown status (read error) counts as live — a spurious
// "uninstalled" would trigger a duplicate install, which is worse than
// briefly adopting a dead winner.
func (b *bundlesAPI) winnerDeleted(ctx context.Context, bd *space.Bundle) bool {
	deleted, err := b.parent.store.TreeDeleted(ctx, bd.RootId)
	return err == nil && deleted
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
	if bd.RootId == "" || loserRootId == bd.RootId || !slices.Contains(bd.Roots, loserRootId) {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleNotLoser, loserRootId)
	}
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil // already resolved
	}
	// Deletion needs the loser's local head entry (any-sync settings
	// delete has no delete-by-id) — absent means "not synced here yet",
	// a retryable state with its own sentinel, not a storage error.
	if _, present, perr := b.parent.store.TreeIsDerived(ctx, loserRootId); perr == nil && !present {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrLoserNotSynced, loserRootId)
	}
	err = b.parent.objects.Delete(ctx, loserRootId)
	if err == nil {
		return nil
	}
	// Another resolver may have raced us — re-check before surfacing.
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil
	}
	// Entry raced away between the probe above and the delete — same
	// retryable state.
	if _, present, perr := b.parent.store.TreeIsDerived(ctx, loserRootId); perr == nil && !present {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrLoserNotSynced, loserRootId)
	}
	return fmt.Errorf("spaceimpl: bundles: delete loser %q: %w", loserRootId, err)
}

// stampRootName writes `any.name` on a freshly created bundle root.
// Load-bearing despite looking cosmetic: it guarantees the root tree
// carries a non-root change (see the Ensure call site).
func (b *bundlesAPI) stampRootName(ctx context.Context, rootId string, req space.EnsureBundleRequest) error {
	obj, err := b.parent.store.Get(ctx, rootId)
	if err != nil {
		return err
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
		return err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     rootId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
	if err != nil {
		return err
	}
	if len(res.Rejections) > 0 {
		return fmt.Errorf("write rejected: %w", res.Rejections[0].Err)
	}
	return nil
}

// readIndexObj is the read-path loader: no derive, no tree creation.
// ONLY a genuinely missing spaceIndex tree (legacy space never seeded,
// joiner before first sync) reads as "no registry" / ErrBundleUnknown;
// every other failure (ctx cancellation, closed DB, poisoned ocache
// load) propagates as an infrastructure error — flattening those to
// "unknown" would make List report an empty registry and callers
// re-run setup, minting a duplicate root.
func (b *bundlesAPI) readIndexObj(ctx context.Context) (*object.Object, error) {
	objectId, err := b.parent.parent.spaceIndexObjectIdFor(ctx, b.parent.id)
	if err != nil {
		return nil, err
	}
	obj, err := b.parent.store.Get(ctx, objectId)
	if err != nil {
		if errors.Is(err, treestorage.ErrUnknownTreeId) {
			return nil, fmt.Errorf("spaceimpl: bundles: %w: spaceIndex tree not present locally: %v", space.ErrBundleUnknown, err)
		}
		return nil, fmt.Errorf("spaceimpl: bundles: load spaceIndex object: %w", err)
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
