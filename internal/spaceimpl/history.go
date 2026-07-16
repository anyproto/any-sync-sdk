package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/history"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/space"
)

// historyAPI implements space.HistoryAPI (proposal §7) on top of the
// two history engines: on-demand causal replay (internal/history
// replay.go/record.go) for states and diffs, and the persistent index
// (index.go) for listing and filtering. All methods lazily backfill a
// stale index before serving it (§4.4).
type historyAPI struct {
	parent *spaceImpl
}

func newHistoryAPI(s *spaceImpl) *historyAPI { return &historyAPI{parent: s} }

// viewParams assembles ViewParams for one object from the live tree
// and the store's current handler set.
func (h *historyAPI) viewParams(ctx context.Context, objectId string) (*object.Object, history.ViewParams, error) {
	obj, err := h.parent.store.Get(ctx, objectId)
	if err != nil {
		return nil, history.ViewParams{}, fmt.Errorf("history: load %s: %w", objectId, err)
	}
	tree := obj.Tree()
	if tree == nil {
		return nil, history.ViewParams{}, fmt.Errorf("history: tree not bound for %s", objectId)
	}
	regs, sharedNames, err := h.parent.store.HistoryReplayRegs()
	if err != nil {
		return nil, history.ViewParams{}, err
	}
	return obj, history.ViewParams{
		ObjectId:       objectId,
		Regs:           regs,
		SharedDatasets: sharedNames,
	}, nil
}

// hasVersion checks the version exists in local tree storage under the
// tree lock (cheap map lookup) so unknown versions map to
// ErrVersionNotFound instead of an opaque tree-build failure.
func hasVersion(tree objecttree.ObjectTree, version space.Version) bool {
	tree.Lock()
	defer tree.Unlock()
	return tree.HasChanges(version)
}

// buildTreeLocked builds a history tree over the LIVE tree's storage
// under that tree's lock. The build is the only storage-touching phase
// of a history operation; holding the lock (a) serializes access to
// the storage's shared, non-thread-safe anyenc parser with the apply
// path, and (b) fences ocache eviction for the scan's duration —
// Object.TryClose needs the same lock, and tree.Close would close the
// storage the scan reads. Iteration afterwards walks the in-memory
// history tree and needs no lock. Empty heads = full tree (backfill).
func buildTreeLocked(tree objecttree.ObjectTree, heads []string) (objecttree.HistoryTree, error) {
	tree.Lock()
	defer tree.Unlock()
	return objecttree.BuildNonVerifiableHistoryTree(objecttree.HistoryTreeParams{
		Storage:         tree.Storage(),
		AclList:         tree.AclList(),
		Heads:           heads,
		IncludeBeforeId: true,
	})
}

// indexFresh reports whether the object's index can be served as-is:
// not stale (cold restore skipped the warm path) and known (object
// does not predate the history feature).
func (h *historyAPI) indexFresh(ctx context.Context, ix *history.Index, objectId string) (bool, error) {
	stale, err := ix.IsStale(ctx, objectId)
	if err != nil || stale {
		return false, err
	}
	return ix.HasState(ctx, objectId)
}

// ensureFresh lazily backfills the object's index when it is not
// fresh. Inline and batched; §4.4.
func (h *historyAPI) ensureFresh(ctx context.Context, ix *history.Index, obj *object.Object, objectId string) error {
	fresh, err := h.indexFresh(ctx, ix, objectId)
	if err != nil {
		return err
	}
	if fresh {
		return nil
	}
	// Full-tree walk (empty Heads = every change), decrypting through
	// the object's key machinery; batched tx writes inside Backfill.
	// Synchronous by design in v1 — the async/progress contract is
	// deferred (see internal/history/index.go note).
	histTree, err := buildTreeLocked(obj.Tree(), nil)
	if err != nil {
		return mapHistoryErr(err)
	}
	return ix.Backfill(ctx, histTree, objectId, 0)
}

func (h *historyAPI) ListChanges(ctx context.Context, objectId string, f space.HistoryFilter, limit int, cursor string) (space.ChangeList, error) {
	if objectId == "" {
		return space.ChangeList{}, errors.New("history: objectId required")
	}
	obj, err := h.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ChangeList{}, fmt.Errorf("history: load %s: %w", objectId, err)
	}
	ix, err := h.parent.store.HistoryIndex(ctx)
	if err != nil {
		return space.ChangeList{}, err
	}
	if err := h.ensureFresh(ctx, ix, obj, objectId); err != nil {
		return space.ChangeList{}, err
	}
	if limit <= 0 {
		limit = history.DefaultListLimit
	}

	filter := history.Filter{
		ObjectId: objectId,
		Dataset:  f.Dataset,
		RecordId: f.RecordId,
		TraceId:  f.TraceId,
		Author:   f.Author,
	}

	if f.Coalesce == nil {
		metas, next, lerr := ix.ListChanges(ctx, filter, limit, cursor)
		if lerr != nil {
			return space.ChangeList{}, lerr
		}
		return space.ChangeList{Changes: publicMetas(metas), Cursor: next}, nil
	}

	// Coalesced listing: raw pages shrink, so keep fetching until we
	// have `limit` groups or history is exhausted. A group whose chain
	// continues past the fetched window splits at the window boundary —
	// a display-level artifact, the causal handles stay correct.
	opts := history.CoalesceOpts{Window: f.Coalesce.Window}
	var groups []history.ChangeMeta
	cur := cursor
	for {
		raw, next, lerr := ix.ListChanges(ctx, filter, limit*3, cur)
		if lerr != nil {
			return space.ChangeList{}, lerr
		}
		groups = append(groups, history.Coalesce(raw, opts)...)
		cur = next
		if cur == "" || len(groups) >= limit {
			break
		}
	}
	nextCursor := cur
	if len(groups) > limit {
		groups = groups[:limit]
		// Resume after the oldest member of the last kept group.
		nextCursor = groups[len(groups)-1].OrderId
	}
	return space.ChangeList{Changes: publicMetas(groups), Cursor: nextCursor}, nil
}

func (h *historyAPI) ViewAt(ctx context.Context, objectId string, version space.Version) (space.HistoricalView, error) {
	if version == "" {
		return nil, errors.New("history: version required")
	}
	obj, params, err := h.viewParams(ctx, objectId)
	if err != nil {
		return nil, err
	}
	if !hasVersion(obj.Tree(), version) {
		return nil, fmt.Errorf("%w: %s", space.ErrVersionNotFound, version)
	}
	tree, err := buildTreeLocked(obj.Tree(), []string{version})
	if err != nil {
		return nil, mapHistoryErr(err)
	}
	params.Heads = []string{version}
	params.Tree = tree
	view, err := history.BuildView(ctx, params)
	if err != nil {
		return nil, mapHistoryErr(err)
	}
	return &historicalView{view: view}, nil
}

func (h *historyAPI) RecordAt(ctx context.Context, objectId, dataset, recordId string, version space.Version) (*anyenc.Value, error) {
	if version == "" {
		return nil, errors.New("history: version required")
	}
	obj, params, err := h.viewParams(ctx, objectId)
	if err != nil {
		return nil, err
	}
	if !hasVersion(obj.Tree(), version) {
		return nil, fmt.Errorf("%w: %s", space.ErrVersionNotFound, version)
	}

	// Index-fed pre-filter, but only off an already-fresh index; nil
	// (decode-and-filter fallback) otherwise — RecordAt stays correct
	// either way, only the constant factor changes. Deliberately NOT
	// ensureFresh: a synchronous full-tree backfill costs more than
	// the fallback replay it would optimize.
	var touched []string
	if ix, ixErr := h.parent.store.HistoryIndex(ctx); ixErr == nil {
		if fresh, fErr := h.indexFresh(ctx, ix, objectId); fErr == nil && fresh {
			touched, _ = ix.TouchedChangeIds(ctx, objectId, dataset, recordId)
		}
	}

	tree, err := buildTreeLocked(obj.Tree(), []string{version})
	if err != nil {
		return nil, mapHistoryErr(err)
	}
	rec, err := history.RecordAt(ctx, history.RecordAtParams{
		ObjectId:         objectId,
		Dataset:          dataset,
		RecordId:         recordId,
		Version:          version,
		Tree:             tree,
		Regs:             params.Regs,
		SharedDatasets:   params.SharedDatasets,
		TouchedChangeIds: touched,
	})
	if err != nil {
		return nil, mapHistoryErr(err)
	}
	return rec, nil
}

func (h *historyAPI) Diff(ctx context.Context, objectId string, base, version space.Version, f space.DiffFilter) (space.DiffResult, error) {
	if version == "" {
		return space.DiffResult{}, errors.New("history: version required")
	}
	obj, params, err := h.viewParams(ctx, objectId)
	if err != nil {
		return space.DiffResult{}, err
	}
	tree := obj.Tree()
	if !hasVersion(tree, version) {
		return space.DiffResult{}, fmt.Errorf("%w: %s", space.ErrVersionNotFound, version)
	}

	// One locked build of version's history tree serves everything
	// below: PrevIds resolution, the ancestor walk, and the replay.
	versionTree, err := buildTreeLocked(tree, []string{version})
	if err != nil {
		return space.DiffResult{}, mapHistoryErr(err)
	}

	// base == "": per-change effect diff against version's parents,
	// resolved off the (private) history tree — no live-tree lock.
	baseHeads := []string{base}
	if base == "" {
		ch, gerr := versionTree.GetChange(version)
		if gerr != nil || ch == nil {
			return space.DiffResult{}, fmt.Errorf("%w: %s", space.ErrVersionNotFound, version)
		}
		baseHeads = ch.PreviousIds
	} else if !hasVersion(tree, base) {
		return space.DiffResult{}, fmt.Errorf("%w: %s", space.ErrVersionNotFound, base)
	}
	if len(baseHeads) == 1 && baseHeads[0] == tree.Id() {
		baseHeads = nil // first content change: diff against the empty projection
	}

	params.Dataset = f.Dataset
	filter := history.DiffFilter{Dataset: f.Dataset, RecordIds: f.RecordIds}

	// Single-replay fast path (proposal §5): base is an ancestor of
	// version in the common "what changed since" case and always for
	// effect diffs (PrevIds are ancestors by construction).
	rp := params
	rp.Tree = versionTree
	res, err := history.DiffRange(ctx, rp, baseHeads, version, filter)
	if errors.Is(err, history.ErrNotAncestor) {
		// Concurrent versions: two-way diff of the two causal pasts.
		// versionTree is reusable here — ErrNotAncestor comes from the
		// ancestor walk, which runs before DiffRange's (single-walk)
		// iteration ever starts.
		res, err = h.diffConcurrent(ctx, tree, params, baseHeads, versionTree, version, filter)
	}
	if err != nil {
		return space.DiffResult{}, mapHistoryErr(err)
	}

	out := space.DiffResult{Base: base, Version: version}
	for _, dd := range res.Datasets {
		pd := space.DatasetDiff{Dataset: dd.Dataset}
		for _, rd := range dd.Records {
			pr := space.RecordDiff{Id: rd.Id, Kind: space.DiffKind(rd.Kind.String())}
			for _, fd := range rd.Fields {
				pr.Fields = append(pr.Fields, space.FieldDiff{Path: fd.Path, Before: fd.Before, After: fd.After})
			}
			pd.Records = append(pd.Records, pr)
		}
		out.Datasets = append(out.Datasets, pd)
	}
	return out, nil
}

// diffConcurrent handles the neither-is-ancestor case (proposal §5):
// two full views, two-way structural diff. Rare — a caller comparing
// two concurrent branches explicitly. versionTree is the caller's
// already-built (and not yet walked) history tree at version, so only
// the base cut pays a locked storage scan.
func (h *historyAPI) diffConcurrent(ctx context.Context, tree objecttree.ObjectTree, params history.ViewParams, baseHeads []string, versionTree objecttree.HistoryTree, version space.Version, filter history.DiffFilter) (history.DiffResult, error) {
	baseTree, err := buildTreeLocked(tree, baseHeads)
	if err != nil {
		return history.DiffResult{}, err
	}
	bp := params
	bp.Heads = baseHeads
	bp.Tree = baseTree
	baseView, err := history.BuildView(ctx, bp)
	if err != nil {
		return history.DiffResult{}, err
	}
	defer baseView.Close()

	vp := params
	vp.Heads = []string{version}
	vp.Tree = versionTree
	versionView, err := history.BuildView(ctx, vp)
	if err != nil {
		return history.DiffResult{}, err
	}
	defer versionView.Close()

	return history.DiffViews(ctx, baseView, versionView, filter)
}

// historicalView adapts history.View to the public interface.
type historicalView struct {
	view *history.View
}

func (v *historicalView) Version() space.Version { return v.view.Version }
func (v *historicalView) Datasets() []string     { return v.view.Datasets() }
func (v *historicalView) Close() error           { return v.view.Close() }

func (v *historicalView) Record(ctx context.Context, dataset, recordId string) (*anyenc.Value, error) {
	return v.view.Record(ctx, dataset, recordId), nil
}

func (v *historicalView) Records(ctx context.Context, dataset string) ([]*anyenc.Value, error) {
	return v.view.Records(ctx, dataset), nil
}

func publicMetas(metas []history.ChangeMeta) []space.ChangeMeta {
	out := make([]space.ChangeMeta, 0, len(metas))
	for _, m := range metas {
		pm := space.ChangeMeta{
			Version:   m.Version,
			Author:    m.Author,
			Timestamp: m.Timestamp,
			Dataset:   m.Dataset,
			TraceIds:  m.TraceIds,
			Truncated: m.Truncated,
			GroupSize: m.GroupSize,
		}
		for _, tr := range m.Touched {
			pm.Touched = append(pm.Touched, space.TouchedRecord{
				Dataset:  tr.Dataset,
				RecordId: tr.RecordId,
				Ops:      tr.Ops,
			})
		}
		out = append(out, pm)
	}
	return out
}

// mapHistoryErr converts internal history errors to their public
// space-package counterparts, preserving detail via wrapping.
func mapHistoryErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, history.ErrViewTooLarge):
		return fmt.Errorf("%w: %v", space.ErrViewTooLarge, err)
	case errors.Is(err, history.ErrHistoryTruncated):
		return fmt.Errorf("%w: %v", space.ErrHistoryTruncated, err)
	case errors.Is(err, history.ErrVersionNotFound):
		return fmt.Errorf("%w: %v", space.ErrVersionNotFound, err)
	default:
		return err
	}
}
