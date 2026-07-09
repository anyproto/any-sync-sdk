package history

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// ErrNotAncestor — the base cut is not inside version's causal past
// (concurrent versions). Callers fall back to a two-view DiffViews.
var ErrNotAncestor = errors.New("history: base is not an ancestor of version")

// DiffRange computes the diff between the cuts base..version with ONE
// replay (proposal §5): materialize causal(base), snapshot exactly the
// records the delta (causal(version) \ causal(base)) touches, apply
// the delta, and diff the snapshots. Sound because CRDT gating is
// record-local — a change that doesn't touch record R cannot affect
// R's bytes, so records untouched by the delta are identical between
// the cuts and never need loading.
//
// Empty baseHeads (or heads equal to the tree root) mean the empty
// projection: the diff of version against nothing — used for the
// per-change effect diff of a first content change.
func DiffRange(ctx context.Context, p ViewParams, baseHeads []string, version Version, f DiffFilter) (DiffResult, error) {
	if version == "" {
		return DiffResult{}, errors.New("history: DiffRange: version required")
	}
	if len(f.RecordIds) > 0 && f.Dataset == "" {
		return DiffResult{}, errors.New("history: DiffFilter.RecordIds requires Dataset")
	}

	tree := p.Tree
	if tree == nil {
		if p.Storage == nil || p.Acl == nil {
			return DiffResult{}, errors.New("history: DiffRange: Tree or Storage+Acl required")
		}
		var err error
		tree, err = objecttree.BuildNonVerifiableHistoryTree(objecttree.HistoryTreeParams{
			Storage:         p.Storage,
			AclList:         p.Acl,
			Heads:           []string{version},
			IncludeBeforeId: true,
		})
		if err != nil {
			return DiffResult{}, mapTreeErr(err)
		}
	}

	ancestors, err := ancestorsWithin(tree, baseHeads)
	if err != nil {
		return DiffResult{}, err
	}

	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	if err != nil {
		return DiffResult{}, fmt.Errorf("history: open scratch store: %w", err)
	}
	defer db.Close()
	ctrl, _, err := scratchController(ctx, db, p)
	if err != nil {
		return DiffResult{}, err
	}

	known := make(map[string]struct{}, len(p.Regs))
	for _, reg := range p.Regs {
		known[reg.Name] = struct{}{}
	}
	maxRecords := p.MaxRecords
	if maxRecords <= 0 {
		maxRecords = DefaultMaxViewRecords
	}

	wantRecord := func(id string) bool {
		if len(f.RecordIds) == 0 {
			return true
		}
		for _, want := range f.RecordIds {
			if want == id {
				return true
			}
		}
		return false
	}
	inScope := func(decoded *crdt.Change) bool {
		if p.Dataset != "" && decoded.Dataset != p.Dataset {
			return false
		}
		if f.Dataset != "" && decoded.Dataset != f.Dataset {
			return false
		}
		_, ok := known[decoded.Dataset]
		return ok
	}

	codec := object.NewCodec()
	objectAuthor, objectCreatedAt := rootMeta(tree)

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return DiffResult{}, fmt.Errorf("history: scratch tx: %w", err)
	}
	txCtx := tx.Context()

	type recKey struct{ dataset, record string }
	var (
		touched  = map[recKey]struct{}{}
		records  int
		fatalErr error
	)

	// Pass 1: materialize causal(base); collect which records the
	// delta touches (its own decode is all that's needed — payloads
	// are not retained).
	pass1 := func(ch *objecttree.Change) bool {
		decoded, ok := ch.Model.(*crdt.Change)
		if !ok || decoded == nil || !inScope(decoded) {
			return true
		}
		stampEnvelope(decoded, ch, p.ObjectId, objectAuthor, objectCreatedAt)
		if _, isBase := ancestors[decoded.ChangeId]; isBase {
			records += len(decoded.Records)
			if records > maxRecords {
				fatalErr = ErrViewTooLarge
				return false
			}
			if applyErr := ctrl.ApplyChange(txCtx, *decoded); applyErr != nil {
				fatalErr = fmt.Errorf("history: apply %s: %w", decoded.ChangeId, applyErr)
				return false
			}
			return true
		}
		recordIds, idErr := crdt.ResolveRecordIds(*decoded)
		if idErr != nil {
			return true // malformed change: not applyable either, skip
		}
		for _, rec := range recordIds {
			if wantRecord(rec) {
				touched[recKey{decoded.Dataset, rec}] = struct{}{}
			}
		}
		return true
	}
	if err := tree.IterateRoot(decodeConvert(codec, tree.Id()), pass1); err != nil {
		_ = tx.Rollback()
		return DiffResult{}, mapTreeErr(err)
	}
	if fatalErr != nil {
		_ = tx.Rollback()
		return DiffResult{}, fatalErr
	}

	// Snapshot the touched records at base (Get clones, so the values
	// survive further applies).
	before := make(map[recKey]*anyenc.Value, len(touched))
	for key := range touched {
		before[key] = ctrl.Get(txCtx, key.dataset, key.record)
	}

	// Pass 2: apply the delta on top of the materialized base.
	pass2 := func(ch *objecttree.Change) bool {
		decoded, ok := ch.Model.(*crdt.Change)
		if !ok || decoded == nil || !inScope(decoded) {
			return true
		}
		stampEnvelope(decoded, ch, p.ObjectId, objectAuthor, objectCreatedAt)
		if _, isBase := ancestors[decoded.ChangeId]; isBase {
			return true
		}
		records += len(decoded.Records)
		if records > maxRecords {
			fatalErr = ErrViewTooLarge
			return false
		}
		if applyErr := ctrl.ApplyChange(txCtx, *decoded); applyErr != nil {
			fatalErr = fmt.Errorf("history: apply %s: %w", decoded.ChangeId, applyErr)
			return false
		}
		return true
	}
	if err := tree.IterateRoot(decodeConvert(codec, tree.Id()), pass2); err != nil {
		_ = tx.Rollback()
		return DiffResult{}, mapTreeErr(err)
	}
	if fatalErr != nil {
		_ = tx.Rollback()
		return DiffResult{}, fatalErr
	}
	if err := tx.Commit(); err != nil {
		return DiffResult{}, fmt.Errorf("history: commit scratch: %w", err)
	}

	// Diff before/after per touched record, grouped by dataset.
	byDataset := map[string][]RecordDiff{}
	keys := make([]recKey, 0, len(touched))
	for key := range touched {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dataset != keys[j].dataset {
			return keys[i].dataset < keys[j].dataset
		}
		return keys[i].record < keys[j].record
	})
	for _, key := range keys {
		after := ctrl.Get(ctx, key.dataset, key.record)
		if rd := DiffRecords(key.record, before[key], after); rd != nil {
			byDataset[key.dataset] = append(byDataset[key.dataset], *rd)
		}
	}

	res := DiffResult{Base: joinHeads(baseHeads), Version: version}
	names := make([]string, 0, len(byDataset))
	for ds := range byDataset {
		names = append(names, ds)
	}
	sort.Strings(names)
	for _, ds := range names {
		res.Datasets = append(res.Datasets, DatasetDiff{Dataset: ds, Records: byDataset[ds]})
	}
	return res, nil
}

// ancestorsWithin returns the ChangeId set of heads' causal past
// (inclusive), walked IN MEMORY over the already-built history tree —
// no storage access. Heads equal to the tree root contribute nothing;
// a head missing from the tree means base is not an ancestor of the
// tree's cut → ErrNotAncestor.
func ancestorsWithin(tree objecttree.HistoryTree, heads []string) (map[string]struct{}, error) {
	rootId := tree.Id()
	seen := make(map[string]struct{})
	stack := make([]string, 0, len(heads))
	for _, h := range heads {
		if h != "" && h != rootId {
			stack = append(stack, h)
		}
	}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[id]; ok {
			continue
		}
		ch, err := tree.GetChange(id)
		if err != nil || ch == nil {
			return nil, fmt.Errorf("%w: %s not in version's causal past", ErrNotAncestor, id)
		}
		seen[id] = struct{}{}
		for _, prev := range ch.PreviousIds {
			if prev != rootId {
				stack = append(stack, prev)
			}
		}
	}
	return seen, nil
}

func joinHeads(heads []string) string {
	return strings.Join(heads, ",")
}
