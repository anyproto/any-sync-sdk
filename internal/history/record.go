package history

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Engine A fast path: record-filtered replay (proposal §4.2). Sound
// because CRDT gating is strictly record-local — changes that don't
// touch record M cannot influence M's bytes
// (TestFilteredReplayMatchesFullReplay / TestFilteredReplayFuzz,
// internal/crdt).

// RecordAtParams describes one record-scope historical read.
type RecordAtParams struct {
	ObjectId string
	Dataset  string
	RecordId string
	// Version is the single cut head (ChangeId); the record is
	// reconstructed from exactly its causal past, inclusive.
	Version Version

	Storage objecttree.Storage
	Acl     list.AclList
	Regs    []crdt.HandlerReg
	// SharedDatasets — same contract as ViewParams.SharedDatasets.
	SharedDatasets []string

	// Tree — same contract as ViewParams.Tree: a pre-built history
	// tree at Version (built under the live tree's lock when the
	// storage is shared with a live tree).
	Tree objecttree.HistoryTree

	// TouchedChangeIds optionally pre-filters iteration to the changes
	// known to touch (Dataset, RecordId) — fed from the history index
	// (§4.4). The causal-past intersection happens implicitly: the
	// history tree at Version only iterates ancestors. nil = fallback
	// mode, which decodes every ancestor change and applies the ones
	// that touch the record (correct without an index, just slower).
	TouchedChangeIds []string
}

// RecordAt reconstructs one record as of Version. Returns nil when the
// record does not exist at that cut; a tombstoned record comes back as
// its tombstone row (callers check crdt.DeletedAtField). If the
// dataset's registration sets DisableFilteredReplay, the read routes
// through a full-object view instead (slow path, same result).
func RecordAt(ctx context.Context, p RecordAtParams) (*anyenc.Value, error) {
	if p.Dataset == "" || p.RecordId == "" {
		return nil, errors.New("history: RecordAt: Dataset and RecordId required")
	}
	if p.Version == "" {
		return nil, errors.New("history: RecordAt: Version required")
	}
	if filteredReplayDisabled(p.Regs, p.Dataset) {
		view, err := BuildView(ctx, ViewParams{
			ObjectId:       p.ObjectId,
			Heads:          []string{p.Version},
			Dataset:        p.Dataset,
			Storage:        p.Storage,
			Acl:            p.Acl,
			Tree:           p.Tree,
			Regs:           p.Regs,
			SharedDatasets: p.SharedDatasets,
		})
		if err != nil {
			return nil, err
		}
		defer view.Close()
		return view.Record(ctx, p.Dataset, p.RecordId), nil
	}

	tree := p.Tree
	if tree == nil {
		if p.Storage == nil || p.Acl == nil {
			return nil, errors.New("history: RecordAt: Tree or Storage+Acl required")
		}
		var err error
		tree, err = objecttree.BuildNonVerifiableHistoryTree(objecttree.HistoryTreeParams{
			Storage:         p.Storage,
			AclList:         p.Acl,
			Heads:           []string{p.Version},
			IncludeBeforeId: true,
		})
		if err != nil {
			return nil, mapTreeErr(err)
		}
	}
	return recordFromTree(ctx, tree, p)
}

func recordFromTree(ctx context.Context, tree objecttree.HistoryTree, p RecordAtParams) (*anyenc.Value, error) {
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	if err != nil {
		return nil, fmt.Errorf("history: open scratch store: %w", err)
	}
	defer db.Close()

	shared := crdt.SharedCollections{}
	for _, name := range p.SharedDatasets {
		coll, cerr := db.Collection(ctx, name)
		if cerr != nil {
			return nil, fmt.Errorf("history: open scratch shared collection %s: %w", name, cerr)
		}
		shared[name] = coll
	}
	ctrl, err := crdt.NewControllerWithShared(ctx, p.ObjectId, db, shared, p.Regs...)
	if err != nil {
		return nil, fmt.Errorf("history: scratch controller: %w", err)
	}

	var touched map[string]struct{}
	if p.TouchedChangeIds != nil {
		touched = make(map[string]struct{}, len(p.TouchedChangeIds))
		for _, id := range p.TouchedChangeIds {
			touched[id] = struct{}{}
		}
	}

	codec := object.NewCodec()
	objectAuthor, objectCreatedAt := rootMeta(tree)

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("history: scratch tx: %w", err)
	}
	txCtx := tx.Context()

	var fatalErr error

	baseConvert := decodeConvert(codec, tree.Id())
	convert := func(ch *objecttree.Change, decrypted []byte) (any, error) {
		// Index-fed mode: skip the decode for changes known not to
		// touch the record — this is where the fast path wins.
		if touched != nil {
			if _, ok := touched[ch.Id]; !ok {
				return nil, nil
			}
		}
		m, cerr := baseConvert(ch, decrypted)
		if cerr != nil || m == nil {
			return nil, cerr
		}
		decoded := m.(*crdt.Change)
		if decoded.Dataset != p.Dataset {
			return nil, nil
		}
		// ChangeId must be stamped BEFORE touchesRecord: empty-id
		// records resolve their id from it (shortId convention).
		decoded.ChangeId = ch.Id
		// Fallback mode: apply only changes whose payload names the
		// record. (Index-fed changes already passed this test at index
		// time; re-checking is harmless and guards a stale index row.)
		if !touchesRecord(decoded, p.RecordId) {
			return nil, nil
		}
		return decoded, nil
	}

	iter := func(ch *objecttree.Change) bool {
		decoded, ok := ch.Model.(*crdt.Change)
		if !ok || decoded == nil {
			return true
		}
		stampEnvelope(decoded, ch, p.ObjectId, objectAuthor, objectCreatedAt)
		// The whole change applies (not just the target's RecordChange):
		// that is exactly the workload the soundness test verifies, and
		// sibling rows in the scratch are invisible to this API — it
		// returns one record, not a store (proposal §4.2 caveats).
		if applyErr := ctrl.ApplyChange(txCtx, *decoded); applyErr != nil {
			fatalErr = fmt.Errorf("history: apply %s: %w", ch.Id, applyErr)
			return false
		}
		return true
	}

	if err := tree.IterateRoot(convert, iter); err != nil {
		_ = tx.Rollback()
		return nil, mapTreeErr(err)
	}
	if fatalErr != nil {
		_ = tx.Rollback()
		return nil, fatalErr
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("history: commit scratch: %w", err)
	}

	// Get clones off any-store's buffer, so the value survives db.Close.
	return ctrl.Get(ctx, p.Dataset, p.RecordId), nil
}

// touchesRecord reports whether the decoded change carries ops for the
// record id. Empty-id records resolve to their content-derived id
// (shortId convention) before matching.
func touchesRecord(ch *crdt.Change, recordId string) bool {
	for i := range ch.Records {
		if ch.Records[i].Id == recordId {
			return true
		}
	}
	// Records with empty ids resolve from the ChangeId; cheap to check
	// only when present.
	for i := range ch.Records {
		if ch.Records[i].Id == "" {
			ids, err := crdt.ResolveRecordIds(*ch)
			if err != nil {
				return false
			}
			for _, id := range ids {
				if id == recordId {
					return true
				}
			}
			return false
		}
	}
	return false
}

func filteredReplayDisabled(regs []crdt.HandlerReg, dataset string) bool {
	for _, reg := range regs {
		if reg.Name == dataset {
			return reg.DisableFilteredReplay
		}
	}
	return false
}
