// Selective sync by tree type (SYN-18).
//
// A Store built with SelectiveTypes head-syncs its space in full — every
// tree id and its heads are known and the sync diff converges — but
// downloads, stores and materializes only trees whose root changeType is
// in the set. Every other tree is recorded as:
//
//   - a heads-only STUB entry in any-sync's headstorage (heads +
//     CommonSnapshot, zero change rows), which is what makes the
//     headsync diff converge — without it the refused id would reappear
//     in every sync round's missing list forever;
//   - a SKIP MARKER row in the per-space `<spaceId>_skiplist` collection,
//     which keeps boot catch-up from force-loading the stub and lets a
//     future type-set widening find what to re-fetch.
//
// The two writes are not atomic (different stores) and don't need to be:
// a marker without a stub leaves the diff divergent, so the next sync
// round re-probes and rewrites the stub; a stub without a marker makes
// catch-up call Get on the id, which re-probes and rewrites the marker.
//
// Skip decisions ride two paths, both anchored on the raw root change
// (the tree id is the cid of the root bytes, so the type can't be
// spoofed once the cid checks out):
//
//   - remote fetches go out as PROBE requests (root + current heads, no
//     change bodies); selectiveTreeValidator classifies the response
//     before anything is persisted and either skips, or triggers one
//     full re-fetch for a selected type;
//   - head updates for locally-missing trees carry the root too, and
//     the treesyncer's PullFilter (Store.ShouldPullTree) refreshes the
//     stub heads without any round trip.
//
// ACL, settings and key-value sync are reserved paths outside tree-type
// filtering and stay full; the tech space never sets SelectiveTypes.
package spaceobjects

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/util/cidutil"
	"go.uber.org/zap"
)

// skiplistDataset is the per-space skip-marker collection suffix; full
// name `<spaceId>_skiplist`. One row per skipped tree:
// {id: treeId, type: rootChangeType}.
const skiplistDataset = "skiplist"

const skiplistTypeKey = "type"

var (
	// ErrTreeTypeSkipped aborts a remote tree fetch whose root
	// changeType is not in the selective set. By the time it is
	// returned the stub + marker are recorded; nothing was persisted
	// to tree storage.
	ErrTreeTypeSkipped = errors.New("spaceobjects: tree type not selected for sync")

	// errProbeSelectedType aborts a probe fetch that found a SELECTED
	// type — openTree reacts with one full (non-probe) re-fetch. Never
	// escapes openTree.
	errProbeSelectedType = errors.New("spaceobjects: probe hit a selected tree type")
)

// SelectiveMode reports whether this store filters trees by root
// changeType (cfg.Sync.TreeTypes non-empty and this is not the tech
// space).
func (s *Store) SelectiveMode() bool { return len(s.selective) > 0 }

func (s *Store) typeSelected(changeType string) bool {
	_, ok := s.selective[changeType]
	return ok
}

func (s *Store) skiplistColl(ctx context.Context) (anystore.Collection, error) {
	return s.db.Collection(ctx, s.spaceId+"_"+skiplistDataset)
}

// IsTreeSkipped reports whether treeId carries a skip marker. Always
// false outside selective mode.
func (s *Store) IsTreeSkipped(ctx context.Context, treeId string) (bool, error) {
	if !s.SelectiveMode() {
		return false, nil
	}
	coll, err := s.skiplistColl(ctx)
	if err != nil {
		return false, err
	}
	if _, err := coll.FindId(ctx, treeId); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// parseVerifiedRoot unmarshals a raw root change after checking that
// its bytes hash to its id. The id IS the tree id, so a verified root
// pins the tree's changeType — a peer can't relabel a tree to sneak it
// past (or into) the filter.
func parseVerifiedRoot(raw *treechangeproto.RawTreeChangeWithId) (*treechangeproto.RootChange, error) {
	if raw == nil || len(raw.GetRawChange()) == 0 {
		return nil, errors.New("spaceobjects: empty root change")
	}
	if !cidutil.VerifyCid(raw.RawChange, raw.Id) {
		return nil, errors.New("spaceobjects: root change cid mismatch")
	}
	rawChange := &treechangeproto.RawTreeChange{}
	if err := rawChange.UnmarshalVT(raw.RawChange); err != nil {
		return nil, fmt.Errorf("spaceobjects: unmarshal raw root: %w", err)
	}
	root := &treechangeproto.RootChange{}
	if err := root.UnmarshalVT(rawChange.Payload); err != nil {
		return nil, fmt.Errorf("spaceobjects: unmarshal root payload: %w", err)
	}
	return root, nil
}

// ShouldPullTree is the PullFilter decision for a head update on a
// locally-missing tree. True = fetch as usual. False = the tree's type
// is not selected; the stub heads were refreshed from the update and
// the fetch is swallowed.
//
// Anything that can't be classified safely (no/invalid root) returns
// true — the fetch path's validator is the authoritative gate.
func (s *Store) ShouldPullTree(ctx context.Context, treeId string, raw *treechangeproto.RawTreeChangeWithId, heads []string) bool {
	if !s.SelectiveMode() {
		return true
	}
	root, err := parseVerifiedRoot(raw)
	if err != nil {
		return true
	}
	if s.typeSelected(root.ChangeType) {
		return true
	}
	if err := s.recordSkippedTree(ctx, treeId, root, heads); err != nil {
		// Still skip: without the stub the diff stays divergent and the
		// next sync round re-probes this id.
		storeLog.Warn("selective: record skipped tree failed",
			zap.String("spaceId", s.spaceId), zap.String("treeId", treeId), zap.Error(err))
	}
	return false
}

// recordSkippedTree writes the skip marker and the heads-only stub for
// a tree we refuse to pull. Idempotent; refreshes the stub heads on
// every call. Never resurrects a deleted tree.
func (s *Store) recordSkippedTree(ctx context.Context, treeId string, root *treechangeproto.RootChange, heads []string) error {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return fmt.Errorf("spaceobjects: selective get space: %w", err)
	}
	st := handle.Inner().Storage()
	if st == nil {
		return errors.New("spaceobjects: selective: space storage unavailable")
	}
	hs := st.HeadStorage()
	if entry, gerr := hs.GetEntry(ctx, treeId); gerr == nil && entry.DeletedStatus != headstorage.DeletedStatusNotDeleted {
		return nil
	}
	if len(heads) == 0 {
		heads = []string{treeId}
	}
	coll, err := s.skiplistColl(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: open skiplist: %w", err)
	}
	changeType := root.ChangeType
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set(skiplistTypeKey, a.NewString(changeType))
		return v, true, nil
	})
	if _, err := coll.UpsertId(ctx, treeId, mod); err != nil {
		return fmt.Errorf("spaceobjects: upsert skip marker: %w", err)
	}
	update := headstorage.HeadsUpdate{
		Id:             treeId,
		Heads:          heads,
		CommonSnapshot: &treeId,
		IsDerived:      &root.IsDerived,
	}
	if root.ParentId != "" {
		update.ParentId = &root.ParentId
	}
	if err := hs.UpdateEntry(ctx, update); err != nil {
		return fmt.Errorf("spaceobjects: write stub heads entry: %w", err)
	}
	return nil
}

// unmarkSkipped drops the skip marker (tree deleted, or re-fetched
// after a type-set change). Best-effort: a missing row is fine.
func (s *Store) unmarkSkipped(ctx context.Context, treeId string) error {
	if !s.SelectiveMode() {
		return nil
	}
	coll, err := s.skiplistColl(ctx)
	if err != nil {
		return err
	}
	if err := coll.DeleteId(ctx, treeId); err != nil && !errors.Is(err, anystore.ErrDocNotFound) {
		return err
	}
	return nil
}

// selectiveTreeValidator classifies a remote tree fetch by its root
// changeType before anything is persisted (validators run on the first
// streamed response; storage creation is deferred).
//
//   - type not selected → record stub + marker, abort with
//     ErrTreeTypeSkipped (at most one response batch was downloaded);
//   - selected, probe response (no change bodies) → abort with
//     errProbeSelectedType so openTree re-fetches in full. A non-probe
//     stream always carries at least the root in its first batch, so
//     an empty Changes list is unambiguous;
//   - selected, changes present (old responder ignored the probe flag,
//     or the full re-fetch) → delegate to any-sync's default validation.
func (s *Store) selectiveTreeValidator(probe bool) objecttree.ValidatorFunc {
	return func(payload treestorage.TreeStorageCreatePayload, creator objecttree.TreeStorageCreator, aclList list.AclList) (objecttree.ObjectTree, error) {
		root, err := parseVerifiedRoot(payload.RootRawChange)
		if err != nil {
			return nil, err
		}
		if !s.typeSelected(root.ChangeType) {
			// Deliberately not the request ctx: the stub write must not
			// be lost to a cancel between classification and abort.
			if rerr := s.recordSkippedTree(context.Background(), payload.RootRawChange.Id, root, payload.Heads); rerr != nil {
				return nil, rerr
			}
			return nil, ErrTreeTypeSkipped
		}
		if probe && len(payload.Changes) == 0 {
			return nil, errProbeSelectedType
		}
		return objecttree.ValidateRawTreeDefault(payload, creator, aclList)
	}
}
