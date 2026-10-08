package anysyncx

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/response"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/sync/syncdeps"
	"github.com/anyproto/any-sync/net/peer"
)

// Fetching a tree into storage, apart from materializing it.
//
// A headsync round names a missing tree by id alone, and what a tree
// is — a type, a collection, an object — is in its root's changeType.
// So the sync job fetches its missing trees first, several at a time,
// into the space's storage and nothing else: no listener, no replay,
// no tree kept open. The root read on the way in says which of the
// fetched trees are definitions, and the job materializes those before
// the rest (syncjob.go).

// treeFetcher fetches a missing tree into the space's storage and
// reports its root changeType. A tree already in storage — the diff
// can name one as missing: a tree this device holds as its root alone
// is not in the diff — is reported from the stored root without a
// request, with local set.
type treeFetcher interface {
	fetch(ctx context.Context, peerId, treeId string) (changeType string, local bool, err error)
}

// errFetchedNothing is a peer that answered a tree request without a
// tree.
var errFetchedNothing = errors.New("anysyncx: tree request answered without a tree")

// storageFetcher is the space's treeFetcher: any-sync's sync client
// sends the request and the default validator stores what comes back —
// the same request and the same validation a tree build runs, without
// the build.
type storageFetcher struct {
	syncClient synctree.SyncClient
	storage    spacestorage.SpaceStorage
	acl        list.AclList
}

func (f *storageFetcher) fetch(ctx context.Context, peerId, treeId string) (string, bool, error) {
	// Checked right before the request, as any-sync's own fetch does: a
	// tree can be deleted while it waits in the queue.
	if ct, stored, err := f.stored(ctx, treeId); stored || err != nil {
		return ct, stored, err
	}
	coll := &fetchCollector{storage: f.storage, acl: f.acl}
	req := f.syncClient.CreateNewTreeRequest(peerId, treeId)
	if err := f.syncClient.SendTreeRequest(peer.CtxWithPeerId(ctx, peerId), req, coll); err != nil {
		// A pull the peer's head update started may have stored the tree
		// meanwhile; then this request failed for nothing.
		if ct, stored, serr := f.stored(ctx, treeId); stored && serr == nil {
			return ct, true, nil
		}
		return "", false, err
	}
	if coll.tree == nil {
		return "", false, errFetchedNothing
	}
	return coll.changeType, false, nil
}

// stored reports the root changeType of a tree in the space's storage,
// or spacestorage.ErrTreeStorageAlreadyDeleted for a tree deleted here
// (its changes are gone and its head entry says so; fetching it would
// bring a deleted object back). The storage handle is not closed: it
// shares the space's changes collection, as every open tree does.
func (f *storageFetcher) stored(ctx context.Context, treeId string) (changeType string, stored bool, err error) {
	entry, err := f.storage.HeadStorage().GetEntry(ctx, treeId)
	if errors.Is(err, anystore.ErrDocNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if entry.DeletedStatus != headstorage.DeletedStatusNotDeleted {
		return "", false, spacestorage.ErrTreeStorageAlreadyDeleted
	}
	st, err := f.storage.TreeStorage(ctx, treeId)
	if err != nil {
		return "", false, err
	}
	root, err := st.Root(ctx)
	if err != nil {
		return "", false, err
	}
	changeType, err = rootChangeType(&treechangeproto.RawTreeChangeWithId{Id: root.Id, RawChange: root.RawChange})
	return changeType, true, err
}

// fetchCollector stores a tree request's answer: the first response
// creates the tree's storage through the default validator, the rest
// add to it.
type fetchCollector struct {
	storage objecttree.TreeStorageCreator
	acl     list.AclList

	tree       objecttree.ObjectTree
	changeType string
}

func (c *fetchCollector) NewResponse() syncdeps.Response { return &response.Response{} }

func (c *fetchCollector) CollectResponse(ctx context.Context, _, _ string, resp syncdeps.Response) error {
	r, ok := resp.(*response.Response)
	if !ok {
		return synctree.ErrUnexpectedResponseType
	}
	if c.tree != nil {
		c.tree.Lock()
		defer c.tree.Unlock()
		_, err := c.tree.AddRawChanges(ctx, objecttree.RawChangesPayload{NewHeads: r.Heads, RawChanges: r.Changes})
		return err
	}
	changeType, err := rootChangeType(r.Root)
	if err != nil {
		return err
	}
	tree, err := objecttree.ValidateRawTreeDefault(treestorage.TreeStorageCreatePayload{
		RootRawChange: r.Root,
		Changes:       r.Changes,
		Heads:         r.Heads,
	}, c.storage, c.acl)
	if err != nil {
		return err
	}
	c.tree, c.changeType = tree, changeType
	return nil
}

// rootChangeType reads the changeType off a raw root change. The cid is
// not checked: a mislabelled root changes the order the fetched trees
// are materialized in, and the fetch itself validates the tree.
func rootChangeType(raw *treechangeproto.RawTreeChangeWithId) (string, error) {
	if raw == nil || len(raw.GetRawChange()) == 0 {
		return "", errors.New("anysyncx: tree answer carries no root")
	}
	rawChange := &treechangeproto.RawTreeChange{}
	if err := rawChange.UnmarshalVT(raw.RawChange); err != nil {
		return "", fmt.Errorf("anysyncx: unmarshal raw root: %w", err)
	}
	root := &treechangeproto.RootChange{}
	if err := root.UnmarshalVT(rawChange.Payload); err != nil {
		return "", fmt.Errorf("anysyncx: unmarshal root: %w", err)
	}
	return root.ChangeType, nil
}
