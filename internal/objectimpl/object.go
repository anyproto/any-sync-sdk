package objectimpl

import (
	"context"
	"sync"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
)

// objectImpl implements syncsdk.Object and updatelistener.UpdateListener.
type objectImpl struct {
	tree    objecttree.ObjectTree
	spaceID string
	signKey crypto.PrivKey

	mu          sync.Mutex
	subscribers []syncsdk.Handler
}

// NewObject creates a new objectImpl wrapping the given ObjectTree.
func NewObject(tree objecttree.ObjectTree, spaceID string, signKey crypto.PrivKey) *objectImpl {
	return &objectImpl{
		tree:    tree,
		spaceID: spaceID,
		signKey: signKey,
	}
}

// SetTree sets the underlying ObjectTree. This is called after BuildTree
// returns, since the objectImpl must be created first to serve as the listener.
func (o *objectImpl) SetTree(tree objecttree.ObjectTree) {
	o.tree = tree
}

func (o *objectImpl) ID() string {
	return o.tree.Id()
}

func (o *objectImpl) SpaceID() string {
	return o.spaceID
}

func (o *objectImpl) Heads() []string {
	o.tree.Lock()
	defer o.tree.Unlock()
	heads := make([]string, len(o.tree.Heads()))
	copy(heads, o.tree.Heads())
	return heads
}

func (o *objectImpl) AddContent(ctx context.Context, data []byte, opts ...syncsdk.AddOption) (syncsdk.ChangeInfo, error) {
	resolved := syncsdk.ResolveAddOptions(opts)

	o.tree.Lock()
	defer o.tree.Unlock()

	ts := time.Now().Unix()
	res, err := o.tree.AddContent(ctx, objecttree.SignableChangeContent{
		Data:       data,
		Key:        o.signKey,
		IsSnapshot: resolved.IsSnapshot,
		DataType:   resolved.DataType,
		Timestamp:  ts,
	})
	if err != nil {
		return syncsdk.ChangeInfo{}, err
	}

	if len(res.Added) == 0 {
		return syncsdk.ChangeInfo{}, nil
	}

	added := res.Added[0]
	return syncsdk.ChangeInfo{
		ID:          added.Id,
		PreviousIDs: added.PrevIds,
		Data:        data,
		DataType:    resolved.DataType,
		Identity:    o.signKey.GetPublic(),
		Timestamp:   ts,
		IsSnapshot:  resolved.IsSnapshot,
		Version:     added.OrderId,
		AddSeq:    added.AddSeq,
	}, nil
}

func (o *objectImpl) Iterate(visitor func(change syncsdk.ChangeInfo) bool) error {
	o.tree.Lock()
	defer o.tree.Unlock()

	return o.tree.IterateRoot(nil, func(change *objecttree.Change) bool {
		// Skip the root change (tree header)
		if change.Id == o.tree.Id() {
			return true
		}
		return visitor(syncsdk.ChangeInfo{
			ID:          change.Id,
			PreviousIDs: change.PreviousIds,
			Data:        change.Data,
			DataType:    change.DataType,
			Identity:    change.Identity,
			Timestamp:   change.Timestamp,
			IsSnapshot:  change.IsSnapshot,
			Version:     change.OrderId,
			AddSeq:    change.AddSeq,
		})
	})
}

func (o *objectImpl) IterateAfterAddSeq(applySeq uint64, visitor func(change syncsdk.ChangeInfo) bool) error {
	o.tree.Lock()
	defer o.tree.Unlock()
	return o.tree.Storage().GetAfterAddSeq(context.Background(), applySeq,
		func(ctx context.Context, sc objecttree.StorageChange) (bool, error) {
			if sc.Id == o.tree.Id() {
				return true, nil
			}
			ch, err := o.tree.GetChange(sc.Id)
			if err != nil {
				return true, nil
			}
			return visitor(syncsdk.ChangeInfo{
				ID:          ch.Id,
				PreviousIDs: ch.PreviousIds,
				Data:        ch.Data,
				DataType:    ch.DataType,
				Identity:    ch.Identity,
				Timestamp:   ch.Timestamp,
				IsSnapshot:  ch.IsSnapshot,
				Version:     ch.OrderId,
				AddSeq:    sc.AddSeq,
			}), nil
		})
}

func (o *objectImpl) Subscribe(handler syncsdk.Handler) (unsubscribe func()) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.subscribers = append(o.subscribers, handler)
	idx := len(o.subscribers) - 1
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if idx < len(o.subscribers) {
			o.subscribers[idx] = nil
		}
	}
}

func (o *objectImpl) Close() error {
	return o.tree.Close()
}

// Update implements updatelistener.UpdateListener.
// Called by synctree when remote changes arrive (append mode).
func (o *objectImpl) Update(tree objecttree.ObjectTree) error {
	evt := syncsdk.Event{
		Type:     syncsdk.ObjectUpdated,
		SpaceID:  o.spaceID,
		ObjectID: tree.Id(),
		Heads:    tree.Heads(),
	}
	o.dispatch(evt)
	return nil
}

// Rebuild implements updatelistener.UpdateListener.
// Called by synctree on full rebuild.
func (o *objectImpl) Rebuild(tree objecttree.ObjectTree) error {
	evt := syncsdk.Event{
		Type:     syncsdk.ObjectRebuilt,
		SpaceID:  o.spaceID,
		ObjectID: tree.Id(),
		Heads:    tree.Heads(),
	}
	o.dispatch(evt)
	return nil
}

func (o *objectImpl) dispatch(evt syncsdk.Event) {
	o.mu.Lock()
	subs := make([]syncsdk.Handler, len(o.subscribers))
	copy(subs, o.subscribers)
	o.mu.Unlock()

	for _, h := range subs {
		if h != nil {
			h(evt)
		}
	}
}
