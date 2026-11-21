package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/credentialprovider"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"

	"github.com/anyproto/any-sync-sdk/space"
)

const (
	// signRetryMin and signRetryMax bound the wait before the
	// coordinator is asked again to sign a child space it refused: the
	// wait starts at the first and doubles up to the second.
	signRetryMin = time.Minute
	signRetryMax = time.Hour
)

// credentialProvider obtains space receipts from the coordinator via
// SpaceSign. Required by any-sync when joining or pushing spaces.
// Local-only spaces are refused — a receipt exists only to push, and a
// local-only space must never be pushed (defense in depth; their inert
// peer manager already keeps the push path unreachable).
//
// Head sync asks for a receipt on every round and for every node that
// lacks the space. A child space the coordinator refused to sign would
// repeat that question for as long as it stays refused, so its pushes
// fail with the stored answer until the wait is over.
//
// The coordinator signs a child only under a parent the network holds.
// A parent the account owns reaches the network when one of its devices
// pushes it, so a child of such a parent is not asked for before a tree
// node answered a head-sync round of the parent.
//
// A child whose row reads deleted gets no receipt: signed, it would be
// pushed and listed under its parent again. Another device of the
// account can delete a child this device still holds.
type credentialProvider struct {
	coordClient coordinatorclient.CoordinatorClient
	localOnly   *localOnlySpaces

	// onChildSign is told each answer of the coordinator to a request
	// to sign a child space: the check the child failed, RefusalNone
	// when it was signed.
	onChildSign atomic.Pointer[func(ctx context.Context, spaceId string, refusal space.Refusal)]
	// recorded reads the row the index holds for a child space: its
	// refusal, and whether it reads deleted; ok is false for a space
	// with no row. The row is shared by the account's devices and
	// outlives the process; the waits below are this process's own.
	recorded atomic.Pointer[func(spaceId string) (refusal space.Refusal, deleted, ok bool)]

	// ownStored reports a space this device stores and the account
	// owns: one the network holds only once a device of the account
	// pushed it.
	ownStored atomic.Pointer[func(spaceId string) bool]
	// kick starts a head-sync round of the given spaces.
	kick func(spaceIds []string)

	now func() time.Time

	mu      sync.Mutex
	refused map[string]*signRefusal
	// onNetwork holds the spaces a tree node answered a head-sync
	// round of in this run: the network holds them.
	onNetwork map[string]struct{}
	// awaiting maps a parent the network does not hold yet to the
	// children whose sign waits for it.
	awaiting map[string]map[string]struct{}
}

// signRefusal is the last refusal of one child space.
type signRefusal struct {
	err  error
	wait time.Duration
	// next is when the coordinator is asked again; zero means only on
	// request.
	next time.Time
}

func newCredentialProvider(localOnly *localOnlySpaces) *credentialProvider {
	return &credentialProvider{
		localOnly: localOnly,
		now:       time.Now,
		refused:   make(map[string]*signRefusal),
		onNetwork: make(map[string]struct{}),
		awaiting:  make(map[string]map[string]struct{}),
	}
}

func (c *credentialProvider) Init(a *app.App) error {
	c.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	return nil
}

func (c *credentialProvider) Name() string { return credentialprovider.CName }

func (c *credentialProvider) GetCredential(ctx context.Context, header *spacesyncproto.RawSpaceHeaderWithId) ([]byte, error) {
	return c.sign(ctx, header, false)
}

// sign asks the coordinator for the receipt of header. With now it
// asks whatever wait a refusal started.
func (c *credentialProvider) sign(ctx context.Context, header *spacesyncproto.RawSpaceHeaderWithId, now bool) ([]byte, error) {
	if c.localOnly.has(header.Id) {
		return nil, fmt.Errorf("anysyncx: space %s is local-only; refusing coordinator receipt", header.Id)
	}
	hdr := DecodeSpaceHeader(header.RawHeader)
	child := hdr != nil && hdr.ParentSpaceId != ""
	var (
		recorded space.Refusal
		known    bool
	)
	if child {
		var deleted bool
		if fn := c.recorded.Load(); fn != nil {
			recorded, deleted, known = (*fn)(header.Id)
		}
		if deleted {
			return nil, fmt.Errorf("anysyncx: space %s: %w", header.Id, space.ErrSpaceDeleted)
		}
	}
	if child && c.awaitsParent(header.Id, hdr.ParentSpaceId) {
		return nil, fmt.Errorf("anysyncx: space %s: the network does not hold its parent %s yet: %w",
			header.Id, hdr.ParentSpaceId, coordinatorproto.ErrParentSpaceNotAvailable)
	}
	if child && !now {
		if err := c.waiting(header.Id, recorded, known); err != nil {
			return nil, err
		}
	}
	receipt, err := c.coordClient.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{
		SpaceId:     header.Id,
		SpaceHeader: header.RawHeader,
	})
	if child {
		c.answered(ctx, header.Id, err)
	}
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}

// awaitsParent reports that the sign of the child spaceId waits for the
// network to hold its parent, and notes the child for networkHolds.
func (c *credentialProvider) awaitsParent(spaceId, parentSpaceId string) bool {
	if c.holds(parentSpaceId) {
		return false
	}
	own := c.ownStored.Load()
	if own == nil || !(*own)(parentSpaceId) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.onNetwork[parentSpaceId]; ok {
		return false
	}
	children := c.awaiting[parentSpaceId]
	if children == nil {
		children = make(map[string]struct{})
		c.awaiting[parentSpaceId] = children
	}
	children[spaceId] = struct{}{}
	return true
}

func (c *credentialProvider) holds(spaceId string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.onNetwork[spaceId]
	return ok
}

// networkHolds records that a tree node answered a head-sync round of
// spaceId, and starts a round of the children that waited for it.
func (c *credentialProvider) networkHolds(spaceId string) {
	c.mu.Lock()
	if _, ok := c.onNetwork[spaceId]; ok {
		c.mu.Unlock()
		return
	}
	c.onNetwork[spaceId] = struct{}{}
	children := make([]string, 0, len(c.awaiting[spaceId]))
	for child := range c.awaiting[spaceId] {
		children = append(children, child)
	}
	delete(c.awaiting, spaceId)
	c.mu.Unlock()
	if len(children) > 0 && c.kick != nil {
		c.kick(children)
	}
}

// waiting returns the refusal to fail a push of the child spaceId with
// instead of asking the coordinator, nil to ask. recorded is the
// refusal on its row, known false when it has none. A refusal the index
// no longer holds is over, and its wait with it. A child the index
// holds as refused for a parent that cannot be one is asked about only
// on request. Any other refusal is asked about again once its wait is
// over; a restart starts with no wait.
func (c *credentialProvider) waiting(spaceId string, recorded space.Refusal, known bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if known {
		switch recorded {
		case space.RefusalNone:
			delete(c.refused, spaceId)
			return nil
		case space.RefusalParentInvalid:
			return coordinatorproto.ErrParentSpaceInvalid
		}
	}
	r := c.refused[spaceId]
	if r == nil || (!r.next.IsZero() && !c.now().Before(r.next)) {
		return nil
	}
	return r.err
}

// answered records the coordinator's answer to a request to sign the
// child spaceId. An error that names no failed check is no answer: the
// coordinator was not reached, or failed.
func (c *credentialProvider) answered(ctx context.Context, spaceId string, err error) {
	refusal := refusalOf(err)
	if err != nil && refusal == space.RefusalNone {
		return
	}
	c.mu.Lock()
	if refusal == space.RefusalNone {
		delete(c.refused, spaceId)
	} else {
		r := c.refused[spaceId]
		if r == nil {
			r = &signRefusal{}
			c.refused[spaceId] = r
		}
		r.err = err
		r.wait = min(max(r.wait*2, signRetryMin), signRetryMax)
		r.next = c.now().Add(r.wait)
		if refusal == space.RefusalParentInvalid {
			// Headers do not change: the answer is final.
			r.next = time.Time{}
		}
	}
	c.mu.Unlock()
	if fn := c.onChildSign.Load(); fn != nil {
		(*fn)(ctx, spaceId, refusal)
	}
}

// refusalOf names the check behind the coordinator's refusal to sign a
// child space; RefusalNone for nil and for any other error.
func refusalOf(err error) space.Refusal {
	switch {
	case errors.Is(err, coordinatorproto.ErrParentSpaceNotAvailable):
		return space.RefusalParentNotAvailable
	case errors.Is(err, coordinatorproto.ErrParentSpaceInvalid):
		return space.RefusalParentInvalid
	case errors.Is(err, coordinatorproto.ErrParentPermissionDenied):
		return space.RefusalParentPermissionDenied
	case errors.Is(err, coordinatorproto.ErrChildSpacesLimitReached):
		return space.RefusalChildLimitReached
	}
	return space.RefusalNone
}

// OnChildSign registers fn for each answer of the coordinator to a
// request to sign a child space: the check the child failed,
// RefusalNone when it was signed. recorded reads the row the index
// holds for a space: its refusal and whether it reads deleted, ok false
// when it has no row. Both run on the push path.
func (a *App) OnChildSign(
	fn func(ctx context.Context, spaceId string, refusal space.Refusal),
	recorded func(spaceId string) (refusal space.Refusal, deleted, ok bool),
) {
	a.cred.onChildSign.Store(&fn)
	a.cred.recorded.Store(&recorded)
}

// SetOwnStoredFn registers what tells whether this device stores a
// space the account owns. The sign of a child of such a space waits for
// a tree node to hold the parent.
func (a *App) SetOwnStoredFn(fn func(spaceId string) bool) {
	a.cred.ownStored.Store(&fn)
}

// nodeAnswered is told every head-sync round of spaceId a peer
// answered. A round with a tree node of the space means the network
// holds the space and is in reach.
func (a *App) nodeAnswered(spaceId, peerId string) {
	if a.nodeConf == nil || !slices.Contains(a.nodeConf.NodeIds(spaceId), peerId) {
		return
	}
	a.treeNodeAnswered(spaceId)
}

func (a *App) treeNodeAnswered(spaceId string) {
	if a.cred != nil {
		a.cred.networkHolds(spaceId)
	}
	if fn := a.onNodeRound.Load(); fn != nil {
		(*fn)(spaceId)
	}
}

// OnNodeRound registers fn for every head-sync round of a space that
// one of its tree nodes answered: the network is in reach for it. fn
// runs on the sync path.
func (a *App) OnNodeRound(fn func(spaceId string)) {
	a.onNodeRound.Store(&fn)
}

// NodeAnsweredForTest stands for a head-sync round of spaceId that one
// of its tree nodes answered. Test seam.
func (a *App) NodeAnsweredForTest(spaceId string) { a.treeNodeAnswered(spaceId) }

// SignSpace asks the coordinator to sign spaceId now, whatever wait a
// refusal started, and returns its answer. A signed space is pushed by
// a sync round started in the background.
func (a *App) SignSpace(ctx context.Context, spaceId string) error {
	h, err := a.GetSpace(ctx, spaceId)
	if err != nil {
		return err
	}
	state, err := h.Inner().Storage().StateStorage().GetState(ctx)
	if err != nil {
		return err
	}
	header := &spacesyncproto.RawSpaceHeaderWithId{RawHeader: state.SpaceHeader, Id: state.SpaceId}
	if _, err := a.cred.sign(ctx, header, true); err != nil {
		return err
	}
	a.sync.SyncSpaces([]string{spaceId})
	return nil
}
