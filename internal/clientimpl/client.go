package clientimpl

import (
	"context"
	"crypto/rand"
	"sync"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/internal/bootstrap"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/util/crypto"
)

type clientImpl struct {
	app            *app.App
	spaceService   commonspace.SpaceService
	coordClient    coordinatorclient.CoordinatorClient
	joiningClient  aclclient.AclJoiningClient
	cfg            syncsdk.Config
	peerId         string
	networkId      string

	mu       sync.Mutex
	spaces   map[string]*spaceimpl.SpaceImpl
	handlers []syncsdk.Handler
	closed   bool
}

// New creates and starts a new SDK client.
func New(ctx context.Context, cfg syncsdk.Config) (syncsdk.Client, error) {
	if cfg.SigningKey == nil {
		return nil, syncsdk.ErrInvalidConfig
	}
	if cfg.StoragePath == "" {
		return nil, syncsdk.ErrInvalidConfig
	}
	if cfg.PeerKey == nil {
		peerKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		cfg.PeerKey = peerKey
	}
	if cfg.MasterKey == nil {
		cfg.MasterKey = cfg.SigningKey
	}

	a, err := bootstrap.NewApp(ctx, cfg)
	if err != nil {
		return nil, err
	}

	spaceService := a.MustComponent(commonspace.CName).(commonspace.SpaceService)
	coordClient := a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)

	joiningClient := aclclient.NewAclJoiningClient()
	if err := joiningClient.Init(a); err != nil {
		_ = a.Close(ctx)
		return nil, err
	}

	return &clientImpl{
		app:           a,
		spaceService:  spaceService,
		coordClient:   coordClient,
		joiningClient: joiningClient,
		cfg:           cfg,
		peerId:        cfg.PeerKey.GetPublic().PeerId(),
		networkId:     cfg.Network.NetworkID,
		spaces:        make(map[string]*spaceimpl.SpaceImpl),
	}, nil
}

func (c *clientImpl) CreateSpace(ctx context.Context, opts ...syncsdk.SpaceCreateOption) (syncsdk.Space, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	readKey, err := crypto.NewRandomAES()
	if err != nil {
		return nil, err
	}
	metadataKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	payload := spacepayloads.SpaceCreatePayload{
		SigningKey:      c.cfg.SigningKey,
		MasterKey:       c.cfg.MasterKey,
		ReplicationKey:  0,
		ReadKey:         readKey,
		MetadataKey:     metadataKey,
		SpaceType:       "anytype.space",
	}

	spaceId, err := c.spaceService.CreateSpace(ctx, payload)
	if err != nil {
		return nil, err
	}

	return c.OpenSpace(ctx, spaceId)
}

func (c *clientImpl) DeriveSpace(ctx context.Context, spaceType string) (syncsdk.Space, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	payload := spacepayloads.SpaceDerivePayload{
		SigningKey: c.cfg.SigningKey,
		MasterKey: c.cfg.MasterKey,
		SpaceType: spaceType,
	}

	spaceId, err := c.spaceService.DeriveSpace(ctx, payload)
	if err != nil {
		return nil, err
	}

	return c.OpenSpace(ctx, spaceId)
}

func (c *clientImpl) OpenSpace(ctx context.Context, spaceID string) (syncsdk.Space, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, syncsdk.ErrClientClosed
	}

	if sp, ok := c.spaces[spaceID]; ok {
		return sp, nil
	}

	sp := spaceimpl.New(spaceID, c.spaceService, c.cfg, c.coordClient)
	c.spaces[spaceID] = sp
	return sp, nil
}

func (c *clientImpl) JoinSpace(ctx context.Context, invite string) (syncsdk.Space, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	spaceID, inviteKey, approvalRequired, err := syncsdk.DecodeInvite(invite)
	if err != nil {
		return nil, err
	}

	if approvalRequired {
		_, err = c.joiningClient.RequestJoin(ctx, spaceID, list.RequestJoinPayload{
			InviteKey: inviteKey,
		})
		if err != nil {
			return nil, err
		}
		return nil, syncsdk.ErrJoinRequestPending
	}

	_, err = c.joiningClient.InviteJoin(ctx, spaceID, list.InviteJoinPayload{
		InviteKey:   inviteKey,
		Permissions: list.AclPermissionsWriter,
	})
	if err != nil {
		return nil, err
	}
	return c.OpenSpace(ctx, spaceID)
}

func (c *clientImpl) Subscribe(handler syncsdk.Handler) (unsubscribe func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers = append(c.handlers, handler)
	idx := len(c.handlers) - 1
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if idx < len(c.handlers) {
			c.handlers[idx] = nil
		}
	}
}

func (c *clientImpl) DeleteSpace(ctx context.Context, spaceID string) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	confirmation, err := coordinatorproto.PrepareDeleteConfirmation(c.cfg.SigningKey, spaceID, c.peerId, c.networkId)
	if err != nil {
		return err
	}
	if err := c.coordClient.SpaceDelete(ctx, spaceID, confirmation); err != nil {
		return err
	}

	// Close and remove the space from local cache if it was open.
	c.mu.Lock()
	sp, ok := c.spaces[spaceID]
	if ok {
		delete(c.spaces, spaceID)
	}
	c.mu.Unlock()
	if ok {
		_ = sp.Close(ctx)
	}
	return nil
}

func (c *clientImpl) DeleteAccount(ctx context.Context) (int64, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	confirmation, err := coordinatorproto.PrepareAccountDeleteConfirmation(c.cfg.SigningKey, c.peerId, c.networkId)
	if err != nil {
		return 0, err
	}
	ts, err := c.coordClient.AccountDelete(ctx, confirmation)
	if err != nil {
		return 0, err
	}
	// Close all connections — the coordinator associates the peer connection
	// with the account, so any reuse after deletion returns "account is deleted".
	_ = c.Close(ctx)
	return ts, nil
}

func (c *clientImpl) RevertAccountDeletion(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	return c.coordClient.AccountRevertDeletion(ctx)
}

func (c *clientImpl) NetworkConfig(ctx context.Context) (syncsdk.NetworkConfig, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return syncsdk.NetworkConfig{}, syncsdk.ErrClientClosed
	}
	c.mu.Unlock()

	resp, err := c.coordClient.NetworkConfiguration(ctx, "")
	if err != nil {
		return syncsdk.NetworkConfig{}, err
	}

	nodes := make([]syncsdk.NodeInfo, len(resp.Nodes))
	for i, n := range resp.Nodes {
		types := make([]string, len(n.Types))
		for j, t := range n.Types {
			types[j] = nodeTypeToString(t)
		}
		nodes[i] = syncsdk.NodeInfo{
			PeerID:    n.PeerId,
			Addresses: n.Addresses,
			Types:     types,
		}
	}

	return syncsdk.NetworkConfig{
		NetworkID: resp.NetworkId,
		Nodes:     nodes,
	}, nil
}

func nodeTypeToString(t coordinatorproto.NodeType) string {
	switch t {
	case coordinatorproto.NodeType_TreeAPI:
		return "tree"
	case coordinatorproto.NodeType_FileAPI:
		return "file"
	case coordinatorproto.NodeType_CoordinatorAPI:
		return "coordinator"
	case coordinatorproto.NodeType_ConsensusAPI:
		return "consensus"
	case coordinatorproto.NodeType_NamingNodeAPI:
		return "naming"
	case coordinatorproto.NodeType_PaymentProcessingAPI:
		return "payment"
	default:
		return "unknown"
	}
}

func (c *clientImpl) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	spaces := make([]*spaceimpl.SpaceImpl, 0, len(c.spaces))
	for _, sp := range c.spaces {
		spaces = append(spaces, sp)
	}
	c.spaces = nil
	c.mu.Unlock()

	for _, sp := range spaces {
		_ = sp.Close(ctx)
	}
	return c.app.Close(ctx)
}
