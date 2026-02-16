package components

import (
	"context"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/rpc/server"
)

const SpaceSyncHandlerCName = "client.spacesynchandler"

// SpaceSyncHandler implements DRPCSpaceSyncServer and dispatches incoming
// sync requests from nodes to the correct commonspace.Space.
// Nodes send ObjectSyncRequestStream and HeadSync back to the client when
// they need to fetch tree data the client has.
type SpaceSyncHandler struct {
	spacesyncproto.DRPCSpaceSyncUnimplementedServer

	mu     sync.RWMutex
	spaces map[string]commonspace.Space
}

func NewSpaceSyncHandler() *SpaceSyncHandler {
	return &SpaceSyncHandler{
		spaces: make(map[string]commonspace.Space),
	}
}

func (h *SpaceSyncHandler) Init(a *app.App) error {
	srv := a.MustComponent(server.CName).(server.DRPCServer)
	return spacesyncproto.DRPCRegisterSpaceSync(srv, h)
}

func (h *SpaceSyncHandler) Name() string {
	return SpaceSyncHandlerCName
}

// RegisterSpace registers a space so incoming sync requests can be dispatched to it.
func (h *SpaceSyncHandler) RegisterSpace(spaceId string, sp commonspace.Space) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.spaces[spaceId] = sp
}

// UnregisterSpace removes a space from the handler.
func (h *SpaceSyncHandler) UnregisterSpace(spaceId string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.spaces, spaceId)
}

func (h *SpaceSyncHandler) getSpace(spaceId string) (commonspace.Space, error) {
	h.mu.RLock()
	sp, ok := h.spaces[spaceId]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("space %s not found", spaceId)
	}
	return sp, nil
}

// ObjectSyncRequestStream handles incoming stream sync requests from nodes.
// This is the critical path: when a node receives a HeadUpdate it doesn't have,
// it sends an ObjectSyncRequestStream back to the client to fetch the tree data.
func (h *SpaceSyncHandler) ObjectSyncRequestStream(msg *spacesyncproto.ObjectSyncMessage, stream spacesyncproto.DRPCSpaceSync_ObjectSyncRequestStreamStream) error {
	sp, err := h.getSpace(msg.SpaceId)
	if err != nil {
		return err
	}
	return sp.HandleStreamSyncRequest(stream.Context(), msg, stream)
}

// HeadSync handles incoming diff requests from nodes.
func (h *SpaceSyncHandler) HeadSync(ctx context.Context, req *spacesyncproto.HeadSyncRequest) (*spacesyncproto.HeadSyncResponse, error) {
	sp, err := h.getSpace(req.SpaceId)
	if err != nil {
		return nil, err
	}
	return sp.HandleRangeRequest(ctx, req)
}
