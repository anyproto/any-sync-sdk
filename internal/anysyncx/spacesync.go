package anysyncx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/streampool/streamhandler"
	"storj.io/drpc"
)

// SpaceSyncHandlerCName is registered as our app component name. Has
// to be unique across the app — any-sync's commonspace registers
// itself as a server-side handler for the same DRPC service, but the
// component-name registry needs distinct keys.
const spaceSyncHandlerCName = "client.spacesynchandler"

// spaceSyncHandler dispatches incoming sync DRPC requests from sync
// nodes to the right commonspace.Space. Spaces are registered/
// unregistered as the cache loads/closes them.
//
// Holds a reference to the App's HeadCache so HeadSync can answer
// the common full-range fast path without touching the (possibly
// evicted) commonspace.
type spaceSyncHandler struct {
	spacesyncproto.DRPCSpaceSyncUnimplementedServer

	mu         sync.RWMutex
	spaces     map[string]commonspace.Space
	streamPool streampool.StreamPool
	headCache  *HeadCache
}

func newSpaceSyncHandler() *spaceSyncHandler {
	return &spaceSyncHandler{spaces: make(map[string]commonspace.Space)}
}

func (h *spaceSyncHandler) Init(a *app.App) error {
	h.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	srv := a.MustComponent(server.CName).(server.DRPCServer)
	return spacesyncproto.DRPCRegisterSpaceSync(srv, h)
}

func (h *spaceSyncHandler) Name() string { return spaceSyncHandlerCName }

// RegisterSpace exposes a space to inbound sync RPCs. Called when a
// space is loaded into ocache.
func (h *spaceSyncHandler) RegisterSpace(spaceId string, sp commonspace.Space) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.spaces[spaceId] = sp
}

// UnregisterSpace removes a space — called when its ocache entry closes.
func (h *spaceSyncHandler) UnregisterSpace(spaceId string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.spaces, spaceId)
}

// RegisteredSpaceIds returns a snapshot for the stream-handler subscription
// preamble.
func (h *spaceSyncHandler) RegisteredSpaceIds() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]string, 0, len(h.spaces))
	for id := range h.spaces {
		ids = append(ids, id)
	}
	return ids
}

func (h *spaceSyncHandler) getSpace(spaceId string) (commonspace.Space, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	sp, ok := h.spaces[spaceId]
	if !ok {
		return nil, fmt.Errorf("anysyncx: space %s not registered", spaceId)
	}
	return sp, nil
}

func (h *spaceSyncHandler) ObjectSyncRequestStream(msg *spacesyncproto.ObjectSyncMessage, stream spacesyncproto.DRPCSpaceSync_ObjectSyncRequestStreamStream) error {
	sp, err := h.getSpace(msg.SpaceId)
	if err != nil {
		return err
	}
	return sp.HandleStreamSyncRequest(stream.Context(), msg, stream)
}

func (h *spaceSyncHandler) HeadSync(ctx context.Context, req *spacesyncproto.HeadSyncRequest) (*spacesyncproto.HeadSyncResponse, error) {
	if resp := h.tryHeadCache(req); resp != nil {
		return resp, nil
	}
	sp, err := h.getSpace(req.SpaceId)
	if err != nil {
		return nil, err
	}
	return sp.HandleRangeRequest(ctx, req)
}

// tryHeadCache answers the common full-range V3 probe ("what's your
// hash for this whole space?") from the HeadCache without loading
// the commonspace. Mirrors any-sync-node's tryNodeHeadSync, minus the
// V2 fallback — older diff types fall through to the deep path.
// Returns nil to signal "fall through to deep diff".
//
// Match shape: DiffType_V3, exactly one Range, Elements=false,
// From=0, To=MaxUint64. Anything else needs the real Merkle walk.
func (h *spaceSyncHandler) tryHeadCache(req *spacesyncproto.HeadSyncRequest) *spacesyncproto.HeadSyncResponse {
	if h.headCache == nil {
		return nil
	}
	if req.DiffType != spacesyncproto.DiffType_V3 {
		return nil
	}
	if len(req.Ranges) != 1 {
		return nil
	}
	r := req.Ranges[0]
	if r.Elements || r.From != 0 || r.To != math.MaxUint64 {
		return nil
	}
	hash, ok := h.headCache.Get(req.SpaceId)
	if !ok || hash == "" {
		return nil
	}
	hashB, err := hex.DecodeString(hash)
	if err != nil {
		return nil
	}
	return &spacesyncproto.HeadSyncResponse{
		DiffType: spacesyncproto.DiffType_V3,
		// Count=1 makes the requesting peer skip the per-element compare
		// step and treat this as an opaque hash batch — the same trick
		// any-sync-node uses to avoid a deep diff round-trip when the
		// hashes happen to match.
		Results: []*spacesyncproto.HeadSyncResult{{
			Hash:  hashB,
			Count: 1,
		}},
	}
}

func (h *spaceSyncHandler) ObjectSyncStream(stream spacesyncproto.DRPCSpaceSync_ObjectSyncStreamStream) error {
	return h.streamPool.ReadStream(stream, 100)
}

// nodeStreamTag tags every outbound stream we open to a sync node.
// Client streams are otherwise untagged (spaceId tags only land if a
// node echoes a SpaceSubscription, which it doesn't), so this constant
// is what lets spacePeerManager ask "is my push channel to a node still
// up?" via streamPool.Streams. It never collides with a spaceId tag —
// spaceIds are base58 object ids.
const nodeStreamTag = "anysyncx/node-stream"

// streamHandler is the StreamPool's outgoing-side handler. It opens
// ObjectSyncStream connections, primes them with the current set of
// subscribed spaces, and decodes inbound HeadUpdate / SpaceSubscription
// messages.
type streamHandler struct {
	syncHandler *spaceSyncHandler
	streamPool  streampool.StreamPool
}

func newStreamHandler(sh *spaceSyncHandler) *streamHandler {
	return &streamHandler{syncHandler: sh}
}

func (h *streamHandler) Init(a *app.App) error {
	h.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	return nil
}

func (h *streamHandler) Name() string { return streamhandler.CName }

func (h *streamHandler) OpenStream(ctx context.Context, p peer.Peer) (drpc.Stream, []string, int, error) {
	conn, err := p.AcquireDrpcConn(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	objectStream, err := spacesyncproto.NewDRPCSpaceSyncClient(conn).ObjectSyncStream(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	if ids := h.syncHandler.RegisteredSpaceIds(); len(ids) > 0 {
		sub := &spacesyncproto.SpaceSubscription{
			SpaceIds: ids,
			Action:   spacesyncproto.SpaceSubscriptionAction_Subscribe,
		}
		payload, mErr := sub.MarshalVT()
		if mErr != nil {
			return nil, nil, 0, mErr
		}
		if sErr := objectStream.Send(&spacesyncproto.ObjectSyncMessage{Payload: payload}); sErr != nil {
			return nil, nil, 0, sErr
		}
	}
	// Tag with nodeStreamTag so spacePeerManager can detect when this
	// channel drops and re-subscribe promptly (see hasNodeStream).
	return objectStream, []string{nodeStreamTag}, 100, nil
}

func (h *streamHandler) HandleMessage(ctx context.Context, _ string, msg drpc.Message) error {
	headUpdate, ok := msg.(*objectmessages.HeadUpdate)
	if !ok {
		return errors.New("anysyncx: unexpected stream message type")
	}
	// Empty SpaceId — subscription control message.
	if headUpdate.SpaceId() == "" {
		var sub spacesyncproto.SpaceSubscription
		if err := sub.UnmarshalVT(headUpdate.Bytes); err != nil {
			return err
		}
		if sub.Action == spacesyncproto.SpaceSubscriptionAction_Subscribe {
			return h.streamPool.AddTagsCtx(ctx, sub.SpaceIds...)
		}
		return h.streamPool.RemoveTagsCtx(ctx, sub.SpaceIds...)
	}
	sp, err := h.syncHandler.getSpace(headUpdate.SpaceId())
	if err != nil {
		return err
	}
	return sp.HandleMessage(ctx, headUpdate)
}

func (h *streamHandler) NewReadMessage() drpc.Message { return &objectmessages.HeadUpdate{} }
