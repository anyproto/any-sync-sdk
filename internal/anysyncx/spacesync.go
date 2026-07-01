package anysyncx

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/streampool/streamhandler"
	"go.uber.org/zap"
	"storj.io/drpc"
)

var streamLog = logger.NewNamed("anysyncx.streamhandler")

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

// snapshotSpaces returns the currently-registered spaces. Used by the
// stream handler to run a recovery head-sync after a node stream
// (re)opens.
func (h *spaceSyncHandler) snapshotSpaces() []commonspace.Space {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]commonspace.Space, 0, len(h.spaces))
	for _, sp := range h.spaces {
		out = append(out, sp)
	}
	return out
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
	// resyncing coalesces concurrent recovery head-syncs: every node
	// stream (re)open triggers kickResync, but only one pass runs at a
	// time. See kickResync.
	resyncing atomic.Bool
}

// resyncDebounce lets the burst of node-stream reopens that follow a
// dropped connection settle before the recovery head-sync runs, so the
// three per-node OpenStream callbacks coalesce into a single pass.
const resyncDebounce = 300 * time.Millisecond

// resyncTimeout bounds each per-space recovery head-sync so one
// unreachable node can't wedge the recovery goroutine.
const resyncTimeout = 20 * time.Second

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
	ids := h.syncHandler.RegisteredSpaceIds()
	if len(ids) > 0 {
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
	// A push (HeadUpdate) is fire-and-forget: any-sync's streampool tears
	// the whole shared node stream down on the first read/write/handle
	// error (net/streampool/stream.go readLoop's deferred streamClose), so
	// a change pushed while this channel was down between the drop and this
	// reopen is lost — only the ~30s periodic headsync would otherwise
	// recover it. Kick a recovery head-sync now that the channel is back so
	// missed pushes converge promptly instead of on the next diff tick.
	h.kickResync()
	// Tag with nodeStreamTag so spacePeerManager can detect when this
	// channel drops and re-subscribe promptly (see hasNodeStream).
	return objectStream, []string{nodeStreamTag}, 100, nil
}

// kickResync schedules one recovery head-sync pass across all registered
// spaces. Concurrent (re)opens — e.g. the three per-node streams
// reopening after a connection reset — coalesce into a single pass via
// the resyncing guard, and a short debounce lets that burst settle
// first. Best-effort and fully detached from the caller: head-sync is
// the same diff the periodic timer runs, just triggered on reconnect.
func (h *streamHandler) kickResync() {
	if !h.resyncing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer h.resyncing.Store(false)
		time.Sleep(resyncDebounce)
		for _, sp := range h.syncHandler.snapshotSpaces() {
			ctx, cancel := context.WithTimeout(context.Background(), resyncTimeout)
			if err := sp.SyncHeads(ctx); err != nil {
				streamLog.Debug("resync head-sync", zap.String("spaceId", sp.Id()), zap.Error(err))
			}
			cancel()
		}
	}()
}

// HandleMessage processes one inbound stream message. It NEVER returns a
// non-nil error: any-sync's streampool readLoop tears the whole shared
// node stream down the moment a handler errors (net/streampool/stream.go:
// readLoop's deferred streamClose). That stream multiplexes the push
// subscriptions for every space, so killing it over one unprocessable
// message drops realtime delivery for all of them until a fresh stream
// reopens — and any change pushed in that window is lost, recoverable
// only by the ~30s periodic diff. Recoverable conditions (a HeadUpdate
// for a space we've offloaded or not yet registered, a malformed control
// frame) are therefore logged and swallowed; the change is reconciled by
// head-sync. The errors are logged at debug because they are expected
// during normal churn (offload, lazy load).
func (h *streamHandler) HandleMessage(ctx context.Context, _ string, msg drpc.Message) error {
	headUpdate, ok := msg.(*objectmessages.HeadUpdate)
	if !ok {
		streamLog.Debug("unexpected stream message type", zap.String("type", fmt.Sprintf("%T", msg)))
		return nil
	}
	// Empty SpaceId — subscription control message.
	if headUpdate.SpaceId() == "" {
		var sub spacesyncproto.SpaceSubscription
		if err := sub.UnmarshalVT(headUpdate.Bytes); err != nil {
			streamLog.Debug("decode subscription control", zap.Error(err))
			return nil
		}
		if sub.Action == spacesyncproto.SpaceSubscriptionAction_Subscribe {
			if err := h.streamPool.AddTagsCtx(ctx, sub.SpaceIds...); err != nil {
				streamLog.Debug("add stream tags", zap.Error(err))
			}
			return nil
		}
		if err := h.streamPool.RemoveTagsCtx(ctx, sub.SpaceIds...); err != nil {
			streamLog.Debug("remove stream tags", zap.Error(err))
		}
		return nil
	}
	sp, err := h.syncHandler.getSpace(headUpdate.SpaceId())
	if err != nil {
		// Space not registered (offloaded, or a stale push for a space this
		// device never loaded). Swallow — head-sync reconciles if we later
		// load it.
		streamLog.Debug("head update for unregistered space", zap.String("spaceId", headUpdate.SpaceId()))
		return nil
	}
	if err := sp.HandleMessage(ctx, headUpdate); err != nil {
		streamLog.Debug("apply head update", zap.String("spaceId", headUpdate.SpaceId()), zap.Error(err))
	}
	return nil
}

func (h *streamHandler) NewReadMessage() drpc.Message { return &objectmessages.HeadUpdate{} }
