package components

import (
	"context"
	"fmt"

	"storj.io/drpc"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/streampool/streamhandler"
)

// ClientStreamHandler implements streamhandler.StreamHandler for the SDK client.
// It opens ObjectSyncStream connections to peers and dispatches incoming
// HeadUpdate messages to the appropriate space.
type ClientStreamHandler struct {
	spaceSyncHandler *SpaceSyncHandler
	streamPool       streampool.StreamPool
}

func NewStreamHandler(spaceSyncHandler *SpaceSyncHandler) *ClientStreamHandler {
	return &ClientStreamHandler{
		spaceSyncHandler: spaceSyncHandler,
	}
}

func (h *ClientStreamHandler) Init(a *app.App) error {
	h.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	return nil
}

func (h *ClientStreamHandler) Name() string {
	return streamhandler.CName
}

func (h *ClientStreamHandler) OpenStream(ctx context.Context, p peer.Peer) (stream drpc.Stream, tags []string, queueSize int, err error) {
	spaceIds := h.spaceSyncHandler.RegisteredSpaceIds()
	conn, err := p.AcquireDrpcConn(ctx)
	if err != nil {
		return
	}
	objectStream, err := spacesyncproto.NewDRPCSpaceSyncClient(conn).ObjectSyncStream(ctx)
	if err != nil {
		return
	}
	// Send subscription for all currently open spaces so the node tags
	// this stream and broadcasts HeadUpdates for those spaces to us.
	if len(spaceIds) > 0 {
		subMsg := &spacesyncproto.SpaceSubscription{
			SpaceIds: spaceIds,
			Action:   spacesyncproto.SpaceSubscriptionAction_Subscribe,
		}
		payload, merr := subMsg.MarshalVT()
		if merr != nil {
			err = merr
			return
		}
		if err = objectStream.Send(&spacesyncproto.ObjectSyncMessage{
			Payload: payload,
		}); err != nil {
			return
		}
	}
	return objectStream, nil, 100, nil
}

func (h *ClientStreamHandler) HandleMessage(ctx context.Context, peerId string, msg drpc.Message) error {
	headUpdate, ok := msg.(*objectmessages.HeadUpdate)
	if !ok {
		return fmt.Errorf("unexpected message type: %T", msg)
	}
	// Empty SpaceId means this is a subscription control message.
	if headUpdate.SpaceId() == "" {
		var subMsg = &spacesyncproto.SpaceSubscription{}
		if err := subMsg.UnmarshalVT(headUpdate.Bytes); err != nil {
			return err
		}
		if subMsg.Action == spacesyncproto.SpaceSubscriptionAction_Subscribe {
			return h.streamPool.AddTagsCtx(ctx, subMsg.SpaceIds...)
		}
		return h.streamPool.RemoveTagsCtx(ctx, subMsg.SpaceIds...)
	}
	sp, err := h.spaceSyncHandler.getSpace(headUpdate.SpaceId())
	if err != nil {
		return err
	}
	return sp.HandleMessage(ctx, headUpdate)
}

func (h *ClientStreamHandler) NewReadMessage() drpc.Message {
	return &objectmessages.HeadUpdate{}
}
