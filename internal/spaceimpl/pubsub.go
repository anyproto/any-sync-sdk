package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/pubsub/pubsubproto"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/space"
)

// pubSubAPI is the public space.PubSubAPI over the app-level pubsub
// engine, bound to one spaceId. Constructed per call — no hidden
// state; the engine holds the subscriptions.
type pubSubAPI struct {
	app     *anysyncx.App
	spaceId string
}

// NewPubSubAPI returns the PubSubAPI for spaceId. Also used by the SDK
// root for the account-wide surface (bound to the tech space).
func NewPubSubAPI(app *anysyncx.App, spaceId string) space.PubSubAPI {
	return &pubSubAPI{app: app, spaceId: spaceId}
}

func (p *pubSubAPI) Publish(ctx context.Context, topic string, payload []byte) error {
	return mapPubSubErr(p.app.PubSubPublish(ctx, p.spaceId, topic, payload))
}

func (p *pubSubAPI) Subscribe(pattern string, cb func(space.PubSubMessage)) (cancel func(), err error) {
	if cb == nil {
		return func() {}, nil
	}
	self := p.app.AccountKeys().SignKey.GetPublic()
	cancel, err = p.app.PubSubSubscribe(p.spaceId, pattern, func(spaceId, topic string, identity crypto.PubKey, payload []byte) {
		cb(space.PubSubMessage{
			SpaceId:        spaceId,
			Topic:          topic,
			SenderIdentity: identity.Account(),
			Self:           identity.Equals(self),
			Payload:        payload,
		})
	})
	if err != nil {
		return nil, mapPubSubErr(err)
	}
	return cancel, nil
}

// mapPubSubErr lifts engine/adapter errors onto the public sentinels;
// unknown errors pass through verbatim.
func mapPubSubErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pubsubproto.ErrInvalidTopic):
		return fmt.Errorf("spaceimpl: pubsub: %w: %w", space.ErrPubSubInvalidTopic, err)
	case errors.Is(err, pubsubproto.ErrInvalidMessage):
		return fmt.Errorf("spaceimpl: pubsub: %w: %w", space.ErrPubSubPayloadTooLarge, err)
	case errors.Is(err, pubsubproto.ErrTopicNotOwned):
		return fmt.Errorf("spaceimpl: pubsub: %w: %w", space.ErrPubSubTopicNotOwned, err)
	case errors.Is(err, pubsubproto.ErrTooManyTopics):
		return fmt.Errorf("spaceimpl: pubsub: %w: %w", space.ErrPubSubTooManyPatterns, err)
	case errors.Is(err, anysyncx.ErrPubSubNoKey):
		return fmt.Errorf("spaceimpl: pubsub: %w: %w", space.ErrPubSubNoReadKey, err)
	}
	return err
}

// pubsubAclWatcher forwards ACL kicks into the pubsub engine's
// membership revalidation, so a removed member's serving-side interest
// is dropped promptly instead of on stream close. UpdateAcl runs
// synchronously on the syncacl write path — hand off to a goroutine
// (ACL records are rare; PubSubRevalidate is a cheap snapshot).
type pubsubAclWatcher struct {
	spaceId string
	app     *anysyncx.App
}

func (w *pubsubAclWatcher) UpdateAcl(_ list.AclList) {
	go w.app.PubSubRevalidate(w.spaceId)
}

// wirePubSubRevalidate subscribes the space's ACL kick fan-out to
// pubsub membership revalidation, once per loaded lifetime (the guard
// entry and the mux both die in closeSpaceRuntime; a reload rewires).
func (s *Service) wirePubSubRevalidate(spaceId string, handle anysyncx.SpaceHandle) {
	s.mu.Lock()
	if s.pubsubAclWired[spaceId] {
		s.mu.Unlock()
		return
	}
	s.pubsubAclWired[spaceId] = true
	s.mu.Unlock()
	if acl := handle.Inner().Acl(); acl != nil {
		s.aclKickFanout(spaceId, acl).add(&pubsubAclWatcher{spaceId: spaceId, app: s.app})
	}
}
