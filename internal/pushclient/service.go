// The account-level push service — space.PushAPI. Composes the key
// provider (spaceimpl.Service.PushKeys), the request builders
// (messages.go) and the DRPC transport (client.go).

package pushclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/anyproto/anytype-push-server/pushclient/pushapi"
	"github.com/mr-tron/base58"

	"github.com/anyproto/any-sync-sdk/space"
)

// KeysProvider resolves a space's push keys from its ACL: the derived
// push signing key (from the first metadata key — fixed for the
// space's life) and the payload encryption key (from the CURRENT read
// key — rotates with it). Implemented by spaceimpl.Service.PushKeys.
type KeysProvider interface {
	PushKeys(ctx context.Context, spaceId string) (spaceKey crypto.PrivKey, encKey crypto.SymKey, err error)
}

// Service implements space.PushAPI. Always constructed by sdk.Open;
// when no push node is configured client is nil and every method
// returns space.ErrPushNotConfigured — so embedders can hold the
// handle unconditionally and gate on the error, not on nil.
type Service struct {
	client  *Client
	keys    KeysProvider
	account *accountdata.AccountKeys
}

// NewService builds the push service. client may be nil (push node not
// configured); keys and account must be non-nil.
func NewService(client *Client, keys KeysProvider, account *accountdata.AccountKeys) *Service {
	return &Service{client: client, keys: keys, account: account}
}

// Compile-time check against the public surface.
var _ space.PushAPI = (*Service)(nil)

// identity is the account identity string the push server sees on the
// secure channel (PubKey.Account() — StrKey form). Signed into
// CreateSpace/RemoveSpace and used as the own-identity topic.
func (s *Service) identity() string {
	return s.account.SignKey.GetPublic().Account()
}

// configured gates every method on the presence of a push node.
func (s *Service) configured() error {
	if s.client == nil {
		return space.ErrPushNotConfigured
	}
	return nil
}

// SetToken registers this device's platform push token.
func (s *Service) SetToken(ctx context.Context, platform space.PushPlatform, token string) error {
	if err := s.configured(); err != nil {
		return err
	}
	if token == "" {
		return errors.New("pushclient: empty token")
	}
	p, err := platformProto(platform)
	if err != nil {
		return err
	}
	return s.client.SetToken(ctx, &pushapi.SetTokenRequest{Platform: p, Token: token})
}

// RevokeToken drops this device's push token.
func (s *Service) RevokeToken(ctx context.Context) error {
	if err := s.configured(); err != nil {
		return err
	}
	return s.client.RevokeToken(ctx)
}

// RegisterSpace registers the space's derived push key with the
// server. Tolerates pushapi.ErrSpaceExists for idempotency (the
// current server always answers Ok, but older/stricter deployments may
// signal the duplicate).
func (s *Service) RegisterSpace(ctx context.Context, spaceId string) error {
	if err := s.configured(); err != nil {
		return err
	}
	spaceKey, _, err := s.keys.PushKeys(ctx, spaceId)
	if err != nil {
		return err
	}
	keyRaw, accountSig, err := buildSpaceRequest(spaceKey, s.identity())
	if err != nil {
		return err
	}
	err = s.client.CreateSpace(ctx, &pushapi.CreateSpaceRequest{SpaceKey: keyRaw, AccountSignature: accountSig})
	if errors.Is(err, pushapi.ErrSpaceExists) {
		return nil
	}
	return err
}

// RemoveSpace unregisters the space's push key.
func (s *Service) RemoveSpace(ctx context.Context, spaceId string) error {
	if err := s.configured(); err != nil {
		return err
	}
	spaceKey, _, err := s.keys.PushKeys(ctx, spaceId)
	if err != nil {
		return err
	}
	keyRaw, accountSig, err := buildSpaceRequest(spaceKey, s.identity())
	if err != nil {
		return err
	}
	return s.client.RemoveSpace(ctx, &pushapi.RemoveSpaceRequest{SpaceKey: keyRaw, AccountSignature: accountSig})
}

// SubscribeAll replaces the account's ENTIRE topic set (full replace —
// see space.PushAPI). Topics for every space are signed with that
// space's push key and submitted in one request.
func (s *Service) SubscribeAll(ctx context.Context, subs []space.PushSpaceTopics) error {
	if err := s.configured(); err != nil {
		return err
	}
	all := &pushapi.Topics{}
	for _, sub := range subs {
		spaceKey, _, err := s.keys.PushKeys(ctx, sub.SpaceId)
		if err != nil {
			return fmt.Errorf("pushclient: space %q: %w", sub.SpaceId, err)
		}
		topics, err := buildTopics(spaceKey, sub.Topics)
		if err != nil {
			return err
		}
		all.Topics = append(all.Topics, topics.Topics...)
	}
	return s.client.SubscribeAll(ctx, &pushapi.SubscribeAllRequest{Topics: all})
}

// Subscriptions returns the account's current topic set as raw
// {base58 spaceKey, topic} rows. The server strips signatures.
func (s *Service) Subscriptions(ctx context.Context) ([]space.PushSubscription, error) {
	if err := s.configured(); err != nil {
		return nil, err
	}
	resp, err := s.client.Subscriptions(ctx)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Topics == nil {
		return nil, nil
	}
	out := make([]space.PushSubscription, 0, len(resp.Topics.Topics))
	for _, t := range resp.Topics.Topics {
		out = append(out, space.PushSubscription{
			SpaceKey: base58.Encode(t.SpaceKey),
			Topic:    t.Topic,
		})
	}
	return out, nil
}

// Notify publishes an encrypted, account-signed notification to the
// given topics within spaceId.
func (s *Service) Notify(ctx context.Context, spaceId string, topics []string, payload []byte, groupId string) error {
	if err := s.configured(); err != nil {
		return err
	}
	if len(topics) == 0 {
		return errors.New("pushclient: no topics")
	}
	spaceKey, encKey, err := s.keys.PushKeys(ctx, spaceId)
	if err != nil {
		return err
	}
	tps, err := buildTopics(spaceKey, topics)
	if err != nil {
		return err
	}
	msg, err := buildMessage(s.account.SignKey, encKey, payload)
	if err != nil {
		return err
	}
	return s.client.Notify(ctx, &pushapi.NotifyRequest{Topics: tps, Message: msg, GroupId: groupId})
}

// NotifySilent wakes the account's own other devices: a body-less
// notify on the caller's own-identity topic within spaceId (the only
// topic the server accepts on the silent path).
func (s *Service) NotifySilent(ctx context.Context, spaceId string, groupId string) error {
	if err := s.configured(); err != nil {
		return err
	}
	spaceKey, _, err := s.keys.PushKeys(ctx, spaceId)
	if err != nil {
		return err
	}
	tps, err := buildTopics(spaceKey, []string{s.identity()})
	if err != nil {
		return err
	}
	return s.client.NotifySilent(ctx, &pushapi.NotifyRequest{Topics: tps, GroupId: groupId})
}

// platformProto maps the public string enum onto the proto enum.
func platformProto(p space.PushPlatform) (pushapi.Platform, error) {
	switch p {
	case space.PushPlatformIOS:
		return pushapi.Platform_IOS, nil
	case space.PushPlatformAndroid:
		return pushapi.Platform_Android, nil
	}
	return 0, fmt.Errorf("pushclient: unknown platform %q", p)
}
