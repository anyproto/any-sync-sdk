package space

import (
	"context"
	"errors"
)

// ErrPushNotConfigured is returned by every PushAPI method when the
// SDK was opened without a push node (config.Push empty). The API
// handle itself is always non-nil — configuration is checked at call
// time, not at Open.
var ErrPushNotConfigured = errors.New("space: push notifications not configured (no push peer in config)")

// PushPlatform selects the mobile push transport a device token
// belongs to. String-typed so config/wire layers can pass it through
// verbatim; the SDK maps it onto the push server's proto enum.
type PushPlatform string

const (
	PushPlatformIOS     PushPlatform = "ios"
	PushPlatformAndroid PushPlatform = "android"
)

// PushSpaceTopics is one space's slice of a SubscribeAll request: the
// topic strings this device wants notifications for within SpaceId.
// Topic strings are an app-level vocabulary (e.g. anytype-heart uses
// "chats", per-chat sha256 ids, and the account identity for
// mentions); the SDK signs them with the space's push key but does not
// interpret them.
type PushSpaceTopics struct {
	SpaceId string
	Topics  []string
}

// PushSubscription is one raw row from PushAPI.Subscriptions: the
// base58-encoded space push PUBLIC key (the server's space identifier
// — not a spaceId; the mapping is client-side via the derived key) and
// the topic string. Signatures are not returned by the server.
type PushSubscription struct {
	SpaceKey string
	Topic    string
}

// PushAPI is the client surface of the push-notification node — an
// out-of-band peer (config.Push) that fans mobile push notifications
// out to the account's registered device tokens by topic.
//
// The server is zero-knowledge about space content: topics are signed
// with a per-space key derived from the ACL (only members hold it) and
// notification payloads are encrypted with a key derived from the
// space's current read key — the server relays ciphertext. All methods
// are account-scoped (the server identifies the caller by the secure
// channel's account key) and return ErrPushNotConfigured when no push
// node is configured.
type PushAPI interface {
	// SetToken registers this device's platform push token (APNs / FCM)
	// with the server. Call again on token rotation; RevokeToken on
	// logout.
	SetToken(ctx context.Context, platform PushPlatform, token string) error

	// RevokeToken removes this device's push token — the device stops
	// receiving notifications for the account.
	RevokeToken(ctx context.Context) error

	// RegisterSpace registers the space's derived push key with the
	// server, enabling Notify/Subscribe for its topics. Requires read
	// access to the space's ACL (members only). Idempotent — the server
	// treats a re-registration as success.
	RegisterSpace(ctx context.Context, spaceId string) error

	// RemoveSpace unregisters the space's push key — its topics and
	// subscriptions are dropped server-side.
	RemoveSpace(ctx context.Context, spaceId string) error

	// SubscribeAll replaces the account's ENTIRE topic set across all
	// spaces (server semantics: full replace, not a merge). Callers own
	// the reconcile: compute the complete desired set — every space,
	// every topic — and submit it in one call; anything absent is
	// unsubscribed.
	SubscribeAll(ctx context.Context, subs []PushSpaceTopics) error

	// Subscriptions returns the account's current topic set as the
	// server holds it — raw {base58 spaceKey, topic} rows, without
	// signatures. Use it to diff local desired state against the server
	// before a SubscribeAll.
	Subscriptions(ctx context.Context) ([]PushSubscription, error)

	// Notify publishes an encrypted notification to the given topic
	// strings within spaceId. The payload (cleartext JSON by
	// convention) is encrypted with the space's derived key and signed
	// by this account; only space members can decrypt. groupId is the
	// server-side collapse key (e.g. a chat id) — an opaque grouping
	// hint, empty for none.
	Notify(ctx context.Context, spaceId string, topics []string, payload []byte, groupId string) error

	// NotifySilent wakes the account's OWN other devices with a
	// data-only push (no user-visible message) — e.g. after a read-state
	// change, so their badges refresh. It targets only the caller's
	// own-identity topic within spaceId; the server drops any other
	// topic on the silent path.
	NotifySilent(ctx context.Context, spaceId string, groupId string) error
}
