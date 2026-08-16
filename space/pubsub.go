package space

import (
	"context"
	"errors"
)

// PubSub errors. Publish/Subscribe wrap these sentinels; match with
// errors.Is.
var (
	// ErrPubSubInvalidTopic — the topic or pattern is malformed:
	// segments are `/`-separated, non-empty, at most 16 per topic and
	// 256 bytes total; wildcards (`*`, `>`) are valid in patterns only.
	ErrPubSubInvalidTopic = errors.New("space: invalid pubsub topic or pattern")
	// ErrPubSubPayloadTooLarge — the payload exceeds the network's
	// per-message cap (64 KiB by default).
	ErrPubSubPayloadTooLarge = errors.New("space: pubsub payload too large")
	// ErrPubSubTopicNotOwned — the topic is in the reserved self-owned
	// `acc/…/<accountId>` namespace of another account.
	ErrPubSubTopicNotOwned = errors.New("space: pubsub topic owned by another account")
	// ErrPubSubNoReadKey — this identity has no read key for the space
	// (keyless reader / guest view), so it cannot encrypt a publish.
	ErrPubSubNoReadKey = errors.New("space: no read key for pubsub")
	// ErrPubSubTooManyPatterns — the per-space subscription pattern cap
	// (100 by default) is exhausted.
	ErrPubSubTooManyPatterns = errors.New("space: too many pubsub patterns")
)

// PubSubMessage is one verified, decrypted pub/sub message handed to a
// Subscribe callback. SenderIdentity is proven by the message's
// signature (checked before dispatch) — the authoritative sender
// account, not anything self-declared inside Payload.
type PubSubMessage struct {
	// SpaceId the message was published in.
	SpaceId string
	// Topic the publisher targeted (concrete, no wildcards).
	Topic string
	// SenderIdentity is the sender's account address (same form as
	// Account().Id() / Member.Identity).
	SenderIdentity string
	// Self is true when the message is the loopback delivery of this
	// account's own publish (any device of the account).
	Self bool
	// Payload is the decrypted application payload.
	Payload []byte
}

// PubSubAPI is ephemeral, space-scoped pub/sub: fire-and-forget,
// at-most-once, no persistence, no replay. Messages are encrypted with
// the space read key, signed by the sender, and fanned out to the
// space's members via the responsible sync node and any directly
// connected LAN peers. Offline members simply miss messages — payloads
// must be idempotent or last-write-wins.
//
// Topics are `/`-separated (e.g. "chat/room1"). Subscribe patterns may
// use NATS-style wildcards: `*` matches exactly one segment, a
// trailing `>` matches one or more. The `acc/…/<accountId>` namespace
// is self-owned — only that account can publish under it (enforced at
// publisher, relay and receiver), which makes it spoof-proof for
// presence-style topics.
//
// The handle is always non-nil; per-call state is checked at use time.
type PubSubAPI interface {
	// Publish sends payload on topic. Returns fast after local
	// validation, signing and encryption — network delivery is
	// asynchronous and unacknowledged. Local subscribers with a
	// matching pattern receive the message synchronously (Self=true).
	Publish(ctx context.Context, topic string, payload []byte) error

	// Subscribe registers cb for messages matching pattern and pushes
	// the interest to the space's peers. cb runs on a single shared
	// dispatch goroutine and MUST NOT block — slow handlers drop
	// messages for every subscriber. Do not call Publish synchronously
	// from cb while holding locks cb needs. The returned cancel is
	// idempotent; cb may fire once more after cancel returns.
	Subscribe(pattern string, cb func(PubSubMessage)) (cancel func(), err error)
}
