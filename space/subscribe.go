package space

import "github.com/anyproto/any-store/v2/anyenc"

// Subscription delivers simplified user events (inserted / updated /
// deleted) for a fixed set of objects. Backed internally by a
// per-subscriber mb/v3 mailbox.
type Subscription interface {
	// Events returns the event channel. Closed when the subscription
	// is canceled.
	Events() <-chan Event

	// Close cancels the subscription and releases the mailbox.
	Close() error
}

// SubscribeOpts narrows the subscription.
type SubscribeOpts struct {
	// Datasets filters to named datasets; empty means all datasets.
	Datasets []string

	// IncludeOwnWrites, when false, filters events originating from
	// the same session as this subscription. v1: always treated as
	// true (session model is deferred). See docs/05-crdt.md §"Own-write
	// events".
	IncludeOwnWrites bool
}

// Event is one batch of record changes delivered to a Subscription.
type Event struct {
	SpaceId  string
	ObjectId string
	Dataset  string
	Records  []RecordEvent
}

// RecordEvent describes one record's change in a single batch. For
// EventUpdated / EventInserted, Fields carries the delta (the ops'
// effects, not the full record); middleware re-reads any-store via
// Query for the full state if needed.
type RecordEvent struct {
	Id     string
	Type   EventType
	Fields map[string]*anyenc.Value
}

// EventType is the simplified user-event shape.
type EventType uint8

const (
	EventInserted EventType = iota
	EventUpdated
	EventDeleted
)
