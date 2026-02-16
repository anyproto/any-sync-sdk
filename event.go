package syncsdk

import "github.com/anyproto/any-sync-sdk/keys"

type EventType int

const (
	ObjectUpdated       EventType = iota + 1
	ObjectRebuilt
	SpaceConnected
	SpaceDisconnected
	JoinRequestReceived // Someone requested to join via approval invite
)

type Event struct {
	Type     EventType
	SpaceID  string
	ObjectID string
	Heads    []string
	Identity keys.PublicKey // Populated for ACL events (nil for object events)
}

type Handler func(Event)
