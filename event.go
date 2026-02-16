package syncsdk

type EventType int

const (
	ObjectUpdated     EventType = iota + 1
	ObjectRebuilt
	SpaceConnected
	SpaceDisconnected
)

type Event struct {
	Type     EventType
	SpaceID  string
	ObjectID string
	Heads    []string
}

type Handler func(Event)
