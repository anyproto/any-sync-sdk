package space

// MembersAPI is a thin facade over the members system collection.
// Members are exposed as any other collection (Query/Subscribe via the
// Space surface) — this facade adds member-specific helpers like
// "get my own member record" and pending-request management.
//
// Placeholder — groomed in the members pass (docs/03-space.md §"Members
// as a collection").
type MembersAPI interface {
	// TODO: Me, Get(identity), List, PendingRequests, UpdateOwnMetadata
}
