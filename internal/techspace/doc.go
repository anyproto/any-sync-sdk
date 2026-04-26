// Package techspace is the hidden account-level space: a derived space
// with owner-only ACL, used as the account's index of spaces, chat-read
// tracker, and (later) account preferences.
//
// Derived deterministically from the account key, so it is always
// re-creatable. Never returned as a Space from the public API —
// callers interact through dedicated space-level methods (the space
// list is exposed via space.SpaceService).
//
// Implements space.Indexer, which is wired at sdk.Open: Space creation
// and deletion flow through techspace to update the space index.
//
// Handlers in this package:
//
//   - SpaceIndexHandler (spaceindex.go) — validates writes to the
//     derived space-index object (one record per space). Unique to
//     the tech space; does not appear on regular user objects.
//
// See docs/02-tech-space.md.
package techspace
