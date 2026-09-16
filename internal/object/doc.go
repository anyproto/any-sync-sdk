// Package object binds one any-sync object tree to one crdt.Controller
// and exposes the lifecycle surface used by space/: Create / Derive /
// Modify / Delete / Subscribe, plus snapshot heuristics (from
// anytype-heart) and ocache wiring so trees are loaded on demand and
// TTL-closed when idle.
//
// Not caller-facing. Middleware never holds an Object handle — it
// operates at the space level and references objects by objectId.
// See docs/object.md.
package object
