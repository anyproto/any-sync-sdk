// Package properties owns the per-space `properties` system dataset:
// the CRDT handlers that keep it consistent with the object-owned
// base-scope data, the variant merge (device > account > base) used
// by projection, and the reserved variant field names
// (_device, _account, _base).
//
// Handlers in this package:
//
//   - SystemPropertiesHandler (system.go) — the "baseProperty"
//     handler from docs/06-data-structure.md, renamed to match its
//     role: it is the single crdt.Handler registered on every user
//     object for base-scope property writes, and it projects those
//     writes into the space's `properties` collection while enforcing
//     "object can only update its own record".
//
// The account-level rewrite handler (applies `_account` variants
// across spaces) lives alongside the tech-space space-index handler
// in internal/techspace/, because its source data is a derived
// object in the tech space rather than a per-object write.
//
// Not public — space.PropertiesAPI wraps the setters and projection.
// See docs/06-data-structure.md § "Object Properties".
package properties
