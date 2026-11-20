// Package store owns the query and subscription primitives over
// any-store. space/ wraps these primitives to build the public Query
// and Subscription types; middleware never imports store directly.
//
// Responsibilities:
//
//   - Query wrapper (filter, sort, limit, offset, projection) around
//     anystore.Collection.Find.
//   - Subscription: per-subscriber mb/v3 instance fed from the subscribe engine.
//   - dbRouter: scope → *anystore.DB, so we can start with one shared
//     DB and move to per-space DBs later (see docs/data-structure.md
//     §"Storage Topology").
package store
