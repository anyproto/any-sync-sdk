// Package eventbus is the internal pub/sub for CRDT-level apply events.
// crdt.Controller publishes on apply; store/ subscriptions, properties/
// handlers, types/ registry, and syncstatus/ consume.
//
// Built on github.com/cheggaaa/mb/v3 — each subscriber gets its own
// bounded mailbox, no shared fan-out. See docs/06-data-structure.md
// §"Subscriptions / Event Flow".
package eventbus
