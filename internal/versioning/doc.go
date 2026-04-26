// Package versioning detects handler/schema version mismatches and
// triggers re-indexing by replaying the object tree through the current
// handler set (tree.Iterate → crdt.ApplyChange).
//
// Runs on SDK init before regular operation resumes. Scopes still being
// groomed (SDK version, per-handler version, any-store schema version);
// v1 minimum is SDK version + per-handler version. See docs/08-versioning.md.
package versioning
