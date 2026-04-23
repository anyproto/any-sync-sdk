# Versioning & Re-indexing

## Vision
The SDK is a living library — new app versions add new dataset handlers, change existing handler logic, add system datasets, or modify indexing. When this happens, the any-store state (derived from replaying object tree changes through handlers) becomes stale and must be rebuilt.

Versioning is the mechanism that detects staleness and triggers re-indexing.

## Why We Need It

- **New handler added** — a dataset that was previously ignored now has a handler. Existing changes must be replayed so the new dataset appears in any-store
- **Handler logic changed** — the same change produces different any-store state. All existing records must be recomputed
- **Schema evolution** — a new version adds or renames fields; old data needs migration
- **New system datasets** — SDK adds a new built-in dataset (e.g., for search indexing); existing objects need to be re-indexed
- **Bug fixes in handlers** — silently corrupted state needs a rebuild

## Version Scopes

Multiple version counters live in different parts of the system. Each scope answers a different question: "does this piece of state need to be rebuilt?"

Candidate scopes (to refine during grooming):

| Scope | Unit | Triggers rebuild of |
|-------|------|---------------------|
| **SDK version** | whole SDK | Everything (migration) |
| **Handler version** | per dataset handler | All records in that dataset across all objects |
| **Dataset version** | per dataset registration | Records of that dataset |
| **Object-index version** | system object index | The index dataset only |
| **any-store schema version** | any-store DB | DB-level migrations (indexes, collections) |

## Re-indexing Flow (rough)

```
On SDK init:
  1. Read stored versions from any-store
  2. Compare with current handler/SDK versions
  3. If mismatch:
     a. For each affected object: open tree, clear derived state in any-store
     b. Re-iterate tree.Iterate → applyChange(handler) → write to any-store
     c. Write new versions
  4. Resume normal operation
```

## Source files
- `any-sync/commonspace/object/tree/objecttree/` — `tree.Iterate` is the foundation for re-indexing

## Grooming Questions

### Version Granularity
1. Which version scopes do we actually need in v1? (Minimum: SDK version + handler version?)
2. Where are versions stored? (Dedicated system dataset in each object? Global in tech space? Local-only on device?)
3. Are versions per-device (re-index on each device independently) or synced (some other device already re-indexed, skip)?

### Re-indexing Strategy
4. Full rebuild vs incremental — in v1, always wipe and replay, or try to migrate in place?
5. When to run — on SDK init (blocking startup), in background after init, or lazy (on first access)?
6. Large objects — can we re-index a million-record object without locking the UI?
7. Failure handling — if re-indexing fails midway, is any-store left in a partially-rebuilt state? Rollback?

### Handler Versioning
8. How does a handler declare its version? Static field on the handler struct? Semver? Single integer?
9. Multiple handlers for the same dataset across versions — coexist or hard replace?
10. Can the caller force a rebuild (`sdk.Reindex(datasetName)`)?

### Edge Cases
11. App downgrade — new version wrote data old handlers don't understand. What happens?
12. Partial handler updates — only one dataset changed; do we re-index only that dataset or the whole object?
13. Re-indexing while sync is active — what if new remote changes arrive mid-rebuild?

### Dependencies
14. Tightly coupled with CRDT (handlers) and Object (tree.Iterate). Affects startup time (Data Structure section).
