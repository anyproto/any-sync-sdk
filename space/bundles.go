// Bundles registry — the public types for the per-space `bundles`
// dataset: one row per bundle installed into the space,
// living on the spaceIndex object. The dataset is system-owned like
// the space list: reads go through the generic dataset surface
// (Space.Query(SpaceIndexObjectId(), "bundles")), writes only through
// the typed BundlesAPI.
//
// Concurrent installs converge by CRDT: the winner field is an LWW
// register, every claimed root accumulates in an add-only set, so the
// losing device's objects stay discoverable for merge and cleanup.

package space

import (
	"context"
	"errors"
)

// Bundles-registry sentinels — wrapped by the BundlesAPI methods so
// callers can classify with errors.Is.
var (
	// ErrBundleUnknown — no live record for the bundle id.
	ErrBundleUnknown = errors.New("bundle unknown")
	// ErrBundleBadRequest — structurally invalid input (empty bundle
	// id, nil NewRoot, empty root id).
	ErrBundleBadRequest = errors.New("bundle bad request")
	// ErrBundleNotLoser — ResolveLoser target is not a loser of the
	// bundle: it is the current winner, or was never claimed in roots.
	ErrBundleNotLoser = errors.New("bundle root is not a loser")
)

// Bundle is one row of the bundles registry.
type Bundle struct {
	// Id is the stable bundle identifier (marketplace id or a
	// hardcoded slug like "bao/v1") — the record id.
	Id string
	// Name is the bundle's display name.
	Name string
	// RootId is the winning root object id — the converged LWW value.
	// Children of the setup are derived from it (DeriveObjectOpts.
	// ParentId), so this one id transitively names the whole install.
	RootId string
	// Roots is every root object id ever claimed for this bundle, in
	// arrival order. Add-only audit trail — entries are never removed;
	// a resolved loser's death is recorded by its tree deletion.
	Roots []string
	// Losers is the live conflict set: Roots minus the winner minus
	// roots whose trees are already deleted. Non-empty means a
	// concurrent-install conflict awaits resolution — merge what
	// matters out of each loser, then ResolveLoser it.
	Losers []string
}

// EnsureBundleRequest is the input to BundlesAPI.Ensure.
type EnsureBundleRequest struct {
	// Id is the stable bundle identifier. Required.
	Id string
	// Name is the display name, written on install ($set — the
	// converged value is whichever install wins). Optional.
	Name string
	// NewRoot creates the bundle's root object and returns its id,
	// called only when no winner exists yet. The root must be a
	// non-derived object (Objects().Create) — derived trees cannot be
	// deleted, and a losing root must be deletable. Required.
	//
	// Ensure stamps `any.name` (Name, falling back to Id) on the new
	// root: the root tree must carry a non-root change to enter the
	// head-sync diff, or a losing root could never be resolved from
	// another device.
	NewRoot func(ctx context.Context) (rootId string, err error)
}

// BundlesAPI is the typed surface over the per-space bundles registry.
// Obtained from Space.Bundles().
type BundlesAPI interface {
	// Ensure installs the bundle or adopts the existing install:
	// when a live record with a winner exists, it is returned as-is
	// (NewRoot is not called); otherwise NewRoot creates the root
	// object and one change registers it ($set rootId + $addToSet
	// roots). Fully local — no network wait; two devices ensuring
	// concurrently each register their root and the CRDT converges on
	// one deterministic winner after sync, the other surfacing in
	// Losers. Callers must therefore treat the returned RootId as
	// provisional until the space has synced, and re-read after.
	Ensure(ctx context.Context, req EnsureBundleRequest) (Bundle, error)

	// Get returns the bundle row. ErrBundleUnknown when no live record
	// exists OR the winning root's tree is deleted — a dead winner
	// reads as uninstalled everywhere (Get, List, Ensure's adopt gate),
	// and the next Ensure reinstalls with a fresh root. As of local
	// state — sync first for a network answer.
	Get(ctx context.Context, bundleId string) (Bundle, error)

	// List returns every live bundle row.
	List(ctx context.Context) ([]Bundle, error)

	// ResolveLoser deletes a losing root object (cascade-deleting its
	// derived children) after the caller has merged whatever content
	// mattered out of it. The target must be a claimed root and must
	// not be the current winner (ErrBundleNotLoser). Idempotent: a
	// root already deleted returns nil. Deletion needs the loser's
	// tree synced to this device — until then the call errors and is
	// retried after sync (or run from the device that created the
	// loser). Never auto-invoked — loser cleanup is always an explicit
	// caller decision.
	ResolveLoser(ctx context.Context, bundleId, loserRootId string) error
}
