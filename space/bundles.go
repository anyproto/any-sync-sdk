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
	// ErrBundleBadRequest — structurally invalid input: empty bundle
	// id, nil NewRoot, empty root id, DerivedRoot-only fields on a
	// created root, an invalid or duplicate part / dataset declaration,
	// or a tech-space request that is not derived-only with Parts.
	ErrBundleBadRequest = errors.New("bundle bad request")
	// ErrBundleNotLoser — ResolveLoser target is not a loser of the
	// bundle: it is the current winner, or was never claimed in roots.
	ErrBundleNotLoser = errors.New("bundle root is not a loser")
	// ErrLoserNotSynced — ResolveLoser target's tree has not synced to
	// this device yet (deletion needs the local head entry). Retry
	// after sync, or resolve from a device that holds the tree.
	ErrLoserNotSynced = errors.New("bundle loser tree not synced locally")

	// ErrBundleRootNotSynced: the registry references a root whose tree
	// (or objects row) has not arrived on this device yet. Transient —
	// retry after sync.
	ErrBundleRootNotSynced = errors.New("bundle root not yet synced locally")
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
	// Derived reports that the winner is the bundle's canonical
	// derived root (EnsureBundleRequest.DerivedRoot). Such an install
	// cannot fork and cannot be uninstalled: the root id is a pure
	// function of (space, bundle id), and derived trees are not
	// deletable. Computed on read, not stored.
	Derived bool
}

// EnsureBundleRequest is the input to BundlesAPI.Ensure. Exactly one
// root strategy is required: NewRoot (a created root) or DerivedRoot
// (the canonical derived root).
type EnsureBundleRequest struct {
	// Id is the stable bundle identifier. Required.
	Id string
	// Name is the display name, written on install ($set — the
	// converged value is whichever install wins). Optional.
	Name string
	// NewRoot creates the bundle's root object and returns its id,
	// called only when no winner exists yet. The root must be a
	// non-derived object (Objects().Create) — a losing root must be
	// deletable, and derived trees are not. Required unless
	// DerivedRoot.
	//
	// Ensure stamps `any.name` (Name, falling back to Id) on the new
	// root: the root tree must carry a non-root change to enter the
	// head-sync diff, or a losing root could never be resolved from
	// another device.
	NewRoot func(ctx context.Context) (rootId string, err error)

	// DerivedRoot installs the bundle on its CANONICAL DERIVED root —
	// derived from the bundle id, so every device computes the same
	// root id with zero communication and concurrent installs cannot
	// fork. Ensure derives the root itself (NewRoot must be nil) and
	// attaches RootTypes.
	//
	// Two consequences, both permanent: the install can never be
	// uninstalled (derived trees are not deletable, so a dead-winner
	// reinstall is impossible), and a claimed canonical root always
	// wins the registry — see BundlesAPI.Ensure. The EXCEPTION, not
	// the default: bundles exist so a converged install does not need
	// a derived object, and the registry resolves created-root forks.
	// Derive only when a fork would be unmergeable — chat-like content,
	// above all the 1-1 general chat, where the convergence gate cannot
	// work — never for anything a user may remove or for id convenience.
	DerivedRoot bool
	// RootTypes are attached to the derived root at materialization,
	// idempotently. DerivedRoot only — a created root gets its types
	// from NewRoot.
	RootTypes []string
	// RootProperties seeds the derived root's property values, keyed
	// typeId → propId → value, written before the install is
	// registered so a failed seed leaves no install to adopt. Every
	// keyed type is attached along with RootTypes — a property write
	// to a type the object does not implement is rejected. The root's
	// own id is not a usable key: a self-typed root (Parts) grants a
	// dataset namespace, not property definitions. DerivedRoot only.
	RootProperties map[string]map[string]any

	// Parts declares parts (with their datasets) on the root — derived
	// or created — which then implements itself as a type: any.types =
	// ["__type__", rootId], typeId = rootId. Records live on the root
	// in the declared collections (`<rootId>_<key>` for a namespaced
	// dataset, the module's canonical collection for a shared one),
	// discoverable through Types().Parts(rootId) / Datasets(rootId) and
	// Space.Datasets(), writable through Modify/Upsert on the root.
	// Declared in one change after the registering write, and on adopt
	// only when the root's tree is local and carries no declaration
	// yet (crash between registering and declaring, a row adopted
	// before the root tree synced); a root with any declaration —
	// live, or removed through Types().RemovePart — is left alone:
	// nothing is patched, added or resurrected by Ensure. Later
	// evolution goes through Types().AddPart / AddDataset /
	// AddDatasetField / PatchDataset with typeId = rootId. An invalid
	// or duplicate draft fails the request with ErrBundleBadRequest
	// before any root is minted. With a created strategy, omit NewRoot
	// and Ensure mints and self-types the root itself — the only create
	// a space with a fenced object lifecycle (the tech space) allows.
	// Required on the tech space.
	Parts []PartDraft
}

// BundlesAPI is the typed surface over the per-space bundles registry.
// Obtained from Space.Bundles().
type BundlesAPI interface {
	// Ensure installs the bundle or adopts the existing install:
	// when a live record with a winner exists, it is returned as-is
	// (no root is created); otherwise the root is minted — NewRoot for
	// a created root, the canonical derivation for DerivedRoot — and
	// one change registers it ($set rootId + $addToSet roots). Fully
	// local — no network wait; two devices ensuring concurrently each
	// register their root and the CRDT converges on one deterministic
	// winner after sync, the other surfacing in Losers. Callers must
	// therefore treat the returned RootId as provisional until the
	// space has synced, and re-read after.
	//
	// Adoption wins over derivation: a bundle already installed on a
	// created root stays on it, even when this call asks for
	// DerivedRoot. Nothing migrates behind the caller's back — check
	// Bundle.Derived to see what the install actually is.
	//
	// A CLAIMED CANONICAL DERIVED ROOT ALWAYS WINS the registry, on
	// every replica, whatever the rootId register says. The claim set
	// is add-only, so the verdict itself is order-independent: every
	// replica reads the same winner from any prefix containing the
	// claim, which is what keeps a derived root (undeletable, and
	// therefore unresolvable as a loser) from ever becoming one.
	//
	// The verdict is not a race, but the CLAIM can be: a device that
	// installs a derived root without a converged registry demotes an
	// existing created install to a loser, irreversibly, on every
	// replica. Converge before installing derived into a space that
	// may already carry a created install of the same id — see
	// docs/bundles.md § Derived roots.
	//
	// With Parts the derived root is stamped as a type implementing
	// itself and the declarations are written on it before the
	// registry row, so a crash mid-install leaves no install to adopt
	// and the retry re-runs idempotently. Two devices declaring the
	// same name concurrently converge on one definition after sync;
	// a DefId read before convergence may change — look definitions
	// up by name when evolving them.
	//
	// On the tech space (Service.Get(SDK.TechSpaceId())) bundles are
	// derived-only and must declare Parts; NewRoot is refused.
	//
	// The bool reports whether THIS call registered the install.
	// False means an existing one was adopted — which for a derived
	// root may still materialize its tree locally.
	Ensure(ctx context.Context, req EnsureBundleRequest) (Bundle, bool, error)

	// Get returns the bundle row. ErrBundleUnknown when no live record
	// exists OR the winning root's tree is deleted — a dead winner
	// reads as uninstalled everywhere (Get, List, Ensure's adopt gate),
	// and the next Ensure reinstalls with a fresh root. As of local
	// state — sync first for a network answer.
	Get(ctx context.Context, bundleId string) (Bundle, error)

	// List returns every live bundle row.
	List(ctx context.Context) ([]Bundle, error)

	// DerivedRootId is the id the bundle's derived root has in this
	// space — a pure function of (space, bundle id), computed without
	// reading the registry, materializing anything, or touching the
	// network. Every device and every member gets the same answer,
	// offline, which is what makes DerivedRoot installs fork-proof.
	//
	// It answers "where would this bundle live", not "is it
	// installed": a bundle installed on a created root lives
	// elsewhere, and an uninstalled one lives nowhere yet.
	DerivedRootId(ctx context.Context, bundleId string) (string, error)

	// ResolveLoser deletes a losing root object (cascade-deleting its
	// derived children) after the caller has merged whatever content
	// mattered out of it. The target must be a claimed root and must
	// not be the current winner (ErrBundleNotLoser). Idempotent: a
	// root already deleted returns nil. Deletion needs the loser's
	// tree synced to this device — until then the call fails with
	// ErrLoserNotSynced (retry after sync, or run from the device that
	// created the loser). Never auto-invoked — loser cleanup is always
	// an explicit caller decision.
	ResolveLoser(ctx context.Context, bundleId, loserRootId string) error
}
