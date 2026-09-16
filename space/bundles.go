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
	// id, no root strategy, both strategies, RootType / RootCollections
	// / RootProperties next to NewRoot, RootType next to a declaration,
	// a seeded value that cannot be encoded, an invalid or duplicate
	// part / dataset / property declaration, Parts or Layout on a
	// Collection declaration, metadata without a declaration, or a
	// tech-space request with NewRoot or without a declaration.
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
	// stamps its membership (RootType / RootCollections).
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
	// RootType is the type of the root Ensure mints — required on a
	// derived root that declares nothing (every object has a type),
	// written with its name in the root's first change and, on a later
	// Ensure, set when the row has no type yet (a type it already has
	// is never replaced). Refused next to a declaration (Parts /
	// Properties / XKey / Collection): a definition object carries its
	// marker in `any.type` and has no type of its own. A caller-minted
	// root (NewRoot) gets its type from NewRoot and refuses it here.
	RootType string
	// RootCollections are added to the root Ensure mints in that same
	// change, and on every later Ensure a collection the row lacks is
	// added ($addToSet, idempotent), so a request that gains one
	// reaches an existing install. NewRoot roots refuse them.
	RootCollections []string
	// RootProperties seeds the root's property values, keyed owner
	// (typeId or collectionId) → propId → value, in that same change
	// (their own op), before the install is registered, so a failed
	// seed leaves no install to adopt. Install only — an adopt never
	// re-seeds, the installer's values sync in. A keyed owner that is
	// neither RootType nor the root's own declaration is added to
	// RootCollections — a property write to an owner the object does
	// not have is rejected. Values are checked to encode before any
	// root is minted. The root's own id is a usable key only through
	// its declaration: its property ids are derived from (rootId,
	// XKey) and known before the install. Roots Ensure mints only.
	RootProperties map[string]map[string]any

	// Parts declares parts (with their datasets) on the root — derived
	// or created — which then defines a type: any.type = "__type__",
	// typeId = rootId. Records live on the objects of that type in the
	// declared collections (`<rootId>_<key>` for a namespaced dataset,
	// the module's canonical collection for a shared one),
	// discoverable through Types().Parts(rootId) / Datasets(rootId) and
	// Space.Datasets(), writable through Modify/Upsert. The root
	// itself hosts them too: a definition object implicitly implements
	// itself, which is how a bundle keeps its own records (favourites
	// entries, an app's layouts) on its root. Refused with Collection.
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
	// and Ensure mints and stamps the root itself — the only create a
	// space with a fenced object lifecycle (the tech space) allows.
	// Parts or Properties are required on the tech space.
	Parts []PartDraft

	// Properties declares property definitions on the root, which then
	// defines a type like Parts does — or a collection, with
	// Collection set — for a bundle that IS a definition other objects
	// use (a wiki collection's `parentId` / `pos`).
	// Every draft needs an XKey, unique within the request: the
	// property id is DERIVED from (root id, XKey), so two devices
	// installing while apart mint one column per handle instead of
	// two. Declared in one change after the registering write; on
	// adopt only the definitions the root lacks are written — one is
	// present when its id exists (live, or removed through
	// Types().RemoveProperty: the tombstone keeps the id) or a live
	// definition carries its handle under any id, so nothing is
	// patched, resurrected or doubled. Later evolution goes through
	// Types().AddProperty / PatchProperty / RemoveProperty with typeId
	// = rootId; a property added that way gets an ordinary
	// change-derived id. Kind, Scope and XKey are validated as
	// AddProperty validates them, before any root is minted.
	Properties []PropertyDraft

	// XKey is the root definition's handle (TypeInfo.XKey /
	// CollectionInfo.XKey, stored as `type.xkey` / `collection.xkey`):
	// the stable slug a consumer resolves the definition by, and what
	// `relation.targetTypes` in other declarations name. An XKey alone
	// is a declaration — a MARKER type, or with Collection a marker
	// collection objects are filed under as a flag, with no columns.
	// Written with the name stamp on install; on adopt it is filled in
	// only when the root carries none (an install that predates the
	// handle), never changed. Not unique on the SDK side: the consumer
	// enforces handle uniqueness.
	XKey string
	// Layout and Hidden seed the root definition's rendering and
	// listing metadata with the name stamp, on install only — adopt
	// never patches them. They need a declaration — Parts, Properties
	// or XKey (ErrBundleBadRequest otherwise); Layout describes a
	// type and is refused with Collection. Hidden is EXPLICIT: a root
	// that only hosts its bundle's records should ask for it, since a
	// listed type is one a client may set on other objects, granting
	// them the bundle's collections; a root that is a definition other
	// objects use (a page, a wiki) stays listed.
	Layout map[string]any
	Hidden bool

	// Collection makes the declaration a COLLECTION instead of a type:
	// the root carries `__collection__` in `any.type`, its handle and
	// flags live under `collection.*`, and Properties are its columns.
	// Parts and Layout are refused with it (a collection has neither).
	Collection bool
}

// DeclaresType reports whether the request makes the root a type
// definition — Parts, Properties, or an XKey alone (a marker type),
// without Collection.
func (r EnsureBundleRequest) DeclaresType() bool {
	return !r.Collection && (len(r.Parts) > 0 || len(r.Properties) > 0 || r.XKey != "")
}

// DeclaresCollection reports whether the request makes the root a
// collection definition — Collection with Properties or an XKey.
func (r EnsureBundleRequest) DeclaresCollection() bool {
	return r.Collection && (len(r.Properties) > 0 || r.XKey != "")
}

// Declares reports whether the request makes the root a definition of
// either kind.
func (r EnsureBundleRequest) Declares() bool {
	return r.DeclaresType() || r.DeclaresCollection()
}

// EnsureOption tunes one Ensure call. Options carry what must never
// come from a request body: a consumer maps client input onto
// EnsureBundleRequest and adds options from its own code paths only.
type EnsureOption func(*EnsureOptions)

// EnsureOptions is the resolved option set.
type EnsureOptions struct {
	// SystemInstall marks the consumer's own catalog install: it lifts
	// the reserved-module refusal (handler.Module.Reserved) for this
	// call. The reservation exists so only the consumer's installs
	// declare such a module.
	SystemInstall bool
}

// SystemInstall marks the call as the consumer's own install — see
// EnsureOptions.SystemInstall.
func SystemInstall() EnsureOption {
	return func(o *EnsureOptions) { o.SystemInstall = true }
}

// ApplyEnsureOptions folds opts into an EnsureOptions.
func ApplyEnsureOptions(opts ...EnsureOption) EnsureOptions {
	var o EnsureOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
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
	// A declaring root (Parts, Properties or XKey; Collection for a
	// collection) is stamped as a definition: the root's first change
	// carries its membership (the marker in `any.type`, or RootType;
	// RootCollections), `any.name`, the definition metadata (`xkey` /
	// `layout` / `hidden` under `type` or `collection`) and the
	// RootProperties values together — one `objects` change — then the
	// registry row, then the declarations (one `properties` change,
	// one `datasets` change for a type): root + up to 3 changes. A
	// crash mid-install leaves a registered row the retry heals
	// idempotently. Two devices declaring the same name concurrently
	// converge on one definition after sync; a DefId read before
	// convergence may change — look definitions up by name when
	// evolving them.
	//
	// On the tech space (Service.Get(SDK.TechSpaceId())) roots are
	// minted by Ensure only — NewRoot is refused — and a declaration
	// (Parts, Properties or XKey) is required; both root strategies
	// are available.
	//
	// The bool reports whether THIS call registered the install.
	// False means an existing one was adopted — which for a derived
	// root may still materialize its tree locally.
	//
	// Options carry what a request body must never say: SystemInstall
	// admits a reserved module (handler.Module.Reserved) for the
	// consumer's own install.
	Ensure(ctx context.Context, req EnsureBundleRequest, opts ...EnsureOption) (Bundle, bool, error)

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
