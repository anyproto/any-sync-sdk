// Bundles registry — the typed API over the `bundles`
// dataset on the in-space spaceIndex object. Ensure is adopt-or-
// install; concurrent installs from different devices converge by
// CRDT (LWW winner + add-only claim set) and losers are resolved
// explicitly by the caller after merging. See space/bundles.go for
// the public contract and docs/bundles.md for the design.

package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

var bundleLog = logger.NewNamed("sdk.bundles")

type bundlesAPI struct {
	parent *spaceImpl
	// locks serialize local Ensure calls per bundle id (read-then-
	// write). Cross-device races are the CRDT's job; this only stops
	// one process from installing the same bundle twice in parallel.
	// Per-id — NewRoot runs under its own bundle's lock only, so a
	// composite install may Ensure OTHER bundles from inside NewRoot
	// and a slow NewRoot doesn't stall unrelated installs. Same-id
	// reentry from NewRoot still self-deadlocks: don't.
	mu    sync.Mutex
	locks map[string]*sync.Mutex
	// derivedIds memoizes canonicalRootId per bundle id. The answer is
	// a pure function of (space, bundle id) and never changes.
	derivedIds sync.Map
}

func newBundlesAPI(parent *spaceImpl) *bundlesAPI {
	return &bundlesAPI{parent: parent, locks: map[string]*sync.Mutex{}}
}

func (b *bundlesAPI) lockFor(bundleId string) *sync.Mutex {
	b.mu.Lock()
	defer b.mu.Unlock()
	l := b.locks[bundleId]
	if l == nil {
		l = &sync.Mutex{}
		b.locks[bundleId] = l
	}
	return l
}

// indexObj returns the resident spaceIndex object, materializing the
// derived tree when absent (idempotent — every deriver mints the same
// tree; the owner's changes merge in when sync delivers them). On the
// tech handle the index object is the tech index, already resident.
func (b *bundlesAPI) indexObj(ctx context.Context) (*object.Object, error) {
	if id := b.parent.techIndexId; id != "" {
		return b.parent.store.Get(ctx, id)
	}
	return b.parent.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangePayload: []byte(spaceindex.WellKnownDeriveSeed),
	})
}

func (b *bundlesAPI) Ensure(ctx context.Context, req space.EnsureBundleRequest) (space.Bundle, bool, error) {
	if req.Id == "" {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	if err := b.validateEnsureRequest(req); err != nil {
		return space.Bundle{}, false, err
	}
	if err := b.parent.writeGate(ctx); err != nil {
		return space.Bundle{}, false, err
	}
	l := b.lockFor(req.Id)
	l.Lock()
	defer l.Unlock()

	obj, err := b.indexObj(ctx)
	if err != nil {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: load spaceIndex object: %w", err)
	}
	if err := b.preflightType(ctx, req); err != nil {
		return space.Bundle{}, false, err
	}

	// Adopt path: a live record whose winner's tree is not deleted
	// means the bundle is installed — locally-known state; the winner
	// may still flip when a concurrent remote install syncs in
	// (callers re-read after sync, see the public contract). A DELETED
	// winner reads as uninstalled — otherwise the bundle id would be
	// permanently wedged (record deletes are rejected, rootId has no
	// unset, ResolveLoser refuses the winner) — and falls through to a
	// fresh install.
	// Adoption wins over derivation: an install already on a created
	// root stays there even when this call asks for a derived one.
	// Migration is content movement, which only the app can decide.
	if bd, live := b.view(ctx, obj.Controller().Get(ctx, spaceindex.BundlesDataset, req.Id)); live {
		// The registry row can arrive before the root tree does (they
		// are different trees). When the winner is the canonical
		// derived root, this device can materialize it itself instead
		// of handing back an id whose tree is still in flight —
		// idempotent, and it converges with whatever syncs in. The
		// stamp is part of that: a tree still sitting on its root
		// change is left out of this device's head-sync diff, so its
		// local copy only rejoins sync once something writes to it.
		// RootProperties are NOT re-seeded — seeding belongs to the
		// install, and the installer's values sync in.
		// Parts ARE declared when the root carries none yet (crash
		// between declaring and registering, a row adopted before the
		// root tree synced); a root with any declaration — live or
		// removed — is left alone. Properties heal per deterministic
		// id: a definition absent from the root (never written, not
		// removed) is written, nothing else. The name stamp is written
		// only when the root carries none: adopting never renames.
		if req.DerivedRoot && bd.Derived {
			rootId, err := b.deriveRoot(ctx, req)
			if err != nil {
				return space.Bundle{}, false, err
			}
			if err := b.declareType(ctx, rootId, req); err != nil {
				return space.Bundle{}, false, err
			}
			if err := b.stampRootName(ctx, rootId, req, true); err != nil {
				return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: stamp root %q: %w", rootId, err)
			}
		} else if len(req.Parts) > 0 || len(req.Properties) > 0 {
			// Created winner: heal the declaration when the root's
			// tree is local and carries none (crash between the
			// registering write and declaring). A winner whose tree
			// has not arrived is left alone — the installer's
			// declaration syncs with it.
			if _, present, perr := b.parent.store.TreeIsDerived(ctx, bd.RootId); perr == nil && present {
				if err := b.declareType(ctx, bd.RootId, req); err != nil {
					return space.Bundle{}, false, err
				}
			}
		}
		return bd, false, nil
	}

	rootId, err := b.mintRoot(ctx, req)
	if err != nil {
		return space.Bundle{}, false, err
	}
	// Stamp the root with the bundle name so its tree always carries a
	// non-root change: any-sync's head-sync diff skips a tree still
	// sitting on its root change, so an unstamped root is invisible to
	// sync. A created loser root would then be unresolvable from
	// another device; a DERIVED root — which every device materializes
	// for itself — would sit outside that device's diff and never pull
	// the peer's content. Must succeed BEFORE the registering write.
	if err := b.stampRootName(ctx, rootId, req, false); err != nil {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: stamp root %q: %w", rootId, err)
	}

	arena := &anyenc.Arena{}
	var ops []crdt.Op
	if req.Name != "" {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleName}, Payload: arena.NewString(req.Name)})
	}
	ops = append(ops,
		crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldBundleRootId}, Payload: arena.NewString(rootId)},
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{spaceindex.FieldBundleRoots}, Payload: arena.NewString(rootId)},
	)
	dataVersion, err := b.parent.store.DataVersion(spaceindex.BundlesDataset)
	if err != nil {
		return space.Bundle{}, false, err
	}
	res, err := b.parent.localWriteRetry(ctx, obj, obj.Id(), crdt.Change{
		Dataset:     spaceindex.BundlesDataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{
			{Id: req.Id, Upsert: true, Ops: ops},
		},
	})
	if err != nil {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: write: %w", err)
	}
	if len(res.Rejections) > 0 {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: write rejected: %w", res.Rejections[0].Err)
	}

	// Declarations land AFTER the registering write: a failure between
	// the two leaves a registered row whose root carries no declaration
	// — exactly the state the adopt path heals on the next Ensure.
	// The inverse order would strand an ORPHAN root with no registry
	// reference: nothing could ever adopt or heal it, and the bundle id
	// would be wedged for good.
	if err := b.declareType(ctx, rootId, req); err != nil {
		return space.Bundle{}, false, err
	}

	// Return the applied state, not the input — an inbound install may
	// have landed between the read above and this write's apply.
	row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, req.Id)
	if !liveBundleRow(row) {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: record %q absent after write", req.Id)
	}
	return b.materialize(ctx, row), true, nil
}

// validateEnsureRequest is the structural gate on Ensure input, run
// before any lock or write so a bad request mints nothing.
func (b *bundlesAPI) validateEnsureRequest(req space.EnsureBundleRequest) error {
	if req.DerivedRoot {
		if req.NewRoot != nil {
			return fmt.Errorf("spaceimpl: %w: NewRoot and DerivedRoot are exclusive — a derived root is derived by Ensure", space.ErrBundleBadRequest)
		}
	} else {
		// NewRoot == nil is the SDK-minted created root: Ensure creates
		// a bare object and stamps it as its own type — the only shape
		// a space whose free object create is fenced (the tech space)
		// can use. The type declaration gives that root its purpose.
		if req.NewRoot == nil && !req.DeclaresType() {
			return fmt.Errorf("spaceimpl: %w: NewRoot or a type declaration (Parts / Properties / Layout / Weight / Hidden) required", space.ErrBundleBadRequest)
		}
		if len(req.RootTypes) > 0 || len(req.RootProperties) > 0 {
			return fmt.Errorf("spaceimpl: %w: RootTypes/RootProperties apply to DerivedRoot only — a created root gets its initial state from NewRoot", space.ErrBundleBadRequest)
		}
	}
	if err := b.validateBundleParts(req.Parts, req.SystemInstall); err != nil {
		return err
	}
	return validateBundleProperties(req.Properties)
}

// preflightType fails a type-declaring request before the permanent
// root is derived: a RootProperties key equal to the root's own id
// (its property ids are not known before the install, so nothing can
// seed them). Collection names cannot collide — a namespaced dataset
// lives under the root's own id and a shared one is the module's
// canonical collection — so there is no name ownership to settle.
func (b *bundlesAPI) preflightType(ctx context.Context, req space.EnsureBundleRequest) error {
	if !req.DeclaresType() {
		return nil
	}
	canonical, err := b.canonicalRootId(ctx, req.Id)
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: canonical root of %q: %w", req.Id, err)
	}
	if _, self := req.RootProperties[canonical]; self {
		return fmt.Errorf("spaceimpl: %w: RootProperties keyed by the root's own id — its property ids exist only once installed", space.ErrBundleBadRequest)
	}
	return nil
}

// validateBundleProperties rejects a property draft AddProperty would
// refuse, a draft without an XKey (the deterministic id needs one) and
// a duplicate XKey, up front.
func validateBundleProperties(drafts []space.PropertyDraft) error {
	seen := make(map[string]struct{}, len(drafts))
	for i := range drafts {
		p := &drafts[i]
		if p.XKey == "" {
			return fmt.Errorf("spaceimpl: %w: property[%d]: XKey required — the property id derives from it", space.ErrBundleBadRequest, i)
		}
		if _, dup := seen[p.XKey]; dup {
			return fmt.Errorf("spaceimpl: %w: property %q declared twice", space.ErrBundleBadRequest, p.XKey)
		}
		seen[p.XKey] = struct{}{}
		if err := validatePropertyDraft(p); err != nil {
			return fmt.Errorf("spaceimpl: %w: property %q: %w", space.ErrBundleBadRequest, p.XKey, err)
		}
		if _, err := propertyDefPayload(&anyenc.Arena{}, p); err != nil {
			return fmt.Errorf("spaceimpl: %w: property %q: %w", space.ErrBundleBadRequest, p.XKey, err)
		}
	}
	return nil
}

// bundlePropertyId is the deterministic id of a bundle-declared
// property: a function of the root id and the handle, so two devices
// installing while apart mint one definition per handle. Same encoding
// as a change-derived record id.
func bundlePropertyId(rootId, xKey string) string {
	return crdt.DeriveRecordId("bundle-property:" + rootId + ":" + xKey)
}

// declareType writes the type declaration on the root: the parts (one
// change, only on a root with no declaration at all — see
// declareParts) and the properties (one change, only the definitions
// whose deterministic id the root does not carry yet).
func (b *bundlesAPI) declareType(ctx context.Context, rootId string, req space.EnsureBundleRequest) error {
	if err := b.declareParts(ctx, rootId, req.Parts); err != nil {
		return err
	}
	return b.declareProperties(ctx, rootId, req.Properties)
}

// declareProperties writes the definitions absent from the root, each
// under its deterministic id. A definition the root already carries —
// live, or removed through Types().RemoveProperty (the tombstone keeps
// the id) — is left alone: nothing is patched or resurrected.
func (b *bundlesAPI) declareProperties(ctx context.Context, rootId string, drafts []space.PropertyDraft) error {
	if len(drafts) == 0 {
		return nil
	}
	arena := &anyenc.Arena{}
	var recs []crdt.RecordChange
	for i := range drafts {
		id := bundlePropertyId(rootId, drafts[i].XKey)
		exists, err := b.parent.types.propertyDefExists(ctx, rootId, id)
		if err != nil {
			return fmt.Errorf("spaceimpl: bundles: read property %q of root %q: %w", drafts[i].XKey, rootId, err)
		}
		if exists {
			continue
		}
		payload, err := propertyDefPayload(arena, &drafts[i])
		if err != nil {
			return fmt.Errorf("spaceimpl: %w: property %q: %w", space.ErrBundleBadRequest, drafts[i].XKey, err)
		}
		recs = append(recs, crdt.RecordChange{
			Id: id, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		})
	}
	if len(recs) == 0 {
		return nil
	}
	res, err := b.parent.types.writePropertyDefs(ctx, rootId, recs...)
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: declare properties on root %q: %w", rootId, err)
	}
	if len(res.Rejections) > 0 {
		return fmt.Errorf("spaceimpl: bundles: declare properties on root %q: %w", rootId, res.Rejections[0].Err)
	}
	return nil
}

// selfTyped reports whether objectId carries the type marker plus its
// own id — the bundle-root shape.
func (b *bundlesAPI) selfTyped(ctx context.Context, objectId string) (bool, error) {
	types, err := b.parent.store.ObjectTypes(ctx, objectId)
	if err != nil {
		return false, err
	}
	var marker, self bool
	for _, t := range types {
		marker = marker || t == typetype.MetaTypeMarker
		self = self || t == objectId
	}
	return marker && self, nil
}

// validateBundleParts rejects an invalid part or dataset draft, or a
// duplicate key, up front — the same validation declareParts applies,
// so nothing half-registers. Drafts are normalized in place (module
// default, a shared dataset's canonical key). A reserved module is
// refused unless the request is the consumer's own install.
func (b *bundlesAPI) validateBundleParts(drafts []space.PartDraft, systemInstall bool) error {
	seen := make(map[string]struct{}, len(drafts))
	keys := make(map[string]struct{})
	shared := make(map[string]struct{})
	for i := range drafts {
		p := &drafts[i]
		if err := typetype.ValidateKey("part", p.Key); err != nil {
			return fmt.Errorf("spaceimpl: %w: %w", space.ErrBundleBadRequest, err)
		}
		if _, dup := seen[p.Key]; dup {
			return fmt.Errorf("spaceimpl: %w: part %q declared twice", space.ErrBundleBadRequest, p.Key)
		}
		seen[p.Key] = struct{}{}
		for j := range p.Datasets {
			d := &p.Datasets[j]
			if _, err := normalizeDatasetDraft(b.parent.store.Modules(), "", d); err != nil {
				return fmt.Errorf("spaceimpl: %w: dataset %q: %w", space.ErrBundleBadRequest, d.Key, err)
			}
			if !systemInstall {
				if err := checkReservedModule(b.parent.store.Modules(), d); err != nil {
					return fmt.Errorf("spaceimpl: %w: %w", space.ErrBundleBadRequest, err)
				}
			}
			if _, err := draftToDecl(d); err != nil {
				return fmt.Errorf("spaceimpl: %w: dataset %q: %w", space.ErrBundleBadRequest, d.Key, err)
			}
			if _, dup := keys[d.Key]; dup {
				return fmt.Errorf("spaceimpl: %w: dataset %q declared twice", space.ErrBundleBadRequest, d.Key)
			}
			keys[d.Key] = struct{}{}
			if d.Shared {
				if _, dup := shared[d.Module]; dup {
					return fmt.Errorf("spaceimpl: %w: two shared %q datasets", space.ErrBundleBadRequest, d.Module)
				}
				shared[d.Module] = struct{}{}
			}
		}
	}
	return nil
}

// declareParts writes the drafts as parts (with their datasets) with
// typeId = rootId, all in one change, on a root that carries no
// declaration yet. A root with any declaration — live, invalid
// (repair goes through Types()) or removed (a tombstone keeps no
// key; a removal is never undone here) — is left alone: the install
// declared atomically, later evolution is Types().AddPart /
// AddDataset. The apply is synchronous: the catalog knows the datasets
// when this returns.
func (b *bundlesAPI) declareParts(ctx context.Context, rootId string, drafts []space.PartDraft) error {
	if len(drafts) == 0 {
		return nil
	}
	declared, err := b.parent.store.HasDatasetDefs(ctx, rootId)
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: read declarations of root %q: %w", rootId, err)
	}
	if declared {
		return nil
	}
	arena := &anyenc.Arena{}
	var recs []crdt.RecordChange
	for i := range drafts {
		for j := range drafts[i].Datasets {
			if _, err := normalizeDatasetDraft(b.parent.store.Modules(), rootId, &drafts[i].Datasets[j]); err != nil {
				return fmt.Errorf("spaceimpl: %w: dataset %q: %w", space.ErrBundleBadRequest, drafts[i].Datasets[j].Key, err)
			}
		}
		_, r, err := partRecords(arena, &drafts[i])
		if err != nil {
			return fmt.Errorf("spaceimpl: bundles: declare part %q on root %q: %w", drafts[i].Key, rootId, err)
		}
		recs = append(recs, r...)
	}
	if _, err := b.parent.types.writeDatasetDefs(ctx, rootId, recs...); err != nil {
		return fmt.Errorf("spaceimpl: bundles: declare parts on root %q: %w", rootId, err)
	}
	return nil
}

// mintRoot produces the root object of a fresh install.
//
// The created path takes whatever NewRoot made and fails closed on
// anything it cannot verify: the honest path (Objects().Create in this
// space) materializes storage synchronously, so an absent entry or a
// read error means the input is wrong, and a derived root would plant
// a Losers entry that nothing can ever delete.
//
// The derived path mints the canonical root itself — idempotent, so a
// device that already derived it (offline, or on a previous attempt)
// re-registers the same id rather than forking — and seeds
// RootProperties BEFORE the caller registers anything, so a failed
// seed leaves no install: the retry mints and seeds again instead of
// adopting a root whose values never landed.
func (b *bundlesAPI) mintRoot(ctx context.Context, req space.EnsureBundleRequest) (string, error) {
	if req.DerivedRoot {
		rootId, err := b.deriveRoot(ctx, req)
		if err != nil {
			return "", err
		}
		if err := b.seedRootProperties(ctx, rootId, req.RootProperties); err != nil {
			return "", err
		}
		return rootId, nil
	}

	var rootId string
	if req.NewRoot == nil {
		// SDK-minted created root (validate guaranteed Parts).
		id, err := b.parent.objects.Create(ctx, space.CreateObjectOpts{})
		if err != nil {
			return "", fmt.Errorf("spaceimpl: bundles: create root: %w", err)
		}
		rootId = id
	} else {
		id, err := req.NewRoot(ctx)
		if err != nil {
			return "", fmt.Errorf("spaceimpl: bundles: NewRoot: %w", err)
		}
		if id == "" {
			return "", fmt.Errorf("spaceimpl: %w: NewRoot returned an empty id", space.ErrBundleBadRequest)
		}
		derived, present, derr := b.parent.store.TreeIsDerived(ctx, id)
		if derr != nil {
			return "", fmt.Errorf("spaceimpl: bundles: verify root %q: %w", id, derr)
		}
		if !present {
			return "", fmt.Errorf("spaceimpl: %w: root %q has no local tree — create it with Objects().Create in this space", space.ErrBundleBadRequest, id)
		}
		if derived {
			return "", fmt.Errorf("spaceimpl: %w: root %q is a derived object — a created root is required, or ask for DerivedRoot", space.ErrBundleBadRequest, id)
		}
		rootId = id
	}
	// A type-declaring created root is its own type, like the derived
	// path: the marker plus its own id. $addToSet, so a NewRoot that
	// already attached them is untouched.
	if req.DeclaresType() {
		for _, t := range []string{typetype.MetaTypeMarker, rootId} {
			if _, err := b.parent.properties.AttachType(ctx, rootId, t); err != nil {
				return "", fmt.Errorf("spaceimpl: bundles: self-type root %q: %w", rootId, err)
			}
		}
	}
	return rootId, nil
}

// deriveRoot materializes the bundle's canonical derived root and
// attaches RootTypes. Idempotent: a device that already derived it —
// offline, on a previous attempt, or from a peer's synced tree —
// re-uses the same tree instead of forking.
func (b *bundlesAPI) deriveRoot(ctx context.Context, req space.EnsureBundleRequest) (string, error) {
	canonical, err := b.canonicalRootId(ctx, req.Id)
	if err != nil {
		return "", fmt.Errorf("spaceimpl: bundles: canonical root of %q: %w", req.Id, err)
	}
	selfType := ""
	if req.DeclaresType() {
		selfType = canonical
	}
	rootId, err := b.parent.objects.Derive(ctx, space.DeriveObjectOpts{
		Seed:  spaceindex.BundleRootSeed(req.Id),
		Types: derivedRootTypes(req, selfType),
	})
	if err != nil {
		return "", fmt.Errorf("spaceimpl: bundles: derive root of %q: %w", req.Id, err)
	}
	if rootId != canonical {
		// Unreachable unless derivation itself changed shape.
		// Registering a root other devices would not recognize as
		// canonical is worse than refusing to install.
		return "", fmt.Errorf("spaceimpl: bundles: derived root %q is not the canonical %q", rootId, canonical)
	}
	return rootId, nil
}

// derivedRootTypes is what a derived root implements: the requested
// types plus every type RootProperties writes into. A property write
// to a type the object does not implement is rejected, and the created
// path attaches the same union through Objects().Create. With a
// selfType (the root declares a type) the root is also a type object
// implementing itself: the type marker plus its own id come first.
func derivedRootTypes(req space.EnsureBundleRequest, selfType string) []string {
	if len(req.RootProperties) == 0 && selfType == "" {
		return req.RootTypes
	}
	seen := make(map[string]struct{}, len(req.RootTypes)+len(req.RootProperties)+2)
	out := make([]string, 0, len(req.RootTypes)+len(req.RootProperties)+2)
	if selfType != "" {
		seen[typetype.MetaTypeMarker] = struct{}{}
		seen[selfType] = struct{}{}
		out = append(out, typetype.MetaTypeMarker, selfType)
	}
	for _, t := range req.RootTypes {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	// Sorted: map order would make the attach change differ per call
	// for no reason.
	for _, t := range slices.Sorted(maps.Keys(req.RootProperties)) {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// seedRootProperties writes a derived root's initial property values,
// one call per type. A created root gets the same state atomically
// from Objects().Create; a derived object has no create-time hook, so
// they land as ordinary writes — identical on every installer, so
// concurrent seeds converge.
func (b *bundlesAPI) seedRootProperties(ctx context.Context, rootId string, props map[string]map[string]any) error {
	for _, typeId := range slices.Sorted(maps.Keys(props)) {
		kv := props[typeId]
		if len(kv) == 0 {
			continue
		}
		if _, err := b.parent.properties.Set(ctx, rootId, typeId, kv); err != nil {
			return fmt.Errorf("spaceimpl: bundles: seed root properties of type %q: %w", typeId, err)
		}
	}
	return nil
}

// canonicalRootId is the id the bundle's derived root has in this
// space: a pure function of (space, bundle id), computed without
// materializing anything, identical on every device and for every
// member.
func (b *bundlesAPI) canonicalRootId(ctx context.Context, bundleId string) (string, error) {
	if v, ok := b.derivedIds.Load(bundleId); ok {
		return v.(string), nil
	}
	id, err := b.parent.store.DeriveId(ctx, spaceobjects.DeriveOpts{
		ChangeType:    objectChangeType,
		ChangePayload: spaceindex.BundleRootSeed(bundleId),
	})
	if err != nil {
		return "", err
	}
	b.derivedIds.Store(bundleId, id)
	return id, nil
}

func (b *bundlesAPI) DerivedRootId(ctx context.Context, bundleId string) (string, error) {
	if bundleId == "" {
		return "", fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	return b.canonicalRootId(ctx, bundleId)
}

// view lifts a stored row into the public Bundle and reports whether
// it is a LIVE install. A row whose winner's tree is deleted reads as
// uninstalled everywhere (Get, List, Ensure's adopt gate) — otherwise
// the bundle id would be permanently wedged, since record deletes are
// rejected and rootId has no unset.
func (b *bundlesAPI) view(ctx context.Context, row *anyenc.Value) (space.Bundle, bool) {
	if !liveBundleRow(row) {
		return space.Bundle{}, false
	}
	bd := b.materialize(ctx, row)
	if bd.RootId == "" || b.winnerDeleted(ctx, &bd) {
		return bd, false
	}
	return bd, true
}

// materialize decodes a row and applies the two read-side rules: the
// derived-root verdict and the live conflict set.
func (b *bundlesAPI) materialize(ctx context.Context, row *anyenc.Value) space.Bundle {
	bd := decodeBundleRecord(row)
	b.preferDerivedRoot(ctx, &bd)
	b.fillLosers(ctx, &bd)
	return bd
}

// preferDerivedRoot applies the derived-root verdict: once the
// canonical derived root has been claimed, it IS the winner, whatever
// the rootId register converged to.
//
// The claim set is add-only, so every replica reaches this verdict
// from any causal prefix that contains the claim — no ordering, no
// race, no window where two devices disagree about a root they can
// both compute. Without it a concurrent created install could win the
// LWW register and strand the derived root as a loser, and a derived
// tree cannot be deleted — the one conflict the registry could never
// resolve.
func (b *bundlesAPI) preferDerivedRoot(ctx context.Context, bd *space.Bundle) {
	if bd.Id == "" || len(bd.Roots) == 0 {
		return
	}
	canonical, err := b.canonicalRootId(ctx, bd.Id)
	if err != nil {
		// Falling back to the register means this reader can disagree
		// with every other replica about the winner — and about
		// Derived, which drives how callers bind child objects. Rare
		// (a cancelled ctx, a closing store), but never silent.
		bundleLog.Warn("bundle winner falls back to the rootId register",
			zap.String("bundle", bd.Id), zap.String("spaceId", b.parent.id), zap.Error(err))
		return
	}
	if canonical == "" || !slices.Contains(bd.Roots, canonical) {
		return
	}
	bd.RootId = canonical
	bd.Derived = true
}

func (b *bundlesAPI) Get(ctx context.Context, bundleId string) (space.Bundle, error) {
	if bundleId == "" {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		return space.Bundle{}, err
	}
	// A deleted winner = uninstalled (matches Ensure's adopt gate) —
	// reporting the dead RootId as live would send callers deriving
	// children from a tombstoned parent.
	bd, live := b.view(ctx, obj.Controller().Get(ctx, spaceindex.BundlesDataset, bundleId))
	if !live {
		return space.Bundle{}, fmt.Errorf("spaceimpl: %w: %q (no live install)", space.ErrBundleUnknown, bundleId)
	}
	return bd, nil
}

func (b *bundlesAPI) List(ctx context.Context) ([]space.Bundle, error) {
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		if errors.Is(err, space.ErrBundleUnknown) {
			return nil, nil
		}
		return nil, err
	}
	rows := obj.Controller().Records(ctx, spaceindex.BundlesDataset)
	out := make([]space.Bundle, 0, len(rows))
	for _, v := range rows {
		bd, live := b.view(ctx, v)
		if !live {
			continue // uninstalled — same gate as Get
		}
		out = append(out, bd)
	}
	return out, nil
}

// winnerDeleted reports whether the record's winning root tree is
// deleted. Unknown status (read error) counts as live — a spurious
// "uninstalled" would trigger a duplicate install, which is worse than
// briefly adopting a dead winner.
func (b *bundlesAPI) winnerDeleted(ctx context.Context, bd *space.Bundle) bool {
	deleted, err := b.parent.store.TreeDeleted(ctx, bd.RootId)
	return err == nil && deleted
}

func (b *bundlesAPI) ResolveLoser(ctx context.Context, bundleId, loserRootId string) error {
	if bundleId == "" || loserRootId == "" {
		return fmt.Errorf("spaceimpl: %w: bundle id and loser root id required", space.ErrBundleBadRequest)
	}
	obj, err := b.readIndexObj(ctx)
	if err != nil {
		return err
	}
	row := obj.Controller().Get(ctx, spaceindex.BundlesDataset, bundleId)
	if !liveBundleRow(row) {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleUnknown, bundleId)
	}
	// Same winner the readers see: with a canonical derived root
	// claimed, the created root the register may still name is a
	// LOSER, and deleting it is exactly what the caller is here for.
	bd := decodeBundleRecord(row)
	b.preferDerivedRoot(ctx, &bd)
	if bd.RootId == "" || loserRootId == bd.RootId || !slices.Contains(bd.Roots, loserRootId) {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrBundleNotLoser, loserRootId)
	}
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil // already resolved
	}
	// Deletion needs the loser's local head entry (any-sync settings
	// delete has no delete-by-id) — absent means "not synced here yet",
	// a retryable state with its own sentinel, not a storage error.
	if _, present, perr := b.parent.store.TreeIsDerived(ctx, loserRootId); perr == nil && !present {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrLoserNotSynced, loserRootId)
	}
	err = b.parent.objects.Delete(ctx, loserRootId)
	if err == nil {
		return nil
	}
	// Another resolver may have raced us — re-check before surfacing.
	if deleted, derr := b.parent.store.TreeDeleted(ctx, loserRootId); derr == nil && deleted {
		return nil
	}
	// Entry raced away between the probe above and the delete — same
	// retryable state.
	if _, present, perr := b.parent.store.TreeIsDerived(ctx, loserRootId); perr == nil && !present {
		return fmt.Errorf("spaceimpl: %w: %q", space.ErrLoserNotSynced, loserRootId)
	}
	return fmt.Errorf("spaceimpl: bundles: delete loser %q: %w", loserRootId, err)
}

// stampRootName writes `any.name` on a freshly created bundle root,
// with the type's rendering and listing metadata (`type.layout` /
// `type.weight` / `type.hidden`) when the root declares a type.
// Load-bearing despite looking cosmetic: it guarantees the root tree
// carries a non-root change (see the Ensure call site).
func (b *bundlesAPI) stampRootName(ctx context.Context, rootId string, req space.EnsureBundleRequest, adopt bool) error {
	obj, err := b.parent.store.Get(ctx, rootId)
	if err != nil {
		return err
	}
	name := req.Name
	if name == "" {
		name = req.Id
	}
	// Already stamped: a derived root is materialized by every device
	// that installs it, and re-writing the same name on each would add
	// a change per device for nothing. The record's existence is the
	// property that matters — the tree already carries a non-root
	// change. On adopt any existing name is kept: a later Ensure with
	// a different (or no) Name must not rename the root.
	if row := obj.Controller().Get(ctx, properties.Dataset, rootId); row != nil {
		if cur := row.Get("any", "name"); cur != nil && (adopt || string(cur.GetStringBytes()) == name) {
			return nil
		}
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set("any.name", arena.NewString(name))
	// The type's own metadata rides the same change: what the request
	// declares, nothing implied — a root hosting only its bundle's
	// records asks for Hidden itself.
	if req.DeclaresType() {
		if req.Hidden {
			payload.Set(typetype.TypeId+"."+typetype.FieldHiddenProp, arena.NewTrue())
		}
		if req.Weight != 0 {
			payload.Set(typetype.TypeId+"."+typetype.FieldWeightProp, arena.NewNumberInt(req.Weight))
		}
		if len(req.Layout) > 0 {
			layout, err := encodeXFormat(arena, req.Layout)
			if err != nil {
				return fmt.Errorf("%w: Layout: %w", space.ErrBundleBadRequest, err)
			}
			payload.Set(typetype.TypeId+"."+typetype.FieldLayoutProp, layout)
		}
	}
	dataVersion, err := b.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return err
	}
	res, err := b.parent.localWriteRetry(ctx, obj, rootId, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     rootId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
	if err != nil {
		return err
	}
	if len(res.Rejections) > 0 {
		return fmt.Errorf("write rejected: %w", res.Rejections[0].Err)
	}
	return nil
}

// readIndexObj is the read-path loader: no derive, no tree creation.
// ONLY a genuinely missing spaceIndex tree (legacy space never seeded,
// joiner before first sync) reads as "no registry" / ErrBundleUnknown;
// every other failure (ctx cancellation, closed DB, poisoned ocache
// load) propagates as an infrastructure error — flattening those to
// "unknown" would make List report an empty registry and callers
// re-run setup, minting a duplicate root.
func (b *bundlesAPI) readIndexObj(ctx context.Context) (*object.Object, error) {
	objectId, err := b.parent.indexObjectId(ctx)
	if err != nil {
		return nil, err
	}
	obj, err := b.parent.store.Get(ctx, objectId)
	if err != nil {
		if errors.Is(err, treestorage.ErrUnknownTreeId) {
			return nil, fmt.Errorf("spaceimpl: bundles: %w: spaceIndex tree not present locally: %v", space.ErrBundleUnknown, err)
		}
		return nil, fmt.Errorf("spaceimpl: bundles: load spaceIndex object: %w", err)
	}
	return obj, nil
}

// fillLosers computes the live conflict set: every claimed root except
// the winner whose tree is not already deleted. A root with unknown
// deletion status (not present locally, or a read error) counts as
// live — ResolveLoser re-validates and tree deletion is idempotent.
func (b *bundlesAPI) fillLosers(ctx context.Context, bd *space.Bundle) {
	if bd.RootId == "" {
		return
	}
	for _, r := range bd.Roots {
		if r == bd.RootId {
			continue
		}
		if deleted, err := b.parent.store.TreeDeleted(ctx, r); err == nil && deleted {
			continue
		}
		bd.Losers = append(bd.Losers, r)
	}
}

// liveBundleRow reports whether v is a live bundles row — present and
// not a tombstone (the handler rejects deletes, but inbound changes
// from future writers are gated tolerantly, so keep the check).
func liveBundleRow(v *anyenc.Value) bool {
	return v != nil && v.Get(crdt.DeletedAtField) == nil
}

// decodeBundleRecord lifts a bundles row (as returned by
// Controller.Get / Records) into the public space.Bundle. Losers is
// left empty — fillLosers owns it.
func decodeBundleRecord(v *anyenc.Value) space.Bundle {
	if v == nil {
		return space.Bundle{}
	}
	bd := space.Bundle{
		Id:     v.GetString(crdt.IdField),
		Name:   v.GetString(spaceindex.FieldBundleName),
		RootId: v.GetString(spaceindex.FieldBundleRootId),
	}
	if arr := v.Get(spaceindex.FieldBundleRoots); arr != nil && arr.Type() == anyenc.TypeArray {
		items, _ := arr.Array()
		for _, it := range items {
			if it.Type() == anyenc.TypeString {
				bd.Roots = append(bd.Roots, string(it.GetStringBytes()))
			}
		}
	}
	return bd
}
