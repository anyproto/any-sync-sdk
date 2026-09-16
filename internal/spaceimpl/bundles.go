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
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
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

func (b *bundlesAPI) Ensure(ctx context.Context, req space.EnsureBundleRequest, opts ...space.EnsureOption) (space.Bundle, bool, error) {
	if req.Id == "" {
		return space.Bundle{}, false, fmt.Errorf("spaceimpl: %w: empty bundle id", space.ErrBundleBadRequest)
	}
	eo := space.ApplyEnsureOptions(opts...)
	if err := b.validateEnsureRequest(req, eo.SystemInstall); err != nil {
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
			// Membership before declarations: the declaring writes land
			// on a definition object, so the marker must be on the row
			// first.
			if err := b.stampRoot(ctx, rootId, req, true); err != nil {
				return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: stamp root %q: %w", rootId, err)
			}
			if err := b.declareType(ctx, rootId, req); err != nil {
				return space.Bundle{}, false, err
			}
		} else if req.Declares() {
			// Created winner: heal what the root lacks when its tree is
			// local — a collection or handle the request gained since
			// the install (the stamp adds / fills only what is absent),
			// then the declaration when the root carries none (crash
			// between the registering write and declaring). A winner
			// whose tree has not arrived is left alone — the
			// installer's writes sync with it.
			if _, present, perr := b.parent.store.TreeIsDerived(ctx, bd.RootId); perr == nil && present {
				if err := b.stampRoot(ctx, bd.RootId, req, true); err != nil {
					return space.Bundle{}, false, fmt.Errorf("spaceimpl: bundles: stamp root %q: %w", bd.RootId, err)
				}
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
	// The root's first content change: its types, the bundle name, the
	// type metadata and the seeded values, together. Load-bearing
	// beyond keeping the DAG short — any-sync's head-sync diff skips a
	// tree still sitting on its root change, so an unstamped root is
	// invisible to sync: a created loser root would be unresolvable
	// from another device, and a DERIVED root — which every device
	// materializes for itself — would sit outside that device's diff
	// and never pull the peer's content. Must succeed BEFORE the
	// registering write.
	if err := b.stampRoot(ctx, rootId, req, false); err != nil {
		// A created root Ensure minted itself is an orphan nothing
		// references and nothing can adopt; a derived root is
		// re-derivable and a NewRoot root is the caller's. Best effort.
		if !req.DerivedRoot && req.NewRoot == nil {
			if derr := b.parent.objects.Delete(ctx, rootId); derr != nil {
				bundleLog.Warn("orphaned bundle root after a failed stamp",
					zap.String("bundle", req.Id), zap.String("root", rootId), zap.Error(derr))
			}
		}
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
// systemInstall is the consumer's own install (space.SystemInstall),
// which may name a reserved module.
func (b *bundlesAPI) validateEnsureRequest(req space.EnsureBundleRequest, systemInstall bool) error {
	if req.DerivedRoot {
		if req.NewRoot != nil {
			return fmt.Errorf("spaceimpl: %w: NewRoot and DerivedRoot are exclusive — a derived root is derived by Ensure", space.ErrBundleBadRequest)
		}
	} else {
		// NewRoot == nil is the SDK-minted created root: Ensure creates
		// a bare object and stamps it as its own definition — the only
		// shape a space whose free object create is fenced (the tech
		// space) can use. The declaration gives that root its purpose.
		if req.NewRoot == nil && !req.Declares() {
			return fmt.Errorf("spaceimpl: %w: NewRoot or a declaration (Parts / Properties / XKey) required", space.ErrBundleBadRequest)
		}
		// A caller-minted root got its initial state from NewRoot;
		// only a root Ensure mints takes it from the request.
		if req.NewRoot != nil && (req.RootType != "" || len(req.RootCollections) > 0 || len(req.RootProperties) > 0) {
			return fmt.Errorf("spaceimpl: %w: RootType/RootCollections/RootProperties apply to a root Ensure mints (DerivedRoot, or a created root with a declaration) — a NewRoot root gets its initial state from NewRoot", space.ErrBundleBadRequest)
		}
	}
	if req.Collection {
		if len(req.Parts) > 0 {
			return fmt.Errorf("spaceimpl: %w: a collection declares no parts", space.ErrBundleBadRequest)
		}
		if len(req.Layout) > 0 {
			return fmt.Errorf("spaceimpl: %w: a collection has no layout", space.ErrBundleBadRequest)
		}
		if !req.DeclaresCollection() && req.Hidden {
			return fmt.Errorf("spaceimpl: %w: Hidden needs a declaration (Properties / XKey)", space.ErrBundleBadRequest)
		}
	} else if !req.DeclaresType() && (len(req.Layout) > 0 || req.Hidden) {
		// Rendering and listing metadata describe a definition; a root
		// with no declaration is not one.
		return fmt.Errorf("spaceimpl: %w: Layout/Hidden need a type declaration (Parts / Properties / XKey)", space.ErrBundleBadRequest)
	}
	if req.DerivedRoot && !req.Declares() && req.RootType == "" {
		// Every object has a type: a derived root that declares nothing
		// takes it from RootType.
		return fmt.Errorf("spaceimpl: %w: a derived root that declares nothing needs a RootType", space.ErrBundleBadRequest)
	}
	if req.Declares() && req.RootType != "" {
		// A definition object carries its marker in the type slot; it
		// has no type of its own.
		return fmt.Errorf("spaceimpl: %w: RootType and a declaration are exclusive — a definition root carries its marker in any.type", space.ErrBundleBadRequest)
	}
	if req.RootType == typetype.MetaTypeMarker || req.RootType == collectiontype.MetaMarker {
		return fmt.Errorf("spaceimpl: %w: RootType names a marker — declare instead", space.ErrBundleBadRequest)
	}
	for _, c := range req.RootCollections {
		if c == "" {
			return fmt.Errorf("spaceimpl: %w: RootCollections: empty id", space.ErrBundleBadRequest)
		}
		if c == typetype.MetaTypeMarker || c == collectiontype.MetaMarker || c == anytype.TypeId {
			return fmt.Errorf("spaceimpl: %w: RootCollections names %q", space.ErrBundleBadRequest, c)
		}
	}
	if len(req.Layout) > 0 {
		// Probed here so a bad layout fails before any root is minted,
		// like every other part of the declaration.
		if _, err := encodeXFormat(&anyenc.Arena{}, req.Layout); err != nil {
			return fmt.Errorf("spaceimpl: %w: Layout: %w", space.ErrBundleBadRequest, err)
		}
	}
	// Seeded values likewise: an SDK-minted created root is minted
	// before the stamp that carries them, so a value that cannot be
	// encoded must fail here, not leave an orphan per attempt. Whether
	// a seeded owner resolves on this device is the write's verdict.
	arena := &anyenc.Arena{}
	for ownerId, kv := range req.RootProperties {
		if ownerId == "" {
			return fmt.Errorf("spaceimpl: %w: RootProperties: empty owner id", space.ErrBundleBadRequest)
		}
		for propId, v := range kv {
			if propId == "" {
				return fmt.Errorf("spaceimpl: %w: RootProperties[%s]: empty property id", space.ErrBundleBadRequest, ownerId)
			}
			if _, err := goToAnyenc(arena, v); err != nil {
				return fmt.Errorf("spaceimpl: %w: RootProperties[%s.%s]: %w", space.ErrBundleBadRequest, ownerId, propId, err)
			}
		}
	}
	if err := b.validateBundleParts(req.Parts, systemInstall); err != nil {
		return err
	}
	return validateBundleProperties(req.Properties)
}

// preflightType fails a declaring request before the permanent root
// is derived: a RootProperties key or RootCollections entry equal to
// the root's own id. The root's own values are seeded through its
// declaration (Properties with deterministic ids), never by naming
// the root as an owner. Collection names cannot collide — a namespaced
// dataset lives under the root's own id and a shared one is the
// module's canonical collection — so there is no name ownership to
// settle.
func (b *bundlesAPI) preflightType(ctx context.Context, req space.EnsureBundleRequest) error {
	if !req.Declares() {
		return nil
	}
	canonical, err := b.canonicalRootId(ctx, req.Id)
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: canonical root of %q: %w", req.Id, err)
	}
	if _, self := req.RootProperties[canonical]; self {
		return fmt.Errorf("spaceimpl: %w: RootProperties keyed by the root's own id — a definition object implements itself; seed its values through Properties", space.ErrBundleBadRequest)
	}
	if slices.Contains(req.RootCollections, canonical) {
		return fmt.Errorf("spaceimpl: %w: RootCollections names the root's own id — a definition object implements itself", space.ErrBundleBadRequest)
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

// declareProperties writes the definitions the root lacks, each under
// its deterministic id. A definition is present — and left alone,
// nothing patched or resurrected — when the root carries its id (live,
// or removed through Types().RemoveProperty: the tombstone keeps the
// id) or a live definition with its handle under any id: one column
// per handle is the point, whichever install or AddProperty minted it.
//
// The root is loaded first so the presence check and the write see the
// same materialized state: a created winner adopted before its tree
// was ever loaded here would otherwise read as bare, and the write
// would land as a modify on the definitions the load brings in. A
// write the apply path still rejects for that reason (the definition
// arrived between the check and the write) is treated as present.
func (b *bundlesAPI) declareProperties(ctx context.Context, rootId string, drafts []space.PropertyDraft) error {
	if len(drafts) == 0 {
		return nil
	}
	obj, err := b.parent.store.Get(ctx, rootId)
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: load root %q: %w", rootId, err)
	}
	ctrl := obj.Controller()
	handles := map[string]struct{}{}
	for _, v := range ctrl.Records(ctx, typetype.DatasetPropertyDefs) {
		if v == nil || v.Get(crdt.DeletedAtField) != nil {
			continue
		}
		if xk := v.GetString(typetype.FieldXKey); xk != "" {
			handles[xk] = struct{}{}
		}
	}
	arena := &anyenc.Arena{}
	var recs []crdt.RecordChange
	for i := range drafts {
		id := bundlePropertyId(rootId, drafts[i].XKey)
		if _, live := handles[drafts[i].XKey]; live || ctrl.Get(ctx, typetype.DatasetPropertyDefs, id) != nil {
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
	dataVersion, err := b.parent.store.DataVersion(typetype.DatasetPropertyDefs)
	if err != nil {
		return err
	}
	res, err := b.parent.localWriteRetry(ctx, obj, rootId, crdt.Change{
		Dataset:     typetype.DatasetPropertyDefs,
		DataVersion: dataVersion,
		Records:     recs,
	})
	if err != nil {
		return fmt.Errorf("spaceimpl: bundles: declare properties on root %q: %w", rootId, err)
	}
	for _, rej := range res.Rejections {
		// The record exists after all (it landed between the check and
		// the write): the definition is present, which is the goal.
		if rej.RecordIndex >= 0 && rej.RecordIndex < len(recs) &&
			ctrl.Get(ctx, typetype.DatasetPropertyDefs, recs[rej.RecordIndex].Id) != nil {
			continue
		}
		return fmt.Errorf("spaceimpl: bundles: declare properties on root %q: %w", rootId, rej.Err)
	}
	return nil
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

// mintRoot produces the BARE root object of a fresh install — its
// tree and nothing else; stampRoot writes the first content change.
//
// The created path takes whatever NewRoot made and fails closed on
// anything it cannot verify: the honest path (Objects().Create in this
// space) materializes storage synchronously, so an absent entry or a
// read error means the input is wrong, and a derived root would plant
// a Losers entry that nothing can ever delete.
//
// The derived path mints the canonical root itself — idempotent, so a
// device that already derived it (offline, or on a previous attempt)
// re-registers the same id rather than forking.
func (b *bundlesAPI) mintRoot(ctx context.Context, req space.EnsureBundleRequest) (string, error) {
	if req.DerivedRoot {
		return b.deriveRoot(ctx, req)
	}

	var rootId string
	if req.NewRoot == nil {
		// SDK-minted created root (validate guaranteed a declaration):
		// the tree only — types, name, metadata and seeded values all
		// ride the stamp, so the root is root + one change.
		obj, err := b.parent.store.Create(ctx, spaceobjects.CreateOpts{ChangeType: objectChangeType})
		if err != nil {
			return "", fmt.Errorf("spaceimpl: bundles: create root: %w", err)
		}
		rootId = obj.Id()
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
	return rootId, nil
}

// deriveRoot materializes the bundle's canonical derived root — the
// tree only; stampRoot attaches its types. Idempotent: a device that
// already derived it — offline, on a previous attempt, or from a
// peer's synced tree — re-uses the same tree instead of forking.
func (b *bundlesAPI) deriveRoot(ctx context.Context, req space.EnsureBundleRequest) (string, error) {
	canonical, err := b.canonicalRootId(ctx, req.Id)
	if err != nil {
		return "", fmt.Errorf("spaceimpl: bundles: canonical root of %q: %w", req.Id, err)
	}
	// The store-level derive: the tree only, no membership — the
	// stamp writes the marker or RootType in the root's first change,
	// the same shape the SDK-minted created root takes.
	obj, err := b.parent.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangeType:    objectChangeType,
		ChangePayload: spaceindex.BundleRootSeed(req.Id),
	})
	if err != nil {
		return "", fmt.Errorf("spaceimpl: bundles: derive root of %q: %w", req.Id, err)
	}
	rootId := obj.Id()
	if rootId != canonical {
		// Unreachable unless derivation itself changed shape.
		// Registering a root other devices would not recognize as
		// canonical is worse than refusing to install.
		return "", fmt.Errorf("spaceimpl: bundles: derived root %q is not the canonical %q", rootId, canonical)
	}
	return rootId, nil
}

// installRootMembers is what a root Ensure mints is: its type — the
// marker when the request declares a definition, RootType otherwise,
// empty for a bare root — and its collections: RootCollections plus
// every RootProperties owner that is neither the type nor the root
// itself (a property write to an owner the object does not have is
// rejected). `any` is universal and never listed.
func installRootMembers(req space.EnsureBundleRequest) spaceobjects.ObjectMembers {
	m := spaceobjects.ObjectMembers{}
	switch {
	case req.DeclaresType():
		m.Type = typetype.MetaTypeMarker
	case req.DeclaresCollection():
		m.Type = collectiontype.MetaMarker
	default:
		m.Type = req.RootType
	}
	seen := map[string]struct{}{anytype.TypeId: {}}
	if m.Type != "" {
		seen[m.Type] = struct{}{}
	}
	for _, c := range req.RootCollections {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		m.Collections = append(m.Collections, c)
	}
	// Sorted: map order would make the attach change differ per call
	// for no reason.
	for _, c := range slices.Sorted(maps.Keys(req.RootProperties)) {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		m.Collections = append(m.Collections, c)
	}
	return m
}

// metaNamespace is the objects-row namespace a declaring root's
// metadata lives under: `type` for a type, `collection` for a
// collection.
func metaNamespace(req space.EnsureBundleRequest) string {
	if req.DeclaresCollection() {
		return collectiontype.TypeId
	}
	return typetype.TypeId
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

// stampRoot writes a bundle root's first content change: its
// membership (the marker when it declares a definition, or RootType;
// RootCollections and every collection RootProperties writes into),
// `any.name`, the definition's metadata (`xkey` / `layout` / `hidden`
// under `type` or `collection`) and the seeded RootProperties values
// — ONE `objects` change, so a peer sees the row whole and the DAG
// carries no attach-then-name chatter. The local-write pre-flight
// grants every namespace the change itself sets, so the metadata and
// the seeded values validate in the same change. The seeds ride their
// own op: a value a peer cannot resolve drops that op alone, never
// the membership or the name.
//
// Load-bearing despite looking cosmetic: it guarantees the root tree
// carries a non-root change (see the Ensure call site).
//
// A row that already carries a name — a derived root materialized by
// every device that installs it, an adopted winner, a NewRoot the
// caller named — is not renamed and its metadata is not patched; what
// it LACKS is still written: a type when the row has none, a
// collection the request lists that the row does not carry
// ($addToSet, idempotent), and a handle when the row has none. Seeds
// belong to the install: an adopt never writes them, the installer's
// values sync in.
func (b *bundlesAPI) stampRoot(ctx context.Context, rootId string, req space.EnsureBundleRequest, adopt bool) error {
	obj, err := b.parent.store.Get(ctx, rootId)
	if err != nil {
		return err
	}
	name := req.Name
	if name == "" {
		name = req.Id
	}
	declares := req.Declares()
	ns := metaNamespace(req)
	want := installRootMembers(req)

	row := obj.Controller().Get(ctx, properties.Dataset, rootId)
	have := spaceobjects.ObjectMembers{}
	named := false
	if row != nil {
		have = spaceobjects.ObjectMembersOfRow(row)
		if cur := row.Get(anytype.TypeId, "name"); cur != nil {
			named = adopt || string(cur.GetStringBytes()) == name
		}
	}
	// A declaring root carries its marker whatever type the row has
	// (a definition has no type of its own); a RootType only fills an
	// empty slot — a type the row already has is never replaced.
	setType := ""
	switch {
	case declares && have.Type != want.Type:
		// A marker never flips: a type root stays a type root. A root
		// the caller minted keeps the type the caller gave it — a
		// declaring request over a NewRoot with a type is a conflict,
		// not a retype. A root Ensure minted (derived, or created for
		// an earlier version's RootType) takes the marker.
		if have.Type == typetype.MetaTypeMarker || have.Type == collectiontype.MetaMarker {
			return fmt.Errorf("spaceimpl: %w: root %q is a %s; a declaration never changes kind", space.ErrBundleBadRequest, rootId, have.Type)
		}
		if req.NewRoot != nil && have.Type != "" {
			return fmt.Errorf("spaceimpl: %w: NewRoot root %q already has type %q — a declaring root carries its marker in any.type; mint the root with Ensure instead", space.ErrBundleBadRequest, rootId, have.Type)
		}
		setType = want.Type
	case !declares && want.Type != "" && have.Type == "":
		setType = want.Type
	}
	var missing []string
	for _, c := range want.Collections {
		if !slices.Contains(have.Collections, c) {
			missing = append(missing, c)
		}
	}
	healXKey := named && declares && req.XKey != "" &&
		row.GetString(ns, typetype.FieldXKeyProp) == ""
	if named && setType == "" && len(missing) == 0 && !healXKey {
		return nil
	}

	arena := &anyenc.Arena{}
	var ops []crdt.Op
	// The type only when the row has none; $addToSet per collection,
	// never a whole-array $set: a NewRoot that already has members, or
	// a peer's copy of a derived root, must not be clobbered.
	if setType != "" {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{anytype.TypeId, anytype.FieldType}, Payload: arena.NewString(setType)})
	}
	for _, c := range missing {
		ops = append(ops, crdt.Op{Type: crdt.OpAddToSet, Path: []string{anytype.TypeId, anytype.FieldCollections}, Payload: arena.NewString(c)})
	}
	payload := arena.NewObject()
	payloadKeys := 0
	if !named {
		payload.Set(anytype.TypeId+".name", arena.NewString(name))
		payloadKeys++
	}
	// The definition's own metadata: what the request declares,
	// nothing implied — a root hosting only its bundle's records asks
	// for Hidden itself. On a named row only an absent handle is
	// filled.
	if declares && (!named || healXKey) && req.XKey != "" {
		payload.Set(ns+"."+typetype.FieldXKeyProp, arena.NewString(req.XKey))
		payloadKeys++
	}
	if declares && !named {
		if req.Hidden {
			payload.Set(ns+"."+typetype.FieldHiddenProp, arena.NewTrue())
			payloadKeys++
		}
		if len(req.Layout) > 0 {
			// Already probed by validateEnsureRequest.
			layout, err := encodeXFormat(arena, req.Layout)
			if err != nil {
				return fmt.Errorf("%w: Layout: %w", space.ErrBundleBadRequest, err)
			}
			payload.Set(ns+"."+typetype.FieldLayoutProp, layout)
			payloadKeys++
		}
	}
	if payloadKeys > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: payload})
	}
	// Seeded values, on install only, sorted so the change is the same
	// on every installer of a derived root. Their own op: independent
	// failure domain from the name and the membership.
	seeded := false
	if !adopt && !named && len(req.RootProperties) > 0 {
		seeds := arena.NewObject()
		seedKeys := 0
		for _, ownerId := range slices.Sorted(maps.Keys(req.RootProperties)) {
			kv := req.RootProperties[ownerId]
			for _, propId := range slices.Sorted(maps.Keys(kv)) {
				v, err := goToAnyenc(arena, kv[propId])
				if err != nil {
					return fmt.Errorf("%w: RootProperties[%s.%s]: %w", space.ErrBundleBadRequest, ownerId, propId, err)
				}
				seeds.Set(ownerId+"."+propId, v)
				seedKeys++
			}
		}
		if seedKeys > 0 {
			ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: seeds})
			seeded = true
		}
	}
	if len(ops) == 0 {
		return nil
	}

	// The DataVersion pins the owners' schema state only when seeded
	// values ride along, as a create with initial values would;
	// membership and metadata alone stamp no constraint, so a peer
	// behind on the definitions applies them rather than parking the
	// root typeless.
	dataVersion := properties.HandlerVersion
	if seeded {
		owners := make([]string, 0, 1+len(want.Collections))
		if want.Type != "" {
			owners = append(owners, want.Type)
		}
		owners = append(owners, want.Collections...)
		if dataVersion, err = dataVersionForOwners(ctx, b.parent.store.Registry(), owners); err != nil {
			return err
		}
	}
	res, err := b.parent.localWriteRetry(ctx, obj, rootId, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     rootId,
			Upsert: true,
			Ops:    ops,
		}},
	})
	if err != nil {
		return wrapSlotErr(err)
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
