package spaceimpl

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// techSpace is the restricted handle over the tech space. It satisfies
// space.Space by explicit delegation to an inner spaceImpl bound to the
// tech Store, so every method is a deliberate decision: reads and
// bundle-dataset writes delegate, lifecycle surfaces refuse with
// space.ErrUnsupported. A new Space method fails to compile here until
// it is classified.
type techSpace struct {
	inner   *spaceImpl
	tsp     *techspace.Service
	parent  *Service
	objects techObjects
	types   techTypes
	bundles techBundles
}

var _ space.Space = (*techSpace)(nil)

func newTechSpace(app *anysyncx.App, tsp *techspace.Service, parent *Service) *techSpace {
	inner := newSpace(tsp.SpaceId(), app, tsp, tsp.Store(), parent)
	inner.techIndexId = tsp.IndexObjectId()
	t := &techSpace{inner: inner, tsp: tsp, parent: parent}
	t.objects = techObjects{objectService: inner.objects, t: t}
	t.types = techTypes{inner: inner.types, t: t}
	t.bundles = techBundles{inner.bundles}
	return t
}

func (t *techSpace) Id() string { return t.inner.id }

// Info is synthetic: the tech space has no registry row (it IS the
// registry), is owner-only and derived.
func (t *techSpace) Info() space.SpaceInfo {
	info := space.SpaceInfo{
		Id:        t.inner.id,
		Type:      techspace.TechSpaceType,
		SpaceType: techspace.TechSpaceType,
		Status:    space.StatusActive,
		OwnRole:   space.PermissionOwner,
		Derived:   true,
	}
	if keys := t.inner.app.AccountKeys(); keys != nil {
		info.Author = keys.SignKey.GetPublic().Account()
	}
	return info
}

func (t *techSpace) Objects() space.ObjectService    { return t.objects }
func (t *techSpace) Types() space.TypesAPI           { return t.types }
func (t *techSpace) Bundles() space.BundlesAPI       { return t.bundles }
func (t *techSpace) Properties() space.PropertiesAPI { return unsupportedProperties{} }
func (t *techSpace) ACL() space.ACL                  { return unsupportedACL{} }
func (t *techSpace) Members() space.MembersAPI       { return unsupportedMembers{} }
func (t *techSpace) Files() space.Files              { return unsupportedFiles{} }
func (t *techSpace) History() space.HistoryAPI       { return unsupportedHistory{} }
func (t *techSpace) Payloads() space.PayloadsView    { return unsupportedPayloads{} }
func (t *techSpace) PubSub() space.PubSubAPI         { return unsupportedPubSub{} }
func (t *techSpace) ReadState() space.ReadStateAPI   { return unsupportedReadState{} }
func (t *techSpace) Changes() space.ChangeIndexAPI   { return unsupportedChanges{} }

func (t *techSpace) SyncStatus() space.SyncStatusAPI { return t.inner.SyncStatus() }
func (t *techSpace) Debug() space.DebugAPI           { return t.inner.Debug() }

func (t *techSpace) Query(objectId, dataset string) space.Query {
	return t.inner.Query(objectId, dataset)
}
func (t *techSpace) QueryObjects() space.Query { return t.inner.QueryObjects() }
func (t *techSpace) Aggregate(objectId, dataset string, pipeline any) space.Agg {
	return t.inner.Aggregate(objectId, dataset, pipeline)
}
func (t *techSpace) AggregateObjects(pipeline any) space.Agg {
	return t.inner.AggregateObjects(pipeline)
}
func (t *techSpace) Datasets() []space.DatasetSchema { return t.inner.Datasets() }

// Generic writes reach bundle datasets only — the datasets a bundle
// root declared (catalog-owned) — and never the tech index object:
// its tree is the account's space list, and a write fence keyed on the
// dataset name alone would let a bundle dataset name target it. The
// type-system built-ins (objects, properties, shortIds, datasets) and
// the system datasets (spaces, profile, devices, …) stay
// typed-API-only on this handle; a dataset the store has never heard
// of passes through so the regular unknown-dataset error surfaces.
func (t *techSpace) checkWrite(objectId, dataset string) error {
	if objectId == t.inner.techIndexId {
		return fmt.Errorf("spaceimpl: the tech index object accepts no generic writes: %w", space.ErrUnsupported)
	}
	if _, owned := t.inner.store.DatasetOwner(dataset); owned {
		return nil
	}
	if _, err := t.inner.store.DataVersion(dataset); err == nil {
		return fmt.Errorf("spaceimpl: dataset %q is not a bundle dataset: %w", dataset, space.ErrUnsupported)
	}
	return nil // unknown everywhere: the write path reports it as such
}

func (t *techSpace) Modify(ctx context.Context, batch space.ModifyBatch) (space.ModifyResult, error) {
	if err := t.checkWrite(batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}
	return t.inner.Modify(ctx, batch)
}

func (t *techSpace) ModifyMany(ctx context.Context, batches []space.ModifyBatch) ([]space.ModifyResult, error) {
	for i := range batches {
		if err := t.checkWrite(batches[i].ObjectId, batches[i].Dataset); err != nil {
			return nil, err
		}
	}
	return t.inner.ModifyMany(ctx, batches)
}

func (t *techSpace) Delete(ctx context.Context, batch space.DeleteBatch) (space.ModifyResult, error) {
	if err := t.checkWrite(batch.ObjectId, batch.Dataset); err != nil {
		return space.ModifyResult{}, err
	}
	return t.inner.Delete(ctx, batch)
}

func (t *techSpace) Upsert(ctx context.Context, batch space.UpsertBatch) (space.UpsertResult, error) {
	if err := t.checkWrite(batch.ObjectId, batch.Dataset); err != nil {
		return space.UpsertResult{}, err
	}
	return t.inner.Upsert(ctx, batch)
}

func (t *techSpace) SetMetadata(context.Context, space.SetMetadataRequest) error {
	return errUnsupported("SetMetadata")
}

func (t *techSpace) SpaceIndexObjectId() string { return t.inner.techIndexId }

// WaitIndexSynced: the tech index object is the space list itself.
func (t *techSpace) WaitIndexSynced(ctx context.Context) error { return t.parent.WaitListSynced(ctx) }

func (t *techSpace) SyncHeads(ctx context.Context) error { return t.inner.SyncHeads(ctx) }
func (t *techSpace) TreeHeads(ctx context.Context) ([]space.TreeHeads, error) {
	return t.inner.TreeHeads(ctx)
}

// isBundleRoot reports whether objectId is a self-typed bundle root:
// its row lists both the type marker and its own id.
func (t *techSpace) isBundleRoot(ctx context.Context, objectId string) (bool, error) {
	types, err := t.inner.store.ObjectTypes(ctx, objectId)
	if err != nil {
		return false, err
	}
	var marker, self bool
	for _, ty := range types {
		marker = marker || ty == typetype.MetaTypeMarker
		self = self || ty == objectId
	}
	return marker && self, nil
}

// techObjects keeps Get (a local row read) and refuses free lifecycle:
// tech-space objects exist only as bundle roots. Delete is allowed for
// exactly those — a deleted created winner reads as uninstalled, which
// is how a bundle uninstalls; a derived root stays undeletable (the
// object layer refuses derived deletion).
type techObjects struct {
	*objectService
	t *techSpace
}

func (x techObjects) Delete(ctx context.Context, objectId string) error {
	ok, err := x.t.isBundleRoot(ctx, objectId)
	if err != nil {
		return fmt.Errorf("spaceimpl: tech Objects().Delete: %w", err)
	}
	if !ok {
		return errUnsupported("Objects().Delete (non-bundle-root)")
	}
	return x.objectService.Delete(ctx, objectId)
}

func (techObjects) Create(context.Context, space.CreateObjectOpts) (string, error) {
	return "", errUnsupported("Objects().Create")
}
func (techObjects) Derive(context.Context, space.DeriveObjectOpts) (string, error) {
	return "", errUnsupported("Objects().Derive")
}

// techTypes keeps reads, and the dataset-declaration methods for
// bundle roots only (a bundle root is a type; its datasets evolve
// through them); type lifecycle and property definitions refuse.
type techTypes struct {
	inner *typesAPI
	t     *techSpace
}

func (x techTypes) root(ctx context.Context, typeId string) error {
	ok, err := x.t.isBundleRoot(ctx, typeId)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("spaceimpl: type %q is not a bundle root: %w", typeId, space.ErrUnsupported)
	}
	return nil
}

func (x techTypes) List(ctx context.Context) ([]space.TypeInfo, error) { return x.inner.List(ctx) }
func (x techTypes) Get(ctx context.Context, typeId string) (space.TypeInfo, error) {
	return x.inner.Get(ctx, typeId)
}
func (x techTypes) Properties(ctx context.Context, typeId string) ([]space.PropertyDef, error) {
	return x.inner.Properties(ctx, typeId)
}
func (x techTypes) Datasets(ctx context.Context, typeId string) ([]space.DatasetDef, error) {
	return x.inner.Datasets(ctx, typeId)
}
func (x techTypes) AddDataset(ctx context.Context, typeId string, draft space.DatasetDraft) (string, error) {
	if err := x.root(ctx, typeId); err != nil {
		return "", err
	}
	return x.inner.AddDataset(ctx, typeId, draft)
}
func (x techTypes) AddDatasetField(ctx context.Context, typeId, defId string, draft space.DatasetFieldDraft) (string, error) {
	if err := x.root(ctx, typeId); err != nil {
		return "", err
	}
	return x.inner.AddDatasetField(ctx, typeId, defId, draft)
}
func (x techTypes) RemoveDataset(ctx context.Context, typeId, defId string) error {
	if err := x.root(ctx, typeId); err != nil {
		return err
	}
	return x.inner.RemoveDataset(ctx, typeId, defId)
}
func (x techTypes) RemoveDatasetField(ctx context.Context, typeId, fieldDefId string) error {
	if err := x.root(ctx, typeId); err != nil {
		return err
	}
	return x.inner.RemoveDatasetField(ctx, typeId, fieldDefId)
}
func (x techTypes) PatchDataset(ctx context.Context, typeId, defId string, patch space.DatasetDefPatch) error {
	if err := x.root(ctx, typeId); err != nil {
		return err
	}
	return x.inner.PatchDataset(ctx, typeId, defId, patch)
}
func (techTypes) Create(context.Context, space.TypeCreateParams) (string, error) {
	return "", errUnsupported("Types().Create")
}
func (techTypes) Delete(context.Context, string) error { return errUnsupported("Types().Delete") }
func (techTypes) AddProperty(context.Context, string, space.PropertyDraft) (string, error) {
	return "", errUnsupported("Types().AddProperty")
}
func (techTypes) RemoveProperty(context.Context, string, string) error {
	return errUnsupported("Types().RemoveProperty")
}
func (techTypes) PatchProperty(context.Context, string, string, space.PropertyPatch) error {
	return errUnsupported("Types().PatchProperty")
}

// techBundles: derived-only installs that declare datasets; no loser
// resolution (derived roots have none).
type techBundles struct{ *bundlesAPI }

func (x techBundles) Ensure(ctx context.Context, req space.EnsureBundleRequest) (space.Bundle, bool, error) {
	if err := validateTechEnsureRequest(req); err != nil {
		return space.Bundle{}, false, err
	}
	return x.bundlesAPI.Ensure(ctx, req)
}

func (x techBundles) ResolveLoser(ctx context.Context, bundleId, loserRootId string) error {
	// Load-bearing with created roots: two devices installing while
	// apart fork, and the losing root must be resolvable here like in
	// any space. The delete runs on the inner object service — the
	// public techObjects fence is about free lifecycle, not this.
	return x.bundlesAPI.ResolveLoser(ctx, bundleId, loserRootId)
}

// validateTechEnsureRequest adds the tech-space rules on top of the
// structural gate: Datasets required (the root is its own type — that
// declaration is the install), roots minted by Ensure only (free
// object create is fenced, so NewRoot has nothing legal to call), and
// no foreign types (a type from another space would stamp a
// DataVersion the tech space can never satisfy on a device that lacks
// that space). Both root strategies are allowed: DerivedRoot for
// bundles that must never fork or uninstall, the SDK-minted created
// root for ordinary app installs (deletable; concurrent offline
// installs fork and resolve like in any space).
func validateTechEnsureRequest(req space.EnsureBundleRequest) error {
	if req.NewRoot != nil {
		return fmt.Errorf("spaceimpl: %w: NewRoot is not available on the tech space — Ensure mints the root", space.ErrBundleBadRequest)
	}
	if len(req.Datasets) == 0 {
		return fmt.Errorf("spaceimpl: %w: tech-space bundles must declare Datasets", space.ErrBundleBadRequest)
	}
	if len(req.RootTypes) > 0 || len(req.RootProperties) > 0 {
		return fmt.Errorf("spaceimpl: %w: RootTypes/RootProperties are not available on the tech space — a tech bundle root is its own type", space.ErrBundleBadRequest)
	}
	return nil
}

var (
	_ space.ObjectService = techObjects{}
	_ space.TypesAPI      = techTypes{}
	_ space.BundlesAPI    = techBundles{}
)
