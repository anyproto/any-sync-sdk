package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/types"
	"github.com/anyproto/any-sync-sdk/space"
)

// propertiesAPI implements space.PropertiesAPI for MVP — base-scope
// only. SetAccount, SetDevice, AttachType, DetachType return errors
// pending their dedicated subsystems (rewrite-object in tech space,
// device-local store, etc.).
type propertiesAPI struct {
	parent *spaceImpl
}

func newPropertiesAPI(parent *spaceImpl) *propertiesAPI { return &propertiesAPI{parent: parent} }

// Get returns the computed property record for objectId. MVP: returns
// the raw record from the object's `properties` dataset (whose record
// id is the objectId itself). Variant collapse is a no-op here
// because we only write base-scope.
//
// Returns nil with no error if the object has no property record
// yet (never written) or has been tombstoned. Pure any-store read —
// no any-sync tree build, no cold restore.
func (p *propertiesAPI) Get(ctx context.Context, objectId string, _ space.PropertyReadOpts) (*anyenc.Value, error) {
	coll, err := p.parent.store.SharedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("propertiesAPI: shared objects: %w", err)
	}
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("propertiesAPI: find %s: %w", objectId, err)
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return nil, nil
	}
	// Doc's buffer is reused; clone before returning so callers can
	// retain the value past this call (same contract as
	// Controller.Get).
	var cloned anyencutil.Value
	cloned.FillCopy(v)
	return cloned.Value, nil
}

// SetBase merges the patch into the object's own `properties` record.
// Patch keys are propIds; the resulting paths are
// `{typeId}.{propId}`. Multi-field $set in a single change.
func (p *propertiesAPI) SetBase(ctx context.Context, objectId, typeId string, patch map[string]any) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	if len(patch) == 0 {
		return space.ModifyResult{}, errors.New("propertiesAPI: empty patch")
	}

	arena := &anyenc.Arena{}
	multi := arena.NewObject()
	for propId, val := range patch {
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return space.ModifyResult{}, fmt.Errorf("propertiesAPI: convert %s.%s: %w", typeId, propId, err)
		}
		multi.Set(typeId+"."+propId, v)
	}

	// DataVersion: the latest shortId for this typeId. Empty when the
	// type has had no important changes (e.g. type with no
	// properties yet) — the gate skips empty DataVersion writes.
	dataVersion, err := dataVersionForType(ctx, p.parent.store.Registry(), typeId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// dataVersionForType encodes a single-type DataVersion using the
// registry's latest shortId. Empty registry / empty result yields
// the legacy hardcoded handler version, which the gate treats as
// "no schema constraint" and lets through.
//
// Used by the writer paths (Properties.SetBase, Objects.Create
// bootstrap) for the per-space `objects` dataset.
func dataVersionForType(ctx context.Context, reg *types.LiveRegistry, typeId string) (string, error) {
	if reg == nil || typeId == "" {
		return properties.HandlerVersion, nil
	}
	shortId, err := reg.LatestShortId(ctx, typeId)
	if err != nil {
		return "", fmt.Errorf("dataVersionForType: %w", err)
	}
	if shortId == "" {
		// No important changes on this type yet; nothing to gate on.
		return properties.HandlerVersion, nil
	}
	return types.EncodeDataVersion([]types.DataVersionPair{{TypeId: typeId, ShortId: shortId}}), nil
}

// SetAccount, SetDevice, AttachType, DetachType — deferred.

func (p *propertiesAPI) SetAccount(_ context.Context, _, _ string, _ map[string]any) (space.ModifyResult, error) {
	return space.ModifyResult{}, errors.New("propertiesAPI: SetAccount not implemented")
}

func (p *propertiesAPI) SetDevice(_ context.Context, _, _ string, _ map[string]any) error {
	return errors.New("propertiesAPI: SetDevice not implemented")
}

// AttachType adds typeId to the object's any.types list, declaring that
// the object implements the type. Idempotent ($addToSet — re-attaching
// is a no-op). This is the sanctioned way to let an object host a type's
// properties or datasets: the write-time membership checks
// (SystemPropertiesHandler.PreValidate for properties, Modify for
// datasets) require the type to be present here first.
func (p *propertiesAPI) AttachType(ctx context.Context, objectId, typeId string) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	arena := &anyenc.Arena{}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops: []crdt.Op{{
				Type:    crdt.OpAddToSet,
				Path:    []string{"any", "types"},
				Payload: arena.NewString(typeId),
			}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// DetachType removes typeId from the object's any.types ($pull). Values
// in that namespace and records in the type's datasets become orphan
// data, read-tolerant (docs/06-data-structure.md §"read tolerance").
func (p *propertiesAPI) DetachType(ctx context.Context, objectId, typeId string) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	arena := &anyenc.Arena{}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops: []crdt.Op{{
				Type:    crdt.OpPull,
				Path:    []string{"any", "types"},
				Payload: arena.NewString(typeId),
			}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}
