package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

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
// yet (never written).
func (p *propertiesAPI) Get(ctx context.Context, objectId string, _ space.PropertyReadOpts) (*anyenc.Value, error) {
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return nil, err
	}
	v := obj.Controller().Get(ctx, properties.Dataset, objectId)
	return v, nil
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

func (p *propertiesAPI) AttachType(_ context.Context, _, _ string) (space.ModifyResult, error) {
	return space.ModifyResult{}, errors.New("propertiesAPI: AttachType not implemented")
}

func (p *propertiesAPI) DetachType(_ context.Context, _, _ string) (space.ModifyResult, error) {
	return space.ModifyResult{}, errors.New("propertiesAPI: DetachType not implemented")
}
