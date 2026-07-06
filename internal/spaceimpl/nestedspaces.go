package spaceimpl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// CreateChild creates a space nested under req.ParentSpaceId (docs/16). The
// order matters: the registration record must land in the parent acl before
// the child's first push, because the coordinator only signs a nested space's
// receipt against an existing AclChildRegister. Local storage is created after
// the registration so a crash in between leaves only a harmless acl record
// (re-running CreateChild mints a fresh child id; the stale registration can
// be revoked).
func (s *Service) CreateChild(ctx context.Context, req space.CreateChildRequest) (space.Space, error) {
	if req.ParentSpaceId == "" {
		return nil, errors.New("spaceimpl: CreateChild: ParentSpaceId is required")
	}
	if req.OrgPermission == space.PermissionOwner {
		return nil, errors.New("spaceimpl: CreateChild: the parent cannot own the child")
	}
	keys := s.app.AccountKeys()
	if keys == nil {
		return nil, errors.New("spaceimpl: anysyncx app has no account keys")
	}
	spaceType, err := normalizeSpaceType(req.SpaceType)
	if err != nil {
		return nil, err
	}
	// the parent must be a known, active space on this device
	if _, err := s.Get(ctx, req.ParentSpaceId); err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: parent: %w", err)
	}
	parentHandle, err := s.app.GetSpace(ctx, req.ParentSpaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: load parent: %w", err)
	}
	parentAcl := parentHandle.Inner().Acl()
	parentAcl.RLock()
	legalOwner, err := parentAcl.AclState().OwnerPubKey()
	parentAcl.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: resolve parent owner: %w", err)
	}

	readKey, err := crypto.NewRandomAES()
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: random read key: %w", err)
	}
	metadataKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: metadata key: %w", err)
	}
	ownerMeta, err := encodeSelfSymKeyMetadata(keys.SignKey)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: derive metadata key: %w", err)
	}
	payload := spacepayloads.SpaceCreatePayload{
		SigningKey:  keys.SignKey,
		MasterKey:   keys.SignKey,
		ReadKey:     readKey,
		MetadataKey: metadataKey,
		Metadata:    ownerMeta,
		SpaceType:   spaceType,
		// The child inherits the PARENT's replication key: the whole org
		// co-locates on one node partition, so a client syncs an org over
		// one connection set (docs/16, grooming decision 18).
		ReplicationKey: replicationKeyFromSpaceId(req.ParentSpaceId),
		ParentSpaceId:  req.ParentSpaceId,
		LegalOwner:     legalOwner,
	}
	storagePayload, err := spacepayloads.StoragePayloadForSpaceCreateV1(payload)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: build payload: %w", err)
	}
	childId := storagePayload.SpaceHeaderWithId.Id

	// the parent must be registered + shareable before its acl accepts records
	if err := ensureShareableId(ctx, s.app, req.ParentSpaceId); err != nil {
		return nil, err
	}
	parentAcl.Lock()
	regRec, err := parentAcl.RecordBuilder().BuildChildRegister(list.ChildRegisterPayload{
		ChildSpaceId:   childId,
		ChildAclRootId: storagePayload.AclWithId.Id,
		OrgPermission:  toAclPermissions(req.OrgPermission),
	})
	parentAcl.Unlock()
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: build registration: %w", err)
	}
	if err := addRecordWaitingForLog(ctx, parentHandle.Inner().AclClient(), regRec); err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: register in parent acl: %w", err)
	}
	parentAcl.RLock()
	_, registered := parentAcl.AclState().ChildRegistration(childId)
	parentAcl.RUnlock()
	if !registered {
		return nil, errors.New("spaceimpl: CreateChild: registration not visible after publish")
	}

	if _, err := s.app.CreateSpaceFromStoragePayload(ctx, storagePayload); err != nil {
		return nil, fmt.Errorf("spaceimpl: CreateChild: create storage: %w", err)
	}

	if _, err := s.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:            childId,
		Type:          spaceType,
		SpaceType:     spaceType,
		Name:          req.Name,
		Description:   req.Description,
		IconCID:       req.IconCID,
		ParentSpaceId: req.ParentSpaceId,
		RemoteStatus:  techspace.StatusActive,
	}); err != nil {
		return nil, fmt.Errorf("spaceimpl: write index entry: %w", err)
	}
	if _, err := s.tsp.SetLocalStatus(ctx, childId, techspace.StatusActive); err != nil {
		return nil, fmt.Errorf("spaceimpl: set local status: %w", err)
	}
	if _, err := s.app.GetSpace(ctx, childId); err != nil {
		return nil, err
	}
	store := s.storeFor(childId)
	if _, err := s.ensureSpaceIndexWiring(ctx, childId); err != nil {
		return nil, err
	}
	if err := s.seedSpaceIndexOnCreate(ctx, store, childId, space.CreateRequest{
		Name:        req.Name,
		Description: req.Description,
		IconCID:     req.IconCID,
		SpaceType:   req.SpaceType,
	}, spaceType); err != nil {
		return nil, fmt.Errorf("spaceimpl: seed spaceIndex: %w", err)
	}
	return newSpace(childId, s.app, s.tsp, store, s), nil
}

// Children lists the child registrations in parentSpaceId's acl.
func (s *Service) Children(ctx context.Context, parentSpaceId string) ([]space.ChildRef, error) {
	handle, err := s.app.GetSpace(ctx, parentSpaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: Children: load parent: %w", err)
	}
	acl := handle.Inner().Acl()
	acl.RLock()
	defer acl.RUnlock()
	regs := acl.AclState().ChildRegistrations()
	refs := make([]space.ChildRef, 0, len(regs))
	for _, reg := range regs {
		ref := space.ChildRef{
			ChildSpaceId:   reg.ChildSpaceId,
			ChildAclRootId: reg.ChildAclRootId,
			RecordId:       reg.RecordId,
			OrgPermission:  fromAclPermissions(reg.OrgPermission),
			Revoked:        reg.Revoked,
		}
		if reg.Author != nil {
			ref.Author = reg.Author.Account()
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// resolveChildCredential is the nested-spaces hook for the anysyncx credential
// provider: SpaceSign for a child must carry the AclChildRegister record id,
// which lives in the parent acl — durable across restarts, so no extra state.
func (s *Service) resolveChildCredential(ctx context.Context, parentSpaceId, childSpaceId string) (string, error) {
	handle, err := s.app.GetSpace(ctx, parentSpaceId)
	if err != nil {
		return "", fmt.Errorf("spaceimpl: load parent %s: %w", parentSpaceId, err)
	}
	acl := handle.Inner().Acl()
	acl.RLock()
	defer acl.RUnlock()
	reg, ok := acl.AclState().ChildRegistration(childSpaceId)
	if !ok || reg.Revoked {
		return "", fmt.Errorf("spaceimpl: no active registration for child %s in parent %s", childSpaceId, parentSpaceId)
	}
	return reg.RecordId, nil
}

// ensureShareableId is ensureShareable for a space addressed by id only
// (the caller may not hold a spaceImpl, e.g. the parent in CreateChild).
func ensureShareableId(ctx context.Context, app *anysyncx.App, spaceId string) error {
	return ensureShareableCoord(ctx, app, spaceId)
}
