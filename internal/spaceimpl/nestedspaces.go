package spaceimpl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
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

// RemoveMemberAsLegalOwner removes identity from childSpaceId acting as its
// legalOwner (the parent's current owner) — no read key required (docs/16).
// Writes AclAccountRemoveNoRotate; the read-key rotation completing the
// cut-off is authored by a key-holding member (see the auto-rotation hook in
// the member watcher). When the child's stored legalOwner lags a parent
// ownership transfer, the required AclLegalOwnerUpdate is pushed lazily first.
func (s *Service) RemoveMemberAsLegalOwner(ctx context.Context, childSpaceId, identity string) error {
	target, err := crypto.DecodeAccountAddress(identity)
	if err != nil {
		return fmt.Errorf("spaceimpl: RemoveMemberAsLegalOwner: bad identity: %w", err)
	}
	handle, err := s.childHandleForGovernance(ctx, childSpaceId)
	if err != nil {
		return err
	}
	if err := s.ensureLegalOwnerCurrent(ctx, handle); err != nil {
		return err
	}
	acl := handle.Inner().Acl()
	acl.Lock()
	rec, err := acl.RecordBuilder().BuildAccountRemoveNoRotate(list.AccountRemoveNoRotatePayload{
		Identities: []crypto.PubKey{target},
	})
	acl.Unlock()
	if err != nil {
		return fmt.Errorf("spaceimpl: RemoveMemberAsLegalOwner: build: %w", err)
	}
	if err := addRecordWaitingForLog(ctx, handle.Inner().AclClient(), rec); err != nil {
		return fmt.Errorf("spaceimpl: RemoveMemberAsLegalOwner: publish: %w", err)
	}
	return nil
}

// DeleteChildAsLegalOwner deletes childSpaceId on the network acting as its
// legalOwner — no read access required; overrides deleteRestricted (docs/16).
// When the child is known locally the normal offline-first Delete flow runs
// (its reconciler retry is accepted by the coordinator's legalOwner path);
// otherwise the coordinator is told directly.
func (s *Service) DeleteChildAsLegalOwner(ctx context.Context, childSpaceId string) error {
	if _, ok := s.tsp.Get(ctx, childSpaceId); ok {
		return s.Delete(ctx, childSpaceId)
	}
	return s.app.SpaceDelete(ctx, childSpaceId)
}

// childHandleForGovernance opens a child space for a keyless legalOwner
// action: the space may be entirely unknown locally (the org never joined
// it), so it is tracked first and bootstrapped from the network.
func (s *Service) childHandleForGovernance(ctx context.Context, childSpaceId string) (anysyncx.SpaceHandle, error) {
	if _, ok := s.tsp.Get(ctx, childSpaceId); !ok {
		if err := s.Track(ctx, childSpaceId); err != nil {
			return nil, fmt.Errorf("spaceimpl: track child %s: %w", childSpaceId, err)
		}
	}
	handle, err := s.app.GetSpace(ctx, childSpaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: load child %s: %w", childSpaceId, err)
	}
	return handle, nil
}

// ensureLegalOwnerCurrent pushes the lazy AclLegalOwnerUpdate when the
// child's stored legalOwner key lags the caller (the parent's current owner):
// the proof chain is assembled from the parent acl's raw AclOwnershipChange
// records by signature induction (docs/16, grooming decision 14).
func (s *Service) ensureLegalOwnerCurrent(ctx context.Context, handle anysyncx.SpaceHandle) error {
	ourKey := s.app.AccountKeys().SignKey.GetPublic()
	acl := handle.Inner().Acl()
	acl.RLock()
	stored := acl.AclState().LegalOwner()
	parentSpaceId := acl.AclState().ParentSpaceId()
	acl.RUnlock()
	if stored == nil {
		return fmt.Errorf("spaceimpl: space %s is not a child space", handle.Inner().Id())
	}
	if stored.Equals(ourKey) {
		return nil
	}
	proofs, err := s.assembleLegalOwnerProofs(ctx, parentSpaceId, stored, ourKey)
	if err != nil {
		return err
	}
	acl.Lock()
	rec, err := acl.RecordBuilder().BuildLegalOwnerUpdate(list.LegalOwnerUpdatePayload{OwnershipChanges: proofs})
	acl.Unlock()
	if err != nil {
		return fmt.Errorf("spaceimpl: build legal owner update: %w", err)
	}
	if err := addRecordWaitingForLog(ctx, handle.Inner().AclClient(), rec); err != nil {
		return fmt.Errorf("spaceimpl: publish legal owner update: %w", err)
	}
	return nil
}

// assembleLegalOwnerProofs walks the parent acl's ownership-change records and
// returns the raw signed record bytes forming the induction chain from key
// `from` to key `to`.
func (s *Service) assembleLegalOwnerProofs(ctx context.Context, parentSpaceId string, from, to crypto.PubKey) ([][]byte, error) {
	parentHandle, err := s.app.GetSpace(ctx, parentSpaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: load parent %s: %w", parentSpaceId, err)
	}
	type hop struct {
		recordId string
		author   crypto.PubKey
		newOwner crypto.PubKey
	}
	var hops []hop
	parentAcl := parentHandle.Inner().Acl()
	parentAcl.RLock()
	for _, rec := range parentAcl.Records() {
		data, ok := rec.Model.(*aclrecordproto.AclData)
		if !ok {
			continue
		}
		for _, content := range data.GetAclContent() {
			oc := content.GetOwnershipChange()
			if oc == nil {
				continue
			}
			newOwner, err := crypto.UnmarshalEd25519PublicKeyProto(oc.NewOwnerIdentity)
			if err != nil {
				continue
			}
			hops = append(hops, hop{recordId: rec.Id, author: rec.Identity, newOwner: newOwner})
		}
	}
	parentAcl.RUnlock()

	aclStorage, err := parentHandle.Inner().Storage().AclStorage()
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: parent acl storage: %w", err)
	}
	var (
		proofs   [][]byte
		expected = from
	)
	for _, h := range hops {
		if !h.author.Equals(expected) {
			continue
		}
		storageRec, err := aclStorage.Get(ctx, h.recordId)
		if err != nil {
			return nil, fmt.Errorf("spaceimpl: read parent acl record %s: %w", h.recordId, err)
		}
		proofs = append(proofs, storageRec.RawRecord)
		expected = h.newOwner
		if expected.Equals(to) {
			break
		}
	}
	if !expected.Equals(to) {
		return nil, fmt.Errorf("spaceimpl: no ownership chain from the child's stored legal owner to this account in parent %s", parentSpaceId)
	}
	return proofs, nil
}
