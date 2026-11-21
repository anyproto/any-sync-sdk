package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	anynet "github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

var childLog = logger.NewNamed("sdk.childspaces")

// CreateChild creates a child of req.ParentSpaceId: a space created
// like any other whose header names the parent. The coordinator checks
// the parent and the caller's role in it when the child is first
// pushed, so only what the index already shows is refused here.
func (s *Service) CreateChild(ctx context.Context, req space.CreateChildRequest) (space.Space, error) {
	parentId := req.ParentSpaceId
	if err := validateSpaceId(parentId); err != nil {
		return nil, err
	}
	if parentId == s.tsp.SpaceId() {
		return nil, fmt.Errorf("spaceimpl: CreateChild: the tech space: %w", space.ErrParentSpaceInvalid)
	}
	parent, err := s.acceptedRow(ctx, "CreateChild", parentId)
	if err != nil {
		return nil, err
	}
	if why := notAParent(parent); why != "" {
		return nil, fmt.Errorf("spaceimpl: CreateChild: parent %q is %s: %w", parentId, why, space.ErrParentSpaceInvalid)
	}
	return s.create(ctx, space.CreateRequest{
		Name:        req.Name,
		Description: req.Description,
		IconCID:     req.IconCID,
	}, parentId)
}

// notAParent says why the space of an index row cannot have children,
// "" when nothing on the row rules it out. A parent is a regular
// top-level space. A row with no type yet (joined, never loaded) is not
// judged.
func notAParent(rec techspace.SpaceIndexRecord) string {
	switch {
	case rec.ParentSpaceId != "":
		return "a child space"
	case rec.Derived:
		return "a derived space"
	case rec.Type != "" && rec.Type != space.SpaceTypeAny:
		return "of type " + rec.Type
	}
	return ""
}

// acceptedRow returns the index row of a space the account holds as an
// accepted space: not unknown, deleted or pending.
func (s *Service) acceptedRow(ctx context.Context, op, spaceId string) (techspace.SpaceIndexRecord, error) {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok {
		return rec, fmt.Errorf("spaceimpl: %s: %w %q", op, space.ErrSpaceUnknown, spaceId)
	}
	if rec.IsDeleted() {
		return rec, fmt.Errorf("spaceimpl: %s: space %q: %w", op, spaceId, space.ErrSpaceDeleted)
	}
	return rec, MaterializeBlock(rec)
}

// childSignAnswered keeps the row's refusal equal to the coordinator's
// last answer to a request of this device to sign the child spaceId.
// The write outlives ctx: the answer is given once, and a signed space
// is not asked about again.
func (s *Service) childSignAnswered(ctx context.Context, spaceId string, refusal space.Refusal) {
	s.setRefusal(context.WithoutCancel(ctx), spaceId, refusal)
}

// recordedRefusal reads the refusal the index holds for spaceId; ok is
// false when it has no row.
func (s *Service) recordedRefusal(spaceId string) (space.Refusal, bool) {
	rec, ok := s.tsp.Get(context.Background(), spaceId)
	return rec.Refusal, ok
}

// setRefusal writes refusal on spaceId's row when it differs.
func (s *Service) setRefusal(ctx context.Context, spaceId string, refusal space.Refusal) {
	rec, ok := s.tsp.Get(ctx, spaceId)
	if !ok || rec.Refusal == refusal {
		return
	}
	if _, err := s.tsp.SetRefusal(ctx, spaceId, refusal); err != nil {
		childLog.Warn("record refusal", zap.String("spaceId", spaceId), zap.Error(err))
	}
}

// ownsStored reports a space this device stores and the account owns,
// as far as the index tells: a role the ACL mirror has not written yet
// counts as the owner's.
func (s *Service) ownsStored(spaceId string) bool {
	rec, ok := s.tsp.Get(context.Background(), spaceId)
	if !ok || rec.IsDeleted() || !mayOwn(rec) {
		return false
	}
	return s.app.SpaceExists(spaceId)
}

// mayOwn reports a row whose space the account owns, or may own: the
// ACL mirror has not written a role yet.
func mayOwn(rec techspace.SpaceIndexRecord) bool {
	return rec.OwnRole == space.PermissionOwner || rec.OwnRole == space.PermissionNone
}

// RetryPush asks the coordinator to sign the child spaceId now — see
// space.Service.RetryPush. The answer reaches the row through
// childSignAnswered before the call returns.
func (s *Service) RetryPush(ctx context.Context, spaceId string) error {
	if err := validateSpaceId(spaceId); err != nil {
		return err
	}
	rec, err := s.acceptedRow(ctx, "RetryPush", spaceId)
	if err != nil {
		return err
	}
	if rec.ParentSpaceId == "" {
		return fmt.Errorf("spaceimpl: RetryPush: space %q: %w", spaceId, space.ErrNotChildSpace)
	}
	// Only a device that holds the space can push it, and loading a
	// space no node holds would wait for one.
	if !s.app.SpaceExists(spaceId) {
		return fmt.Errorf("spaceimpl: RetryPush: space %q: %w", spaceId, space.ErrSpaceNotStored)
	}
	return networkErr("RetryPush", s.app.SignSpace(ctx, spaceId))
}

// Children lists the children of parentSpaceId from the coordinator —
// see space.Service.Children.
func (s *Service) Children(ctx context.Context, parentSpaceId string) ([]space.ChildSpace, error) {
	if err := validateSpaceId(parentSpaceId); err != nil {
		return nil, err
	}
	children, err := s.app.Coordinator().SpaceChildren(ctx, parentSpaceId)
	if err != nil {
		return nil, networkErr("Children", err)
	}
	out := make([]space.ChildSpace, 0, len(children))
	for _, child := range children {
		out = append(out, space.ChildSpace{Id: child.SpaceId, Status: childStatus(child.Status)})
	}
	return out, nil
}

func childStatus(st coordinatorproto.SpaceStatus) space.ChildStatus {
	switch st {
	case coordinatorproto.SpaceStatus_SpaceStatusCreated:
		return space.ChildStatusActive
	case coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion,
		coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted:
		return space.ChildStatusDeletionPending
	case coordinatorproto.SpaceStatus_SpaceStatusDeleted:
		return space.ChildStatusDeleted
	}
	return space.ChildStatusUnknown
}

// childTarget checks that spaceId can be acted on as a child space from
// the parent side, as far as the index can tell, and returns its row
// when the account holds one. A row whose header was read (it has a
// type) and names no parent is a top-level space.
func (s *Service) childTarget(ctx context.Context, op, spaceId string) (rec techspace.SpaceIndexRecord, held bool, err error) {
	if err := validateSpaceId(spaceId); err != nil {
		return rec, false, err
	}
	notChild := fmt.Errorf("spaceimpl: %s: space %q: %w", op, spaceId, space.ErrNotChildSpace)
	if spaceId == s.tsp.SpaceId() {
		return rec, false, notChild
	}
	rec, held = s.tsp.Get(ctx, spaceId)
	if held && rec.Type != "" && rec.ParentSpaceId == "" {
		return rec, held, notChild
	}
	return rec, held, nil
}

// childAcl builds the ACL of a child space from the records the nodes
// serve, in memory, like the join controller's snapshot: the child
// needs no row and no storage here.
func (s *Service) childAcl(ctx context.Context, op, childSpaceId string) (list.AclList, error) {
	if _, _, err := s.childTarget(ctx, op, childSpaceId); err != nil {
		return nil, err
	}
	acl, err := s.app.AclSnapshot(ctx, childSpaceId)
	if err != nil {
		return nil, networkErr(op, err)
	}
	return acl, nil
}

// ChildRotationPending reads the pending rotation of a child space from
// the ACL the nodes serve — see space.Service.ChildRotationPending.
func (s *Service) ChildRotationPending(ctx context.Context, childSpaceId string) (bool, error) {
	acl, err := s.childAcl(ctx, "ChildRotationPending", childSpaceId)
	if err != nil {
		return false, err
	}
	acl.RLock()
	defer acl.RUnlock()
	return acl.AclState().RotationPending(), nil
}

// ChildMembers lists the accounts of a child space from the ACL the
// nodes serve — see space.Service.ChildMembers.
func (s *Service) ChildMembers(ctx context.Context, childSpaceId string) ([]space.ChildMember, error) {
	acl, err := s.childAcl(ctx, "ChildMembers", childSpaceId)
	if err != nil {
		return nil, err
	}
	acl.RLock()
	defer acl.RUnlock()
	accounts := acl.AclState().CurrentAccounts()
	out := make([]space.ChildMember, 0, len(accounts))
	for _, acc := range accounts {
		out = append(out, space.ChildMember{
			Identity:   acc.PubKey.Account(),
			Permission: fromAclPermissions(acc.Permissions),
			Status:     fromAclStatus(acc.Status),
		})
	}
	return out, nil
}

// RemoveChildMembers removes members of a child space on the account's
// role in the parent — see space.Service.RemoveChildMembers. Rides the
// joining client, like CancelJoin: the record is built over the ACL the
// nodes serve, so the child needs no row and no storage here.
func (s *Service) RemoveChildMembers(ctx context.Context, childSpaceId string, identities []string) error {
	if len(identities) == 0 {
		return errors.New("spaceimpl: RemoveChildMembers: empty identities")
	}
	pks := make([]crypto.PubKey, 0, len(identities))
	for i, id := range identities {
		pk, err := decodeIdentity(id)
		if err != nil {
			return fmt.Errorf("spaceimpl: RemoveChildMembers: identity %d: %w", i, err)
		}
		pks = append(pks, pk)
	}
	if _, _, err := s.childTarget(ctx, "RemoveChildMembers", childSpaceId); err != nil {
		return err
	}
	jc := s.app.JoiningClient()
	if jc == nil {
		return errors.New("spaceimpl: RemoveChildMembers: joining client unavailable")
	}
	_, err := jc.RemoveChildMembers(ctx, childSpaceId, pks)
	return networkErr("RemoveChildMembers", err)
}

// ChangeChildOwner hands a child space to one of its members on the
// account's role in the parent — see space.Service.ChangeChildOwner.
// Rides the joining client, like RemoveChildMembers.
func (s *Service) ChangeChildOwner(ctx context.Context, childSpaceId, newOwner string, oldOwnerPerm space.Permission) error {
	pk, err := decodeIdentity(newOwner)
	if err != nil {
		return fmt.Errorf("spaceimpl: ChangeChildOwner: %w", err)
	}
	if _, _, err := s.childTarget(ctx, "ChangeChildOwner", childSpaceId); err != nil {
		return err
	}
	jc := s.app.JoiningClient()
	if jc == nil {
		return errors.New("spaceimpl: ChangeChildOwner: joining client unavailable")
	}
	_, err = jc.ChangeChildOwner(ctx, childSpaceId, pk, toAclPermissions(oldOwnerPerm))
	return networkErr("ChangeChildOwner", err)
}

// DeleteChild deletes a child space on the network on the account's
// role in the parent — see space.Service.DeleteChild. The signed delete
// goes to the coordinator on the call path: only its answer tells
// whether the account may delete the child, so nothing local is
// touched before it. A deletion already under way is the answer asked
// for.
func (s *Service) DeleteChild(ctx context.Context, childSpaceId string) error {
	rec, held, err := s.childTarget(ctx, "DeleteChild", childSpaceId)
	if err != nil {
		return err
	}
	err = s.app.SpaceDelete(ctx, childSpaceId)
	if err != nil && !errors.Is(err, coordinatorproto.ErrSpaceDeletionPending) && !errors.Is(err, coordinatorproto.ErrSpaceIsDeleted) {
		return networkErr("DeleteChild", err)
	}
	// A deletion of the parent that waits for its children is sent
	// again now.
	s.kickDeletionReconciler()
	if !held || rec.IsDeleted() {
		return nil
	}
	// The account's own copy goes with the child. Cut short here, the
	// deletion reconciler does the same from the coordinator's status.
	ctx = context.WithoutCancel(ctx)
	if _, err := s.tsp.SetRemoteStatus(ctx, childSpaceId, techspace.StatusDeleted); err != nil {
		s.kickDeletionReconciler()
		return fmt.Errorf("spaceimpl: DeleteChild: mark deleted: %w", err)
	}
	s.OffloadSpace(ctx, childSpaceId)
	return nil
}

// networkErr wraps the error of a call that needs an answer from the
// network. One that reached no coordinator or node reads
// space.ErrOffline. The coordinator's answers for a space it does not
// know and for one in deletion or deleted read as the same sentinels a
// space of the index has: space.ErrSpaceUnknown and
// space.ErrSpaceDeleted.
func networkErr(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, anynet.ErrUnableToConnect):
		return fmt.Errorf("spaceimpl: %s: %w: %w", op, space.ErrOffline, err)
	case errors.Is(err, coordinatorproto.ErrSpaceNotExists):
		return fmt.Errorf("spaceimpl: %s: %w: %w", op, space.ErrSpaceUnknown, err)
	case errors.Is(err, coordinatorproto.ErrSpaceIsDeleted), errors.Is(err, coordinatorproto.ErrSpaceDeletionPending):
		return fmt.Errorf("spaceimpl: %s: %w: %w", op, space.ErrSpaceDeleted, err)
	}
	return fmt.Errorf("spaceimpl: %s: %w", op, err)
}
