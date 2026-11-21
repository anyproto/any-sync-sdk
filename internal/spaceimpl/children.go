package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/app/logger"
	anynet "github.com/anyproto/any-sync/net"
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

// networkErr wraps the error of a call that needs an answer from the
// network; one that reached no coordinator or node reads
// space.ErrOffline.
func networkErr(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, anynet.ErrUnableToConnect):
		return fmt.Errorf("spaceimpl: %s: %w: %w", op, space.ErrOffline, err)
	}
	return fmt.Errorf("spaceimpl: %s: %w", op, err)
}
