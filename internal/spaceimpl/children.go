package spaceimpl

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

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
