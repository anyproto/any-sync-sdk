package anysyncx

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// SpaceDelete tells the coordinator to delete spaceId network-wide. It
// builds the signed deletion confirmation from the account sign key,
// peerId, and networkId — the coordinator verifies the signature and
// the embedded identity before moving the space to PendingDeletion.
// Only the space owner can delete; the coordinator rejects others.
//
// Idempotent from the caller's view: re-sending for a space already
// pending/deleted is the reconciler's normal retry, and the coordinator
// returns the appropriate error which the caller can treat as done.
func (a *App) SpaceDelete(ctx context.Context, spaceId string) error {
	conf, err := coordinatorproto.PrepareDeleteConfirmation(
		a.keys.SignKey, spaceId, a.keys.PeerId, a.NetworkId())
	if err != nil {
		return fmt.Errorf("anysyncx: prepare delete confirmation: %w", err)
	}
	if err := a.coord.SpaceDelete(ctx, spaceId, conf); err != nil {
		return fmt.Errorf("anysyncx: coordinator SpaceDelete %s: %w", spaceId, err)
	}
	return nil
}

// SpaceStatuses fetches the coordinator's view of the given spaces in
// one round trip — status (Created / PendingDeletion / Deleted / …) and
// our permissions (Owner / …) per space. The returned slice is aligned
// with spaceIds. Used by the deletion reconciler to decide whether we
// still owe a SpaceDelete (Owner + Created) or must offload a space the
// network reports gone.
func (a *App) SpaceStatuses(ctx context.Context, spaceIds []string) ([]*coordinatorproto.SpaceStatusPayload, error) {
	if len(spaceIds) == 0 {
		return nil, nil
	}
	statuses, _, err := a.coord.StatusCheckMany(ctx, spaceIds)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: StatusCheckMany: %w", err)
	}
	return statuses, nil
}
