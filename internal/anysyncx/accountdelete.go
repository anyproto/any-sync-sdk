package anysyncx

import (
	"context"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

// AccountDelete asks the coordinator to delete the account. It sends
// the confirmation signed by the account key; the coordinator moves
// every space the account owns to PendingDeletion for the network's
// deletion period. Returns the time at which the network deletes them.
func (a *App) AccountDelete(ctx context.Context) (time.Time, error) {
	conf, err := coordinatorproto.PrepareAccountDeleteConfirmation(
		a.keys.SignKey, a.keys.PeerId, a.NetworkId())
	if err != nil {
		return time.Time{}, fmt.Errorf("anysyncx: prepare account delete confirmation: %w", err)
	}
	ts, err := a.coord.AccountDelete(ctx, conf)
	if err != nil {
		return time.Time{}, fmt.Errorf("anysyncx: coordinator AccountDelete: %w", err)
	}
	return time.Unix(ts, 0).UTC(), nil
}

// AccountRevertDeletion cancels a pending account deletion: every
// PendingDeletion space of the account returns to Created.
func (a *App) AccountRevertDeletion(ctx context.Context) error {
	if err := a.coord.AccountRevertDeletion(ctx); err != nil {
		return fmt.Errorf("anysyncx: coordinator AccountRevertDeletion: %w", err)
	}
	return nil
}

// SpaceStatus fetches the coordinator's status of one space. For a
// space the coordinator has never registered it returns
// coordinatorproto.ErrSpaceNotExists.
func (a *App) SpaceStatus(ctx context.Context, spaceId string) (*coordinatorproto.SpaceStatusPayload, error) {
	st, err := a.coord.StatusCheck(ctx, spaceId)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: StatusCheck %s: %w", spaceId, err)
	}
	return st, nil
}
