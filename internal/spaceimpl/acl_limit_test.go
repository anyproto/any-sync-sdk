package spaceimpl

import (
	"errors"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/stretchr/testify/assert"
	"storj.io/drpc/drpcerr"

	"github.com/anyproto/any-sync-sdk/space"
)

// A limit refusal reaches the caller as space.ErrCoordinatorLimitReached
// whether the chain holds the registered coordinator error (make-shareable)
// or only its wire code (an ACL record refused through the sync node).
func TestLiftLimitReached(t *testing.T) {
	assert.NoError(t, liftLimitReached(nil))

	registered := fmt.Errorf("acl: make shareable: %w", coordinatorproto.ErrSpaceLimitReached)
	assert.Same(t, registered, liftLimitReached(registered))
	assert.ErrorIs(t, liftLimitReached(registered), space.ErrCoordinatorLimitReached)

	wire := drpcerr.WithCode(errors.New("space limit reached"), rpcerr.Code(coordinatorproto.ErrSpaceLimitReached))
	assert.NotErrorIs(t, wire, space.ErrCoordinatorLimitReached)
	lifted := liftLimitReached(wire)
	assert.ErrorIs(t, lifted, space.ErrCoordinatorLimitReached)
	assert.ErrorIs(t, lifted, wire)

	forbidden := drpcerr.WithCode(errors.New("forbidden"), rpcerr.Code(coordinatorproto.ErrForbidden))
	assert.Same(t, forbidden, liftLimitReached(forbidden))
	assert.NotErrorIs(t, liftLimitReached(forbidden), space.ErrCoordinatorLimitReached)

	plain := errors.New("insufficient permissions")
	assert.Same(t, plain, liftLimitReached(plain))
}
