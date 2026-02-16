package syncsdk

import (
	"context"

	"github.com/anyproto/any-sync-sdk/keys"
)

type SpaceCreateOption func(*spaceCreateOptions)
type ObjectCreateOption func(*objectCreateOptions)
type ObjectDeriveOption func(*objectDeriveOptions)

type spaceCreateOptions struct{}
type objectCreateOptions struct {
	changeType string
}
type objectDeriveOptions struct{}

func WithChangeType(t string) ObjectCreateOption {
	return func(o *objectCreateOptions) {
		o.changeType = t
	}
}

// ResolvedObjectCreateOptions holds the resolved values of ObjectCreateOption functions.
type ResolvedObjectCreateOptions struct {
	ChangeType string
}

// ResolveObjectCreateOptions applies the given ObjectCreateOption functions and returns the resolved values.
func ResolveObjectCreateOptions(opts []ObjectCreateOption) ResolvedObjectCreateOptions {
	var o objectCreateOptions
	for _, opt := range opts {
		opt(&o)
	}
	return ResolvedObjectCreateOptions{
		ChangeType: o.changeType,
	}
}

type Space interface {
	ID() string
	GetObject(ctx context.Context, objectID string) (Object, error)
	CreateObject(ctx context.Context, opts ...ObjectCreateOption) (Object, error)
	DeriveObject(ctx context.Context, opts ...ObjectDeriveOption) (Object, error)
	DeleteObject(ctx context.Context, objectID string) error
	ListObjectIDs(ctx context.Context) ([]string, error)
	KeyValue() KeyValue

	// GenerateInvite creates a new invite link for this space.
	GenerateInvite(ctx context.Context, opts ...InviteOption) (invite string, err error)
	// Members returns all members of this space with their permissions and status.
	Members(ctx context.Context) ([]Member, error)
	// AddMember adds an account directly to the space by identity.
	AddMember(ctx context.Context, identity keys.PublicKey, permissions Permission) error
	// RemoveMember removes a member from the space by identity.
	RemoveMember(ctx context.Context, identity keys.PublicKey) error
	// ChangePermissions updates the permission level of an existing member.
	ChangePermissions(ctx context.Context, identity keys.PublicKey, permissions Permission) error
	// AcceptJoinRequest approves a pending join request.
	AcceptJoinRequest(ctx context.Context, identity keys.PublicKey, permissions Permission) error
	// DeclineJoinRequest rejects a pending join request.
	DeclineJoinRequest(ctx context.Context, identity keys.PublicKey) error

	// Push registers the space with the coordinator and makes it available
	// on the network. This is called automatically by operations that need
	// network access, but can be called explicitly to ensure the space is
	// registered before generating invites or sharing.
	Push(ctx context.Context) error

	Subscribe(handler Handler) (unsubscribe func())
	Close(ctx context.Context) error
}
