package anysyncx

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/credentialprovider"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
)

// credentialProvider obtains space receipts from the coordinator via
// SpaceSign. Required by any-sync when joining or pushing spaces.
// Local-only spaces are refused — a receipt exists only to push, and a
// local-only space must never be pushed (defense in depth; their inert
// peer manager already keeps the push path unreachable).
type credentialProvider struct {
	coordClient coordinatorclient.CoordinatorClient
	localOnly   *localOnlySpaces
	// childResolver resolves the AclChildRegister record id in the parent acl for a child
	// space header (nested spaces); set by the SDK layer, nil disables nested receipts
	childResolver atomic.Pointer[ChildCredentialResolver]
}

// ChildCredentialResolver returns the AclChildRegister record id for childSpaceId inside
// parentSpaceId's acl — the pointer SpaceSign needs to validate a nested space.
type ChildCredentialResolver func(ctx context.Context, parentSpaceId, childSpaceId string) (parentAclRecordId string, err error)

func (c *credentialProvider) setChildResolver(r ChildCredentialResolver) {
	c.childResolver.Store(&r)
}

func newCredentialProvider(localOnly *localOnlySpaces) *credentialProvider {
	return &credentialProvider{localOnly: localOnly}
}

func (c *credentialProvider) Init(a *app.App) error {
	c.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	return nil
}

func (c *credentialProvider) Name() string { return credentialprovider.CName }

func (c *credentialProvider) GetCredential(ctx context.Context, header *spacesyncproto.RawSpaceHeaderWithId) ([]byte, error) {
	if c.localOnly.has(header.Id) {
		return nil, fmt.Errorf("anysyncx: space %s is local-only; refusing coordinator receipt", header.Id)
	}
	payload := coordinatorclient.SpaceSignPayload{
		SpaceId:     header.Id,
		SpaceHeader: header.RawHeader,
	}
	parentSpaceId, err := headerParentSpaceId(header.RawHeader)
	if err != nil {
		return nil, err
	}
	if parentSpaceId != "" {
		resolver := c.childResolver.Load()
		if resolver == nil {
			return nil, fmt.Errorf("anysyncx: space %s is nested but no child credential resolver is set", header.Id)
		}
		recId, err := (*resolver)(ctx, parentSpaceId, header.Id)
		if err != nil {
			return nil, fmt.Errorf("anysyncx: resolve parent registration for %s: %w", header.Id, err)
		}
		payload.ParentAclRecordId = recId
	}
	receipt, err := c.coordClient.SpaceSign(ctx, payload)
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}

// headerParentSpaceId extracts the nested-spaces parent link from raw signed header bytes
func headerParentSpaceId(rawHeader []byte) (parentSpaceId string, err error) {
	var raw spacesyncproto.RawSpaceHeader
	if err = raw.UnmarshalVT(rawHeader); err != nil {
		return
	}
	var header spacesyncproto.SpaceHeader
	if err = header.UnmarshalVT(raw.SpaceHeader); err != nil {
		return
	}
	return header.ParentSpaceId, nil
}
