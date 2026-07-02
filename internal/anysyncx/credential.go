package anysyncx

import (
	"context"
	"fmt"

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
	receipt, err := c.coordClient.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{
		SpaceId:     header.Id,
		SpaceHeader: header.RawHeader,
	})
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}
