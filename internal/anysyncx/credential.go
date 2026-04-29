package anysyncx

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/credentialprovider"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
)

// credentialProvider obtains space receipts from the coordinator via
// SpaceSign. Required by any-sync when joining or pushing spaces.
type credentialProvider struct {
	coordClient coordinatorclient.CoordinatorClient
}

func newCredentialProvider() *credentialProvider { return &credentialProvider{} }

func (c *credentialProvider) Init(a *app.App) error {
	c.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	return nil
}

func (c *credentialProvider) Name() string { return credentialprovider.CName }

func (c *credentialProvider) GetCredential(ctx context.Context, header *spacesyncproto.RawSpaceHeaderWithId) ([]byte, error) {
	receipt, err := c.coordClient.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{
		SpaceId:     header.Id,
		SpaceHeader: header.RawHeader,
	})
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}
