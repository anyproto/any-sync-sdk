package components

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/credentialprovider"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
)

// NewCredentialProvider returns a CredentialProvider that obtains space
// receipts from the coordinator via SpaceSign.
func NewCredentialProvider() credentialprovider.CredentialProvider {
	return &credProvider{}
}

type credProvider struct {
	coordClient coordinatorclient.CoordinatorClient
}

func (c *credProvider) Init(a *app.App) error {
	c.coordClient = a.MustComponent(coordinatorclient.CName).(coordinatorclient.CoordinatorClient)
	return nil
}

func (c *credProvider) Name() string {
	return credentialprovider.CName
}

func (c *credProvider) GetCredential(ctx context.Context, spaceHeader *spacesyncproto.RawSpaceHeaderWithId) ([]byte, error) {
	receipt, err := c.coordClient.SpaceSign(ctx, coordinatorclient.SpaceSignPayload{
		SpaceId:     spaceHeader.Id,
		SpaceHeader: spaceHeader.RawHeader,
	})
	if err != nil {
		return nil, err
	}
	return receipt.MarshalVT()
}
