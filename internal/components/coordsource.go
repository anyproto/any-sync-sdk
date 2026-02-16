package components

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
)

// CoordinatorSource implements nodeconf.Source. On GetLast it returns
// ErrConfigurationNotChanged so the nodeconfstore/nodeconf init flow uses
// the locally persisted (or initial) configuration.
type CoordinatorSource struct{}

func NewCoordinatorSource() *CoordinatorSource {
	return &CoordinatorSource{}
}

func (c *CoordinatorSource) Init(_ *app.App) error {
	return nil
}

func (c *CoordinatorSource) Name() string {
	return nodeconf.CNameSource
}

func (c *CoordinatorSource) GetLast(_ context.Context, _ string) (nodeconf.Configuration, error) {
	return nodeconf.Configuration{}, nodeconf.ErrConfigurationNotChanged
}
