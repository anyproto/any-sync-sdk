package anysyncx

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/nodeconf"
)

// coordinatorSource satisfies nodeconf.Source. We always return
// ErrConfigurationNotChanged so nodeconf falls back to the locally
// persisted (or initial) configuration. The SDK doesn't poll the
// coordinator for nodeconf updates — clients ship a fresh NodeConfYAML
// on each app launch instead.
type coordinatorSource struct{}

func newCoordinatorSource() *coordinatorSource { return &coordinatorSource{} }

func (c *coordinatorSource) Init(_ *app.App) error { return nil }
func (c *coordinatorSource) Name() string          { return nodeconf.CNameSource }

func (c *coordinatorSource) GetLast(_ context.Context, _ string) (nodeconf.Configuration, error) {
	return nodeconf.Configuration{}, nodeconf.ErrConfigurationNotChanged
}
