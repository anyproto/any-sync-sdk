package bootstrap

import (
	"context"
	"path/filepath"
	"sync"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/internal/components"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/debugstat"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/node/nodeclient"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/server"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/anyproto/any-sync/nodeconf/nodeconfstore"
	"github.com/anyproto/any-sync/util/syncqueues"
)

var logOnce sync.Once

// NewApp creates and starts a fully wired app.App with all components needed
// for the SDK.
func NewApp(ctx context.Context, cfg syncsdk.Config) (*app.App, error) {
	logOnce.Do(func() {
		logCfg := logger.Config{
			DefaultLevel:  "WARN",
			DisableStdErr: true,
		}
		if cfg.LogPath != "" {
			logCfg.AddOutputPaths = []string{filepath.Join(cfg.LogPath, "any-sync-sdk.log")}
		}
		logCfg.ApplyGlobal()
	})

	keys := accountdata.New(cfg.PeerKey, cfg.SigningKey)

	configAdapter := components.NewConfig(cfg)
	accountAdapter := components.NewAccount(keys)
	storageProvider := components.NewStorageProvider(cfg.StoragePath)
	peerManagerProvider := components.NewPeerManagerProvider()
	treeManager := components.NewTreeManager()
	coordSource := components.NewCoordinatorSource()

	spaceSyncHandler := components.NewSpaceSyncHandler()
	streamHandler := components.NewStreamHandler(spaceSyncHandler)

	a := new(app.App)
	a.Register(configAdapter).
		Register(accountAdapter).
		Register(debugstat.New()).
		Register(components.NewCredentialProvider()).
		Register(nodeconfstore.New()).
		Register(coordSource).
		Register(nodeconf.New()).
		Register(secureservice.New()).
		Register(yamux.New()).
		Register(quic.New()).
		Register(peerservice.New()).
		Register(server.New()).
		Register(streamHandler).
		Register(streampool.New()).
		Register(spaceSyncHandler).
		Register(pool.New()).
		Register(peerManagerProvider).
		Register(coordinatorclient.New()).
		Register(nodeclient.New()).
		Register(storageProvider).
		Register(treeManager).
		Register(syncqueues.New()).
		Register(commonspace.New())

	if err := a.Start(ctx); err != nil {
		return nil, err
	}
	return a, nil
}
