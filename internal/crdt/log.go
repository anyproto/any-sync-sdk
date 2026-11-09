package crdt

import (
	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"
)

var log = logger.NewNamed("sdk.crdt")

// warnSkippedRow reports a stored row the scan could not load.
func warnSkippedRow(objectId, dataset string, err error) {
	log.Warn("skipping a row that failed to load",
		zap.String("objectId", objectId),
		zap.String("dataset", dataset),
		zap.Error(err))
}
