package anysyncx

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"
)

var p2pLog = logger.NewNamed("anysyncx.p2p")

// Port files sit directly under this package's DataDir (the SDK's
// <DataDir>/anysync/), next to the space databases: the last port a
// listener bound, reused on the next start. Plain decimal text — same
// low-ceremony persistence as the nodeconf store path.
const (
	portFileName     = "p2p_port"
	irohPortFileName = "iroh_port"
)

// portFilePath is "" without a DataDir, so nothing is read from or
// written to the working directory.
func portFilePath(dataDir, name string) string {
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, name)
}

// readPortFile returns the persisted port; zero when the file is
// missing or holds no valid port.
func readPortFile(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || port <= 0 || port > 65535 {
		return 0
	}
	return port
}

// savePortFile persists port unless the file already holds it. Failure
// only costs the reuse on the next start, so it is logged, not returned.
func savePortFile(path string, port int) {
	if path == "" || port == readPortFile(path) {
		return
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(port)), 0o600); err != nil {
		p2pLog.Warn("persist listen port", zap.String("file", filepath.Base(path)), zap.Error(err))
	}
}
