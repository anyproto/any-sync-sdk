package anysyncx

import (
	"context"
	"path/filepath"
	"testing"

	anystorev1 "github.com/anyproto/any-store"
	"github.com/anyproto/go-sqlite"
	"github.com/anyproto/go-sqlite/sqlitex"
	"github.com/stretchr/testify/require"
)

// Opening a space store must point sqlite's temp directory at a dir
// under the provider root. Android has no usable entry in sqlite's
// built-in temp-dir candidates, and without one any write tx that
// spills a savepoint sub-journal dies with SQLITE_IOERR — remote
// new-tree pulls never persist (SYN-82).
func TestStorageProviderSqliteTempDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "anysync")
	p := newStorageProvider(root)
	require.NoError(t, p.Init(nil))
	require.DirExists(t, p.tmpDir())

	db, err := anystorev1.Open(context.Background(), filepath.Join(root, "test.db"), p.anyStoreConfig())
	require.NoError(t, err)
	defer db.Close()

	// temp_store_directory sets the process-global sqlite3_temp_directory,
	// so any fresh connection reflects what the open above configured.
	conn, err := sqlite.OpenConn(":memory:")
	require.NoError(t, err)
	defer conn.Close()
	var got string
	require.NoError(t, sqlitex.ExecuteTransient(conn, "PRAGMA temp_store_directory", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			got = stmt.ColumnText(0)
			return nil
		},
	}))
	require.Equal(t, p.tmpDir(), got)
}
