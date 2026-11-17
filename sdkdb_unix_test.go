//go:build unix

package anysyncsdk

import (
	"context"
	"os"
	"syscall"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"
)

// While another holder has sdk.db open the copy is not put in its place:
// the open goes on with the old file, and the move happens once the holder
// is gone.
func TestOpenSDKDB_KeepsFileOpenElsewhere(t *testing.T) {
	ctx := context.Background()
	path := oldFormatSDKDB(t)

	// What an open any-store database in another process holds on the file.
	holder, err := os.Open(path)
	require.NoError(t, err)
	defer holder.Close()
	require.NoError(t, syscall.Flock(int(holder.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))

	db, err := openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+sdkDBCopySuffix)

	require.NoError(t, holder.Close())
	db, err = openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
}
