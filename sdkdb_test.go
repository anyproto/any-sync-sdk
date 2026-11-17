package anysyncsdk

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"
)

// oldFormatSDKDB puts a database in on-disk format 5 at <dir>/sdk.db and
// returns its path. testdata/format5.db.gz is a file any-store v2.4.3
// wrote: the collection "things" with five documents, each over the
// format's 997-byte leaf limit, and an index on "n".
func oldFormatSDKDB(t *testing.T) string {
	t.Helper()
	src, err := os.Open("testdata/format5.db.gz")
	require.NoError(t, err)
	defer src.Close()
	zr, err := gzip.NewReader(src)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "sdk.db")
	dst, err := os.Create(path)
	require.NoError(t, err)
	_, err = io.Copy(dst, zr)
	require.NoError(t, err)
	require.NoError(t, dst.Close())
	return path
}

// requireThings checks the fixture's content in db.
func requireThings(t *testing.T, db anystore.DB) {
	t.Helper()
	ctx := context.Background()
	coll, err := db.OpenCollection(ctx, "things")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	doc, err := coll.FindId(ctx, "doc3")
	require.NoError(t, err)
	require.Equal(t, 3, doc.Value().GetInt("n"))
	require.Greater(t, len(doc.Value().GetString("text")), 997)
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	require.Equal(t, []string{"n"}, indexes[0].Info().Fields)
}

func requireFormat(t *testing.T, db anystore.DB, want int) {
	t.Helper()
	format, err := anystore.SchemaFormat(db)
	require.NoError(t, err)
	require.Equal(t, want, format)
}

// dirNames lists the files beside sdk.db.
func dirNames(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// An sdk.db in an older on-disk format is replaced by a copy in the latest
// one at open; the copy leaves none of its own files behind.
func TestOpenSDKDB_ReplacesOldFormat(t *testing.T) {
	ctx := context.Background()
	path := oldFormatSDKDB(t)

	db, err := openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.ElementsMatch(t, []string{"sdk.db", "sdk.db-wal"}, dirNames(t, path))

	db, err = openSDKDB(ctx, path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
}

// A new sdk.db is created in the latest format.
func TestOpenSDKDB_NewFile(t *testing.T) {
	db, err := openSDKDB(context.Background(), filepath.Join(t.TempDir(), "sdk.db"))
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	require.NoError(t, db.Close())
}

// A copy an interrupted open left can be older than the database: it is
// removed, never put in place.
func TestOpenSDKDB_RemovesStaleCopy(t *testing.T) {
	path := oldFormatSDKDB(t)
	require.NoError(t, os.WriteFile(path+sdkDBCopySuffix, []byte("stale"), 0o600))

	db, err := openSDKDB(context.Background(), path)
	require.NoError(t, err)
	requireFormat(t, db, anystore.LatestSchemaFormat)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.NoFileExists(t, path+sdkDBCopySuffix)
}

// A copy that cannot be made leaves the file as it is, and the open goes
// on in the old format.
func TestOpenSDKDB_KeepsFileWhenCopyFails(t *testing.T) {
	path := oldFormatSDKDB(t)
	// A file beside the copy's name that is not a copy's refuses it.
	blocker := path + sdkDBCopySuffix + "-wal"
	require.NoError(t, os.WriteFile(blocker, []byte("not a log"), 0o600))

	db, err := openSDKDB(context.Background(), path)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	requireThings(t, db)
	require.NoError(t, db.Close())
	require.FileExists(t, blocker)
	require.NoFileExists(t, path+sdkDBCopySuffix)
}

// A cancelled open fails and leaves the file in its format.
func TestOpenSDKDB_Cancelled(t *testing.T) {
	path := oldFormatSDKDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := openSDKDB(ctx, path)
	require.ErrorIs(t, err, context.Canceled)

	db, err := anystore.Open(context.Background(), path, nil)
	require.NoError(t, err)
	requireFormat(t, db, 5)
	requireThings(t, db)
	require.NoError(t, db.Close())
}
