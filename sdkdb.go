package anysyncsdk

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"runtime"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"go.uber.org/zap"
)

// sdkDBCopySuffix names the copy of sdk.db in the latest on-disk format
// until it takes the file's place.
const sdkDBCopySuffix = ".migrate"

// errSDKDBShared reports that another process has sdk.db open.
var errSDKDBShared = errors.New("anysyncsdk: sdk.db is open in another process")

// sdkDBConfig is the configuration sdk.db is opened with, and the one a
// copy of it is written with. Open fills in defaults, so each open gets
// its own.
func sdkDBConfig() *anystore.Config {
	return &anystore.Config{
		UseGlobalPageBuffer: true,
		// One shared DB serves every space's reads; keep the engine's
		// CPU scaling but never drop below 8 concurrent readers.
		ReadConcurrency: max(runtime.NumCPU(), 8),
		// Without the idle flush, checkpoints only happen when the WAL
		// hits the commit-path threshold (10000 frames ≈ 40 MB) — a busy
		// DB sits under a huge WAL that must be replayed on every open.
		// Idle checkpointing keeps it bounded; the sentinel adds a
		// quick-check after unclean shutdowns.
		Durability: anystore.DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 20 * time.Second,
			FlushMode: anystore.FlushModeCheckpointPassive,
			Sentinel:  true,
		},
	}
}

// openSDKDB opens sdk.db. A file in an older on-disk format is replaced by
// a copy in the latest one before anyone else holds the handle; no older
// any-store release reads the result. When the copy cannot be made or put
// in place the old file stays: the open goes on with it and the next open
// tries again. A cancelled open fails, with the old file or the complete
// copy in place.
func openSDKDB(ctx context.Context, path string) (anystore.DB, error) {
	InitPageBuffer()
	db, err := anystore.Open(ctx, path, sdkDBConfig())
	if err != nil {
		return nil, err
	}
	format, err := anystore.SchemaFormat(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if format >= anystore.LatestSchemaFormat {
		return db, nil
	}
	log.Info("sdk.db: copying to the latest on-disk format",
		zap.Int("from", format), zap.Int("to", anystore.LatestSchemaFormat))
	copied, report, err := replaceWithLatestFormat(ctx, db, path)
	switch {
	case err == nil:
		log.Info("sdk.db: on-disk format updated",
			zap.Int("collections", report.Collections), zap.Int("documents", report.Documents),
			zap.Int("indexes", report.Indexes), zap.Duration("took", report.Duration))
		return copied, nil
	case ctx.Err() != nil:
		if copied != nil {
			_ = copied.Close()
		}
		return nil, err
	case copied != nil:
		log.Warn("sdk.db: not copied to the latest on-disk format, staying on the old one", zap.Error(err))
		return copied, nil
	default:
		// The source is closed: path holds the old database or the copy.
		log.Warn("sdk.db: the copy in the latest on-disk format was not put in place, opening what is there", zap.Error(err))
		return anystore.Open(ctx, path, sdkDBConfig())
	}
}

// replaceWithLatestFormat puts a copy of db in the latest on-disk format in
// the place of the file at path, and returns the database opened from it.
// db is the only handle on the file in this process and nothing writes
// through it. The steps and their order are any-store's
// (docs/migration-format-5-to-6.md): a crash at any of them leaves the old
// file with its log, or the copy.
//
// On an error before the source is closed it returns db, still open. After
// that it returns nil, and path is to be opened again.
func replaceWithLatestFormat(ctx context.Context, db anystore.DB, path string) (anystore.DB, anystore.CopyReport, error) {
	tmp := path + sdkDBCopySuffix
	// A copy an interrupted run left can be older than the database.
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return db, anystore.CopyReport{}, err
	}
	// ErrCopyPublished: the copy is complete and only a step after it
	// failed; the directory is synced again below.
	report, err := anystore.CopyToLatestFormat(ctx, db, tmp, sdkDBConfig())
	if err != nil && !errors.Is(err, anystore.ErrCopyPublished) {
		return db, anystore.CopyReport{}, err
	}
	// Close drops a checkpoint error, so checkpoint first.
	if err := db.Flush(ctx, 0, anystore.FlushModeCheckpointTruncate); err != nil {
		_ = os.Remove(tmp)
		return db, anystore.CopyReport{}, err
	}
	if err := db.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, anystore.CopyReport{}, err
	}
	// A process that still has the file open would go on writing to the old
	// file and its log, so the copy is not put in place under it. The lock
	// also keeps a new opener out until the copy is the file.
	unlock, err := lockUnsharedFile(path)
	if err != nil {
		_ = os.Remove(tmp)
		return nil, anystore.CopyReport{}, err
	}
	defer unlock()
	// The copy must never sit beside the old file's log.
	for _, sidecar := range []string{path + "-wal", path + "-wal-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(tmp)
			return nil, anystore.CopyReport{}, err
		}
	}
	if err := syncDirOf(path); err != nil {
		_ = os.Remove(tmp)
		return nil, anystore.CopyReport{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, anystore.CopyReport{}, err
	}
	if err := syncDirOf(path); err != nil {
		return nil, anystore.CopyReport{}, err
	}
	copied, err := anystore.Open(ctx, path, sdkDBConfig())
	if err != nil {
		return nil, anystore.CopyReport{}, err
	}
	return copied, report, nil
}
