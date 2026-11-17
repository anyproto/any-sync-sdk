//go:build unix

package anysyncsdk

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// lockUnsharedFile takes the exclusive lock on the closed database file at
// path that any-store's own open is refused by, and returns its release.
// It fails with errSDKDBShared while another process has the database
// open: an open any-store database holds the shared lock.
func lockUnsharedFile(path string) (unlock func(), err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errSDKDBShared
		}
		return nil, &os.PathError{Op: "flock", Path: path, Err: err}
	}
	return func() { _ = f.Close() }, nil
}

// syncDirOf makes the creation or removal of the file at path durable, as
// any-store syncs a directory: a plain fsync, and a directory that does not
// open is no error.
func syncDirOf(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil
	}
	defer dir.Close()
	for {
		err = syscall.Fsync(int(dir.Fd()))
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return &os.PathError{Op: "fsync", Path: dir.Name(), Err: err}
	}
	return nil
}
