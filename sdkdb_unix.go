//go:build unix

package anysyncsdk

import (
	"os"
	"path/filepath"
	"syscall"
)

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
