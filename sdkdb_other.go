//go:build !unix

package anysyncsdk

// syncDirOf has nothing to sync off unix: the filesystem journals names.
func syncDirOf(string) error {
	return nil
}
