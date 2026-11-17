//go:build !unix

package anysyncsdk

// lockUnsharedFile has nothing to take off unix: any-store holds an open
// database file under an exclusive lock there, so no other process has it.
func lockUnsharedFile(string) (unlock func(), err error) {
	return func() {}, nil
}

// syncDirOf has nothing to sync off unix: the filesystem journals names.
func syncDirOf(string) error {
	return nil
}
