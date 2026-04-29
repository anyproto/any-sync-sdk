// Package spaceimpl is the concrete implementation of space.Service
// and space.Space. The split is the same as the legacy SDK: the public
// space package owns interfaces and types, this internal package owns
// behavior — sdk.Open constructs a *spaceimpl.Service and returns it
// as space.Service.
//
// v1 scope: Create / Get / List / Delete on regular spaces, mediated
// by techspace for the index. Other Space sub-APIs (ACL, Members,
// Types, Properties, SyncStatus, Modify, Delete, Subscribe) are not
// wired yet and return zero values.
package spaceimpl
