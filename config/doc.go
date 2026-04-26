// Package config holds the pure-data configuration types middleware
// passes to sdk.Open. Zero dependencies on other SDK packages — it is
// a leaf so every layer can import it without creating cycles.
//
// Expected sub-sections (groomed per-package):
//
//   - Network   — any-sync network config, bootstrap nodes
//   - Storage   — DB path(s), dbRouter policy (shared vs per-space),
//     any-store tuning
//   - Sync      — timeouts, retries, snapshot heuristic tuning
//   - Log       — optional logger injection
//
// Auth is NOT here — it is a behavior (AuthProvider from the auth
// package), not data, so it is passed separately to sdk.Open.
package config
