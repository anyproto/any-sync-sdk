// Package anysyncsdk is the top-level entrypoint: Open/Close, Config, and
// the public type aliases that middleware uses across packages.
//
// The SDK is consumed in-process by a middleware layer (see
// docs/common-context.md). There are exactly three public import paths:
//
//   - github.com/anyproto/any-sync-sdk        — Open, Close, Config
//   - github.com/anyproto/any-sync-sdk/auth   — AuthProvider + mnemonic helper
//   - github.com/anyproto/any-sync-sdk/space  — the whole caller surface:
//     Space, SpaceService, VersionId, Query, Subscription, ModifyBatch,
//     ACL, Members, TypesAPI, PropertiesAPI, SyncStatus
//
// Everything else lives under internal/ and is not importable from outside
// the module. See docs/ for the grooming notes behind this layout.
package anysyncsdk
