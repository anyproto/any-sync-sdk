package auth

import "context"

// Provider yields the two private keys any-sync needs to operate: the
// account key (identity, signing, encryption) and the device key
// (per-installation peer identity). How the keys are obtained
// (mnemonic derivation, server-side auth, hardware token) is the
// implementation's concern. The SDK does not persist keys — callers
// are responsible for secure storage.
//
// Both keys are Ed25519. The SDK treats the byte slices as opaque
// Ed25519 private-key seeds (the 32-byte secret form any-sync derives
// everything else from). Wrappers that carry more than a seed should
// return only the seed here.
type Provider interface {
	AccountKey(ctx context.Context) ([]byte, error)
	DeviceKey(ctx context.Context) ([]byte, error)
}
