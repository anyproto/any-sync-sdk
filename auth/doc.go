// Package auth is a pluggable provider of the two private keys any-sync
// needs: the account key (identity, signing, encryption) and the device
// key (per-installation peer identity). How the keys are obtained
// (mnemonic derivation, server-side auth, hardware token) is the
// provider's concern; the rest of the SDK only consumes the resulting
// Ed25519 keys.
//
// Ships a built-in mnemonic provider (BIP-39 / SLIP-10 at
// m/44'/2046'/[index]'). Key persistence is the caller's responsibility —
// the SDK does not store keys or mnemonics. See docs/01-auth-module.md.
package auth
