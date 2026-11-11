// Package auth is a pluggable provider of the two private keys any-sync
// needs: the account key (identity, signing, encryption) and the device
// key (per-installation peer identity). How the keys are obtained
// (mnemonic derivation, server-side auth, hardware token) is the
// provider's concern; the rest of the SDK only consumes the resulting
// Ed25519 keys.
//
// Ships providers built from a mnemonic (BIP-39 / SLIP-10 at
// m/44'/2046'/[index]'), from a wallet file, and from keys the host
// already holds. Key persistence is the caller's responsibility unless
// the wallet file holds them. See docs/auth-module.md.
package auth
