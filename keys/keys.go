package keys

import "github.com/anyproto/any-sync/util/crypto"

type PrivateKey = crypto.PrivKey
type PublicKey = crypto.PubKey
type SymmetricKey = crypto.SymKey

func GenerateRandomKey() (PrivateKey, PublicKey, error) {
	privKey, pubKey, err := crypto.GenerateRandomEd25519KeyPair()
	return privKey, pubKey, err
}

func GenerateRandomSymKey() (SymmetricKey, error) {
	return crypto.NewRandomAES()
}
