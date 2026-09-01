package anysyncx

import (
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/util/crypto"
)

// p2pPeersFileName sits under DataDir next to the p2p port file: the
// persisted liveness records of known peers.
const (
	p2pPeersFileName      = "p2p_peers.json"
	accountRecordFileName = "account_record.json"
)

// globalSpaceKV adapts a loaded commonspace to the slice the global p2p
// layer needs: the default key-value store and two ACL answers.
type globalSpaceKV struct {
	cs commonspace.Space
}

func (s globalSpaceKV) Store() keyvaluestorage.Storage { return s.cs.KeyValue().DefaultStore() }

func (s globalSpaceKV) CanWrite() bool {
	acl := s.cs.Acl()
	acl.RLock()
	defer acl.RUnlock()
	st := acl.AclState()
	return st.Permissions(st.Identity()).CanWrite()
}

func (s globalSpaceKV) IsMember(identity string) bool {
	pk, err := crypto.DecodeAccountAddress(identity)
	if err != nil {
		return false
	}
	acl := s.cs.Acl()
	acl.RLock()
	defer acl.RUnlock()
	return !acl.AclState().Permissions(pk).NoPermissions()
}
