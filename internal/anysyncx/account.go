package anysyncx

import (
	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
)

// accountAdapter satisfies any-sync's accountservice.Service component.
// Holds the keypair already decoded from the auth.Provider's seeds.
type accountAdapter struct {
	keys *accountdata.AccountKeys
}

func newAccount(keys *accountdata.AccountKeys) *accountAdapter {
	return &accountAdapter{keys: keys}
}

func (a *accountAdapter) Init(_ *app.App) error { return nil }
func (a *accountAdapter) Name() string          { return accountservice.CName }

func (a *accountAdapter) Account() *accountdata.AccountKeys { return a.keys }
