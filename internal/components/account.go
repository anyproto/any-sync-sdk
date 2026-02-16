package components

import (
	"github.com/anyproto/any-sync/accountservice"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
)

type AccountAdapter struct {
	keys *accountdata.AccountKeys
}

func NewAccount(keys *accountdata.AccountKeys) *AccountAdapter {
	return &AccountAdapter{keys: keys}
}

func (a *AccountAdapter) Init(_ *app.App) error {
	return nil
}

func (a *AccountAdapter) Name() string {
	return accountservice.CName
}

func (a *AccountAdapter) Account() *accountdata.AccountKeys {
	return a.keys
}
