package syncsdk

import "errors"

var (
	ErrSpaceNotFound  = errors.New("space not found")
	ErrObjectNotFound = errors.New("object not found")
	ErrSpaceClosed    = errors.New("space is closed")
	ErrClientClosed   = errors.New("client is closed")
	ErrInvalidConfig  = errors.New("invalid config")
)
