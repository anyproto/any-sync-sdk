package spaceimpl

import (
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/pubsub/pubsubproto"
	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/space"
)

func TestMapPubSubErr(t *testing.T) {
	passthrough := errors.New("passthrough")
	cases := []struct {
		in   error
		want error
	}{
		{nil, nil},
		{pubsubproto.ErrInvalidTopic, space.ErrPubSubInvalidTopic},
		{pubsubproto.ErrInvalidMessage, space.ErrPubSubPayloadTooLarge},
		{pubsubproto.ErrTopicNotOwned, space.ErrPubSubTopicNotOwned},
		{pubsubproto.ErrTooManyTopics, space.ErrPubSubTooManyPatterns},
		{anysyncx.ErrPubSubNoKey, space.ErrPubSubNoReadKey},
		{anysyncx.ErrPubSubGuestSpace, space.ErrPubSubNoReadKey},
		{passthrough, passthrough},
	}
	for _, c := range cases {
		got := mapPubSubErr(c.in)
		if c.want == nil {
			assert.NoError(t, got)
			continue
		}
		assert.ErrorIs(t, got, c.want, "in=%v", c.in)
		// The original error stays matchable through the wrap.
		assert.ErrorIs(t, got, c.in, "in=%v", c.in)
	}
}
