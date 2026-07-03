package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

func TestValidateFormatDraft_NoFormat(t *testing.T) {
	draft := space.PropertyDraft{Kind: space.PropertyKindString}
	filterJSON, err := validateFormatDraft(&draft)
	require.NoError(t, err)
	assert.Empty(t, filterJSON)
	assert.Equal(t, space.PropertyKindString, draft.Kind)
}

func TestValidateFormatDraft_KindDefaulting(t *testing.T) {
	for _, tc := range []struct {
		format space.FormatType
		want   space.PropertyKind
	}{
		{space.FormatLinks, space.PropertyKindArray},
		{space.FormatDate, space.PropertyKindString},
		{space.FormatDatetime, space.PropertyKindString},
	} {
		draft := space.PropertyDraft{Format: &space.PropertyFormatDraft{Type: tc.format}}
		_, err := validateFormatDraft(&draft)
		require.NoError(t, err, tc.format)
		assert.Equal(t, tc.want, draft.Kind, "kind defaulted from format %s", tc.format)
	}
}

func TestValidateFormatDraft_KindMismatch(t *testing.T) {
	for _, tc := range []struct {
		format space.FormatType
		kind   space.PropertyKind
	}{
		{space.FormatLinks, space.PropertyKindString},
		{space.FormatDate, space.PropertyKindArray},
		{space.FormatDatetime, space.PropertyKindNumber},
	} {
		draft := space.PropertyDraft{Kind: tc.kind, Format: &space.PropertyFormatDraft{Type: tc.format}}
		_, err := validateFormatDraft(&draft)
		assert.Error(t, err, "format %s with kind %d must be rejected", tc.format, tc.kind)
	}
}

func TestValidateFormatDraft_LinksItemsMustBeString(t *testing.T) {
	draft := space.PropertyDraft{
		Kind:   space.PropertyKindArray,
		Items:  &space.PropertyDraft{Kind: space.PropertyKindNumber},
		Format: &space.PropertyFormatDraft{Type: space.FormatLinks},
	}
	_, err := validateFormatDraft(&draft)
	assert.Error(t, err)

	draft.Items.Kind = space.PropertyKindString
	_, err = validateFormatDraft(&draft)
	assert.NoError(t, err)
}

func TestValidateFormatDraft_TagsRejected(t *testing.T) {
	draft := space.PropertyDraft{Format: &space.PropertyFormatDraft{Type: space.FormatTags}}
	_, err := validateFormatDraft(&draft)
	assert.Error(t, err, "tags format is reserved until the tag table lands")
}

func TestValidateFormatDraft_UnknownType(t *testing.T) {
	for _, bad := range []space.FormatType{0, 99} {
		draft := space.PropertyDraft{Format: &space.PropertyFormatDraft{Type: bad}}
		_, err := validateFormatDraft(&draft)
		assert.Error(t, err, "format type %d must be rejected", bad)
	}
}

func TestValidateFormatDraft_FilterSerialization(t *testing.T) {
	// String passes through verbatim — no parsing, no validation.
	draft := space.PropertyDraft{Format: &space.PropertyFormatDraft{
		Type:   space.FormatLinks,
		Filter: `{"type":{"$in":["page"]}}`,
	}}
	filterJSON, err := validateFormatDraft(&draft)
	require.NoError(t, err)
	assert.Equal(t, `{"type":{"$in":["page"]}}`, filterJSON)

	// Maps are JSON-marshaled mechanically.
	draft = space.PropertyDraft{Format: &space.PropertyFormatDraft{
		Type:   space.FormatLinks,
		Filter: map[string]any{"type": "person"},
	}}
	filterJSON, err = validateFormatDraft(&draft)
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"person"}`, filterJSON)

	// Unmarshalable filters error.
	draft = space.PropertyDraft{Format: &space.PropertyFormatDraft{
		Type:   space.FormatLinks,
		Filter: make(chan int),
	}}
	_, err = validateFormatDraft(&draft)
	assert.Error(t, err)
}
