package handler_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/properties"
)

// Each property-validation reason classifies to its public
// ValidationReason and still satisfies the umbrella + per-reason
// sentinels via errors.Is.
func TestClassifyValidation_PerReason(t *testing.T) {
	cases := []struct {
		reason   string
		want     handler.ValidationReason
		sentinel error
	}{
		{properties.ReasonInvalidPath, handler.ReasonInvalidPath, handler.ErrValidationInvalidPath},
		{properties.ReasonTypeNotImplemented, handler.ReasonTypeNotImplemented, handler.ErrValidationTypeNotImplemented},
		{properties.ReasonTypeUnknown, handler.ReasonTypeUnknown, handler.ErrValidationTypeUnknown},
		{properties.ReasonUnknownProperty, handler.ReasonUnknownProperty, handler.ErrValidationUnknownProperty},
		{properties.ReasonKindMismatch, handler.ReasonKindMismatch, handler.ErrValidationKindMismatch},
		{properties.ReasonReservedCarrier, handler.ReasonReservedCarrier, handler.ErrValidationReservedCarrier},
	}
	for _, tc := range cases {
		t.Run(string(tc.want), func(t *testing.T) {
			var err error = &properties.ValidationError{Reason: tc.reason}

			r, ok := handler.ClassifyValidation(err)
			assert.True(t, ok)
			assert.Equal(t, tc.want, r)

			// Umbrella + specific-sentinel classification still hold.
			assert.True(t, errors.Is(err, handler.ErrValidation), "umbrella errors.Is")
			assert.True(t, errors.Is(err, tc.sentinel), "per-reason errors.Is")

			// Works through a wrapping fmt.Errorf chain too.
			r2, ok2 := handler.ClassifyValidation(fmt.Errorf("write failed: %w", err))
			assert.True(t, ok2)
			assert.Equal(t, tc.want, r2)
		})
	}
}

func TestClassifyValidation_NonValidationError(t *testing.T) {
	r, ok := handler.ClassifyValidation(errors.New("boom"))
	assert.False(t, ok)
	assert.Equal(t, handler.ValidationReason(""), r)
}

func TestClassifyValidation_Nil(t *testing.T) {
	r, ok := handler.ClassifyValidation(nil)
	assert.False(t, ok)
	assert.Equal(t, handler.ValidationReason(""), r)
}

// A plain crdt.ErrValidation (the umbrella, no *ValidationError in the
// chain) is not classifiable — ClassifyValidation needs the structured
// rejection, not just the umbrella sentinel.
func TestClassifyValidation_UmbrellaOnlyNotClassifiable(t *testing.T) {
	r, ok := handler.ClassifyValidation(handler.ErrValidation)
	assert.False(t, ok)
	assert.Equal(t, handler.ValidationReason(""), r)
}
