package event

import (
	"testing"

	"github.com/google/uuid"
)

func TestEnvelopeValidation(t *testing.T) {
	envelope, err := New(ExpenseCreatedV1, uuid.NewString(), uuid.NewString(), 1, map[string]any{
		"amountMinor": int64(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	envelope.EventType = "expense.created"
	if err := envelope.Validate(); err == nil {
		t.Fatal("expected unsupported unversioned event type")
	}
}
