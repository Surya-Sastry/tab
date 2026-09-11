package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	ExpenseCreatedV1     = "expense.created.v1"
	ExpenseUpdatedV1     = "expense.updated.v1"
	ExpenseVoidedV1      = "expense.voided.v1"
	SettlementRecordedV1 = "settlement.recorded.v1"
	SettlementReversedV1 = "settlement.reversed.v1"
)

var allowed = map[string]struct{}{
	ExpenseCreatedV1: {}, ExpenseUpdatedV1: {}, ExpenseVoidedV1: {},
	SettlementRecordedV1: {}, SettlementReversedV1: {},
}

type Envelope struct {
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	AggregateID      string          `json:"aggregateId"`
	AggregateVersion int64           `json:"aggregateVersion"`
	OccurredAt       time.Time       `json:"occurredAt"`
	CorrelationID    string          `json:"correlationId"`
	CausationID      *string         `json:"causationId"`
	Payload          json.RawMessage `json:"payload"`
}

func New(eventType, aggregateID, correlationID string, version int64, payload any) (Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal payload: %w", err)
	}
	envelope := Envelope{
		EventID: uuid.NewString(), EventType: eventType, AggregateID: aggregateID,
		AggregateVersion: version, OccurredAt: time.Now().UTC(),
		CorrelationID: correlationID, Payload: body,
	}
	return envelope, envelope.Validate()
}

func (e Envelope) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("invalid eventId")
	}
	if _, ok := allowed[e.EventType]; !ok {
		return fmt.Errorf("unsupported eventType %q", e.EventType)
	}
	if e.AggregateID == "" || e.AggregateVersion <= 0 || e.OccurredAt.IsZero() || e.CorrelationID == "" {
		return fmt.Errorf("incomplete event envelope")
	}
	if !json.Valid(e.Payload) {
		return fmt.Errorf("invalid payload")
	}
	return nil
}
