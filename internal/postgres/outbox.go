package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/event"
)

type OutboxItem struct {
	Envelope event.Envelope
	Attempts int
}

type OutboxStats struct {
	Pending   int64
	OldestAge time.Duration
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, staleAfter time.Duration) ([]OutboxItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		WITH claim AS (
			SELECT event_id FROM outbox_events
			WHERE (status='pending' AND available_at <= now())
			   OR (status='publishing' AND locked_at < now() - $1::interval)
			ORDER BY occurred_at
			FOR UPDATE SKIP LOCKED LIMIT $2
		)
		UPDATE outbox_events o
		SET status='publishing',locked_at=now(),attempts=attempts+1
		FROM claim WHERE o.event_id=claim.event_id
		RETURNING o.payload,o.attempts`, staleAfter.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []OutboxItem
	for rows.Next() {
		var body []byte
		var item OutboxItem
		if err := rows.Scan(&body, &item.Attempts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &item.Envelope); err != nil {
			return nil, fmt.Errorf("decode outbox event: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, tx.Commit(ctx)
}

func (s *Store) MarkPublished(ctx context.Context, eventID string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE outbox_events SET status='published',published_at=now(),locked_at=NULL,last_error=NULL
		WHERE event_id=$1 AND status='publishing'`, eventID)
	return err
}

func (s *Store) MarkPublishFailed(ctx context.Context, eventID string, attempts int, cause error) error {
	delay := time.Duration(math.Min(math.Pow(2, float64(attempts)), 300)) * time.Second
	status := "pending"
	if attempts >= 12 {
		status = "dead"
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE outbox_events
		SET status=$2,available_at=now()+$3::interval,locked_at=NULL,last_error=$4
		WHERE event_id=$1`, eventID, status, delay.String(), truncate(cause.Error(), 1000))
	return err
}

func (s *Store) OutboxStats(ctx context.Context) (OutboxStats, error) {
	var stats OutboxStats
	var seconds float64
	err := s.Pool.QueryRow(ctx, `
		SELECT count(*),COALESCE(EXTRACT(EPOCH FROM now()-min(occurred_at)),0)
		FROM outbox_events WHERE status IN ('pending','publishing')`).
		Scan(&stats.Pending, &seconds)
	stats.OldestAge = time.Duration(seconds * float64(time.Second))
	return stats, err
}
func (s *Store) ApplyBalanceEvent(ctx context.Context, consumerName string, envelope event.Envelope) (bool, error) {
	if err := envelope.Validate(); err != nil {
		return false, fmt.Errorf("%w: %v", domain.ErrInvalid, err)
	}
	var payload struct {
		GroupID       string               `json:"groupId"`
		LedgerEntries []domain.LedgerEntry `json:"ledgerEntries"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return false, fmt.Errorf("%w: decode balance payload: %v", domain.ErrInvalid, err)
	}
	if payload.GroupID != envelope.AggregateID {
		return false, fmt.Errorf("%w: balance payload group mismatch", domain.ErrInvalid)
	}
	// An expense whose participants all net to zero carries no ledger entries,
	// because ledger_entries rejects zero deltas. A sole group member paying for
	// themselves is the common case. That is a valid no-op, not a bad event, so
	// it still records a processed marker and commits without touching balances.
	var sum int64
	for _, entry := range payload.LedgerEntries {
		sum += entry.AmountMinor
	}
	if sum != 0 {
		return false, fmt.Errorf("%w: event ledger does not conserve", domain.ErrInvalid)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer_name,event_id) VALUES ($1,$2)
		ON CONFLICT DO NOTHING`, consumerName, envelope.EventID)
	if err != nil || tag.RowsAffected() == 0 {
		return false, err
	}
	for _, entry := range payload.LedgerEntries {
		_, err := tx.Exec(ctx, `
			INSERT INTO balance_projections
			    (group_id,user_id,balance_minor,aggregate_version)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT (group_id,user_id) DO UPDATE
			SET balance_minor=balance_projections.balance_minor+EXCLUDED.balance_minor,
			    aggregate_version=GREATEST(balance_projections.aggregate_version,EXCLUDED.aggregate_version),
			    updated_at=now()`,
			envelope.AggregateID, entry.UserID, entry.AmountMinor, envelope.AggregateVersion)
		if err != nil {
			return false, err
		}
	}
	return true, tx.Commit(ctx)
}

func (s *Store) ProjectionBalances(ctx context.Context, groupID, actorID string) ([]Balance, error) {
	if err := s.RequireMember(ctx, groupID, actorID); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT gm.user_id,COALESCE(bp.balance_minor,0)
		FROM group_members gm LEFT JOIN balance_projections bp
		  ON bp.group_id=gm.group_id AND bp.user_id=gm.user_id
		WHERE gm.group_id=$1 ORDER BY gm.user_id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Balance
	for rows.Next() {
		var balance Balance
		if err := rows.Scan(&balance.UserID, &balance.AmountMinor); err != nil {
			return nil, err
		}
		out = append(out, balance)
	}
	return out, rows.Err()
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
