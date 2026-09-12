package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

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

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
