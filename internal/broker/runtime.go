package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Surya-Sastry/tab/internal/observability"
	"github.com/Surya-Sastry/tab/internal/postgres"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	Store   *postgres.Store
	Writer  *kafka.Writer
	Logger  *slog.Logger
	Metrics *observability.Metrics
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic,
		Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll,
		BatchTimeout: 50 * time.Millisecond, WriteTimeout: 10 * time.Second,
		AllowAutoTopicCreation: true,
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			p.Logger.Error("outbox batch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) error {
	items, err := p.Store.ClaimOutbox(ctx, 100, 30*time.Second)
	if err != nil {
		return err
	}
	if stats, statsErr := p.Store.OutboxStats(ctx); statsErr == nil && p.Metrics != nil {
		p.Metrics.OutboxPending.Set(float64(stats.Pending))
		p.Metrics.OutboxOldestAge.Set(stats.OldestAge.Seconds())
	}
	for _, item := range items {
		body, err := json.Marshal(item.Envelope)
		if err == nil {
			err = p.Writer.WriteMessages(ctx, kafka.Message{
				Key: []byte(item.Envelope.AggregateID), Value: body,
				Headers: []kafka.Header{{Key: "eventId", Value: []byte(item.Envelope.EventID)}},
			})
		}
		if err != nil {
			if p.Metrics != nil {
				p.Metrics.ProducerFailures.Inc()
			}
			_ = p.Store.MarkPublishFailed(ctx, item.Envelope.EventID, item.Attempts, err)
			continue
		}
		if err := p.Store.MarkPublished(ctx, item.Envelope.EventID); err != nil {
			p.Logger.Error("published but mark failed; duplicate is possible",
				"eventId", item.Envelope.EventID, "error", err)
		}
	}
	return nil
}
