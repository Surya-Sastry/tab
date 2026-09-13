package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"strconv"
	"time"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/event"
	"github.com/Surya-Sastry/tab/internal/observability"
	"github.com/Surya-Sastry/tab/internal/postgres"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

type Handler interface {
	Handle(context.Context, event.Envelope) error
}

type permanentError struct{ error }

func permanent(err error) error { return permanentError{error: err} }

func isPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

type Consumer struct {
	Reader      *kafka.Reader
	Store       *postgres.Store
	Name        string
	Handler     Handler
	MaxAttempts int
	Logger      *slog.Logger
	Metrics     *observability.Metrics
}

func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, Topic: topic, GroupID: groupID,
		MinBytes: 1, MaxBytes: 10e6, MaxWait: time.Second,
		CommitInterval: 0,
	})
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	for {
		message, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if c.Metrics != nil {
				c.Metrics.ConsumerFailures.WithLabelValues(c.Name).Inc()
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Warn("consumer fetch failed; reconnecting", "consumer", c.Name, "error", err)
			if err := wait(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		var envelope event.Envelope
		err = json.Unmarshal(message.Value, &envelope)
		if err == nil {
			err = envelope.Validate()
		}
		if err != nil {
			err = permanent(err)
		} else {
			for attempt := 1; ; attempt++ {
				err = c.Handler.Handle(ctx, envelope)
				if err == nil {
					break
				}
				if isPermanent(err) && attempt >= c.MaxAttempts {
					break
				}
				if waitErr := wait(ctx, time.Duration(min(attempt, 10))*time.Second); waitErr != nil {
					return waitErr
				}
			}
		}
		if err != nil {
			if c.Metrics != nil {
				c.Metrics.ConsumerFailures.WithLabelValues(c.Name).Inc()
				c.Metrics.DLQTotal.WithLabelValues(c.Name).Inc()
			}
			c.Logger.Error("event sent to dlq", "consumer", c.Name,
				"eventId", envelope.EventID, "partition", message.Partition,
				"offset", message.Offset, "error", err)
			if dlqErr := c.Store.PutDLQ(ctx, c.Name, envelope, err); dlqErr != nil {
				return fmt.Errorf("store dlq event: %w", dlqErr)
			}
		}
		for attempt := 1; ; attempt++ {
			if err := c.Reader.CommitMessages(ctx, message); err == nil {
				break
			} else if ctx.Err() != nil {
				return ctx.Err()
			} else {
				c.Logger.Warn("offset commit failed; retrying", "consumer", c.Name,
					"partition", message.Partition, "offset", message.Offset, "error", err)
			}
			if err := wait(ctx, time.Duration(min(attempt, 10))*time.Second); err != nil {
				return err
			}
		}
	}
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type BalanceHandler struct {
	Store  *postgres.Store
	Name   string
	Writer *kafka.Writer
}

func (h BalanceHandler) Handle(ctx context.Context, envelope event.Envelope) error {
	_, err := h.Store.ApplyBalanceEvent(ctx, h.Name, envelope)
	if errors.Is(err, domain.ErrInvalid) {
		return permanent(err)
	}
	if err != nil || h.Writer == nil {
		return err
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return h.Writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(envelope.AggregateID), Value: body,
	})
}

type FeedHandler struct {
	Collection *mongo.Collection
}

func EnsureFeedIndexes(ctx context.Context, collection *mongo.Collection) error {
	_, err := collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "eventId", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "groupId", Value: 1}, {Key: "createdAt", Value: -1}}},
	})
	return err
}

func (h FeedHandler) Handle(ctx context.Context, envelope event.Envelope) error {
	item, err := postgres.ActivityFromEnvelope(envelope)
	if err != nil {
		return permanent(err)
	}
	_, err = h.Collection.UpdateOne(ctx, bson.M{"eventId": item.EventID},
		bson.M{"$setOnInsert": item}, options.Update().SetUpsert(true))
	return err
}

type NotificationHandler struct {
	Store    *postgres.Store
	Name     string
	SMTPHost string
	SMTPPort int
}

func (h NotificationHandler) Handle(ctx context.Context, envelope event.Envelope) error {
	// The processed marker intentionally precedes SMTP. This avoids duplicate email
	// but can miss one notification if SMTP fails; financial projections do not use this tradeoff.
	tx, err := h.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer_name,event_id) VALUES ($1,$2)
		ON CONFLICT DO NOTHING`, h.Name, envelope.EventID)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil || tag.RowsAffected() == 0 {
		return err
	}
	address := net.JoinHostPort(h.SMTPHost, strconv.Itoa(h.SMTPPort))
	message := []byte("To: tab-local@example.invalid\r\n" +
		"Subject: Tab activity\r\n\r\nA group expense changed.\r\n")
	return smtp.SendMail(address, nil, "tab@example.invalid",
		[]string{"tab-local@example.invalid"}, message)
}
