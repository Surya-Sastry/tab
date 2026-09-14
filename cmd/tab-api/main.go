package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Surya-Sastry/tab/internal/broker"
	"github.com/Surya-Sastry/tab/internal/config"
	"github.com/Surya-Sastry/tab/internal/event"
	"github.com/Surya-Sastry/tab/internal/httpapi"
	"github.com/Surya-Sastry/tab/internal/observability"
	"github.com/Surya-Sastry/tab/internal/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--healthcheck" {
		client := http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://127.0.0.1:8080/health/live")
		if err != nil || response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		response.Body.Close()
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	if len(cfg.JWTSigningKey) < 32 || len(cfg.AdminReplayKey) < 32 {
		logger.Error("JWT_SIGNING_KEY and ADMIN_REPLAY_KEY must each be at least 32 characters")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	metrics := observability.New(prometheus.DefaultRegisterer)
	api := httpapi.New(store, cfg.JWTSigningKey, cfg.AdminReplayKey, logger, metrics)
	go sampleOutbox(ctx, store, metrics, logger)
	api.ReplayWriter = broker.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer api.ReplayWriter.Close()
	wsReader := broker.NewReader(cfg.KafkaBrokers, "balance.updated", "tab-websocket")
	defer wsReader.Close()
	wsConsumer := broker.Consumer{
		Reader: wsReader, Store: store, Name: "tab-websocket",
		Handler: wsHandler{store: store, hub: api.Hub}, MaxAttempts: 3, Logger: logger,
	}
	go func() {
		if err := wsConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("websocket event consumer stopped", "error", err)
		}
	}()

	var mongoClient *mongo.Client
	if cfg.MongoDBURI != "" {
		mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoDBURI))
		if err != nil {
			logger.Warn("activity projection disabled", "error", err)
		} else {
			defer mongoClient.Disconnect(context.Background())
			api.Activity = mongoClient.Database("tab").Collection("activity")
		}
	}

	server := &http.Server{
		Addr: cfg.HTTPAddress, Handler: api.Routes(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	go func() {
		logger.Info("api listening", "address", cfg.HTTPAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func sampleOutbox(ctx context.Context, store *postgres.Store, metrics *observability.Metrics, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		stats, err := store.OutboxStats(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Warn("outbox metrics query failed", "error", err)
		} else if err == nil {
			metrics.OutboxPending.Set(float64(stats.Pending))
			metrics.OutboxOldestAge.Set(stats.OldestAge.Seconds())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type wsHandler struct {
	store *postgres.Store
	hub   *httpapi.Hub
}

func (h wsHandler) Handle(ctx context.Context, envelope event.Envelope) error {
	tag, err := h.store.Pool.Exec(ctx, `
		INSERT INTO processed_events (consumer_name,event_id) VALUES ('tab-websocket',$1)
		ON CONFLICT DO NOTHING`, envelope.EventID)
	if err != nil || tag.RowsAffected() == 0 {
		return err
	}
	h.hub.Broadcast(envelope.AggregateID, map[string]any{
		"type": "balance.updated", "groupId": envelope.AggregateID,
		"eventId": envelope.EventID, "aggregateVersion": envelope.AggregateVersion,
	})
	return nil
}
