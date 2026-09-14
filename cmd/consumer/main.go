package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Surya-Sastry/tab/internal/broker"
	"github.com/Surya-Sastry/tab/internal/config"
	"github.com/Surya-Sastry/tab/internal/observability"
	"github.com/Surya-Sastry/tab/internal/postgres"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	role := os.Getenv("WORKER_ROLE")
	if role != "balance" && role != "feed" && role != "notification" {
		logger.Error("WORKER_ROLE must be balance, feed, or notification")
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
	observability.Serve(ctx, cfg.MetricsAddress, logger)
	name := "tab-" + role
	reader := broker.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, name)
	defer reader.Close()

	var handler broker.Handler
	var mongoClient *mongo.Client
	switch role {
	case "balance":
		writer := broker.NewWriter(cfg.KafkaBrokers, "balance.updated")
		defer writer.Close()
		handler = broker.BalanceHandler{Store: store, Name: name, Writer: writer}
	case "feed":
		if cfg.MongoDBURI == "" {
			logger.Error("MONGODB_URI is required for feed consumer")
			os.Exit(1)
		}
		mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoDBURI))
		if err != nil {
			logger.Error("mongo failed", "error", err)
			os.Exit(1)
		}
		defer mongoClient.Disconnect(context.Background())
		collection := mongoClient.Database("tab").Collection("activity")
		if err := broker.EnsureFeedIndexes(ctx, collection); err != nil {
			logger.Error("mongo index failed", "error", err)
			os.Exit(1)
		}
		handler = broker.FeedHandler{Collection: collection}
	case "notification":
		handler = broker.NotificationHandler{
			Store: store, Name: name, SMTPHost: cfg.SMTPHost, SMTPPort: cfg.SMTPPort,
		}
	}
	consumer := broker.Consumer{
		Reader: reader, Store: store, Name: name, Handler: handler,
		MaxAttempts: 5, Logger: logger, Metrics: metrics,
	}
	if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("consumer stopped", "role", role, "error", err)
		os.Exit(1)
	}
}
