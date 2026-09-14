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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
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
	writer := broker.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer writer.Close()
	publisher := broker.Publisher{Store: store, Writer: writer, Logger: logger, Metrics: metrics}
	if err := publisher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("publisher stopped", "error", err)
		os.Exit(1)
	}
}
