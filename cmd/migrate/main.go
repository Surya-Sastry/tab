package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/Surya-Sastry/tab/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	path := filepath.Join("migrations", "001_init.sql")
	body, err := os.ReadFile(path)
	if err != nil {
		logger.Error("read migration failed", "path", path, "error", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Error("postgres failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	up := strings.SplitN(string(body), "-- +goose Down", 2)[0]
	tx, err := pool.Begin(context.Background())
	if err != nil {
		logger.Error("begin migration failed", "error", err)
		os.Exit(1)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `SELECT pg_advisory_xact_lock(807020)`); err != nil {
		logger.Error("migration lock failed", "error", err)
		os.Exit(1)
	}
	if _, err := tx.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version bigint PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		logger.Error("migration table failed", "error", err)
		os.Exit(1)
	}
	var applied bool
	if err := tx.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=1)`).Scan(&applied); err != nil {
		logger.Error("migration lookup failed", "error", err)
		os.Exit(1)
	}
	if applied {
		if err := tx.Commit(context.Background()); err != nil {
			logger.Error("migration commit failed", "error", err)
			os.Exit(1)
		}
		logger.Info("migration already applied", "version", 1)
		return
	}
	if _, err := tx.Exec(context.Background(), up); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
	if _, err := tx.Exec(context.Background(), `INSERT INTO schema_migrations (version) VALUES (1)`); err != nil {
		logger.Error("migration record failed", "error", err)
		os.Exit(1)
	}
	if err := tx.Commit(context.Background()); err != nil {
		logger.Error("migration commit failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migration complete", "file", path)
}
