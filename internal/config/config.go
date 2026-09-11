package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress    string
	MetricsAddress string
	DatabaseURL    string
	MongoDBURI     string
	KafkaBrokers   []string
	KafkaTopic     string
	JWTSigningKey  string
	SMTPHost       string
	SMTPPort       int
	AdminReplayKey string
	ShutdownGrace  time.Duration
}

func Load() (Config, error) {
	port, err := strconv.Atoi(value("SMTP_PORT", "1025"))
	if err != nil {
		return Config{}, fmt.Errorf("SMTP_PORT: %w", err)
	}
	grace, err := time.ParseDuration(value("SHUTDOWN_GRACE", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("SHUTDOWN_GRACE: %w", err)
	}
	cfg := Config{
		HTTPAddress:    value("HTTP_ADDRESS", ":8080"),
		MetricsAddress: value("METRICS_ADDRESS", ":9090"),
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		MongoDBURI:     os.Getenv("MONGODB_URI"),
		KafkaBrokers:   split(value("KAFKA_BROKERS", "redpanda:9092")),
		KafkaTopic:     value("KAFKA_TOPIC", "expense.events"),
		JWTSigningKey:  os.Getenv("JWT_SIGNING_KEY"),
		SMTPHost:       value("SMTP_HOST", "mailpit"),
		SMTPPort:       port,
		AdminReplayKey: os.Getenv("ADMIN_REPLAY_KEY"),
		ShutdownGrace:  grace,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func split(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
