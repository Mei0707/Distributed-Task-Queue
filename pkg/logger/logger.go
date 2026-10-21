package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New creates a production-ready structured logger.
// Logs are JSON-formatted for easy ingestion by log aggregators (Datadog, ELK, etc).
func New(env string) (*zap.Logger, error) {
	var cfg zap.Config

	if env == "development" {
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg = zap.NewProductionConfig()
	}

	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return cfg.Build()
}

// NewNop returns a no-op logger useful in tests.
func NewNop() *zap.Logger {
	return zap.NewNop()
}
