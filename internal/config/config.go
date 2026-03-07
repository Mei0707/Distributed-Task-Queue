package config

import (
	"log"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application.
// Values are loaded from config.yaml or environment variables.
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Queue  QueueConfig  `mapstructure:"queue"`
	Worker WorkerConfig `mapstructure:"worker"`
}

type ServerConfig struct {
	Port         string        `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type QueueConfig struct {
	// Queue names and their weights for weighted priority scheduling.
	// Higher weight = more worker attention allocated.
	Critical int `mapstructure:"critical"` // weight 6
	Default  int `mapstructure:"default"`  // weight 3
	Low      int `mapstructure:"low"`      // weight 1
}

type WorkerConfig struct {
	// Total number of concurrent goroutines processing tasks.
	Concurrency int `mapstructure:"concurrency"`

	// How long a task can run before being forcefully cancelled.
	TaskTimeout time.Duration `mapstructure:"task_timeout"`

	// Maximum number of retries before a task is moved to the dead letter queue.
	MaxRetry int `mapstructure:"max_retry"`

	// How long to keep completed tasks in Redis for inspection.
	RetentionTTL time.Duration `mapstructure:"retention_ttl"`
}

// Load reads configuration from file and environment variables.
// Environment variables take precedence over config file values.
// Example: REDIS_ADDR=localhost:6379 overrides redis.addr in config.yaml
func Load() (*Config, error) {
	v := viper.New()

	// --- Default values ---
	v.SetDefault("server.port", "8080")
	v.SetDefault("server.read_timeout", "10s")
	v.SetDefault("server.write_timeout", "10s")

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("queue.critical", 6)
	v.SetDefault("queue.default", 3)
	v.SetDefault("queue.low", 1)

	v.SetDefault("worker.concurrency", 20)
	v.SetDefault("worker.task_timeout", "5m")
	v.SetDefault("worker.max_retry", 5)
	v.SetDefault("worker.retention_ttl", "72h")

	// --- Config file ---
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Printf("Warning: could not read config file: %v — using defaults", err)
		}
	}

	// --- Environment variable overrides ---
	// e.g. REDIS_ADDR, SERVER_PORT, WORKER_CONCURRENCY
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}