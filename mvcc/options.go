package mvcc

import (
	"log/slog"
	"os"
	"time"
)

type config struct {
	gcInterval            time.Duration
	deadlockCheckInterval time.Duration
	logger                *slog.Logger
}

func defaultConfig() config {
	return config{
		gcInterval:            5 * time.Second,
		deadlockCheckInterval: 100 * time.Millisecond,
		logger:                slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
}

// Option — функциональная опция для MVCCMap.
type Option func(*config)

// WithGCInterval устанавливает интервал сборки старых версий.
func WithGCInterval(d time.Duration) Option {
	return func(c *config) { c.gcInterval = d }
}

// WithDeadlockCheckInterval устанавливает интервал проверки дедлоков.
func WithDeadlockCheckInterval(d time.Duration) Option {
	return func(c *config) { c.deadlockCheckInterval = d }
}

// WithLogger устанавливает кастомный slog.Logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) { c.logger = l }
}
