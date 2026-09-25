package logging

import (
	"log/slog"
	"os"
)

// New returns a JSON structured logger suitable for container runtimes.
func New(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", service)
}
