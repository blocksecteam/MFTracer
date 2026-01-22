package utils

import (
	"log/slog"
	"os"
	"strings"
	"transfer-graph-evm/model"

	"github.com/lmittmann/tint"
	"golang.org/x/term"
)

var Logger *slog.Logger

func SetupLoggers() {
	// Default to info level; allow override via config.toml [log.level]
	var logLevel slog.Level

	switch strings.ToLower(strings.TrimSpace(model.GetConfigLogLevel())) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	case "info":
		logLevel = slog.LevelInfo
	default:
		logLevel = slog.LevelInfo
	}

	var baseLogger *slog.Logger
	// Use colored output only when stderr is a terminal. When logs are
	// redirected to a file (or captured), use the plain text handler so
	// no ANSI color escape sequences are written to files.
	if term.IsTerminal(int(os.Stderr.Fd())) {
		baseLogger = slog.New(tint.NewHandler(os.Stderr, &tint.Options{
			Level:      logLevel,
			TimeFormat: "15:04:05",
		}))
	} else {
		// Plain text handler (no colors) for non-tty destinations like files.
		baseLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	}

	Logger = baseLogger.With("component", "main")
	slog.SetDefault(baseLogger)
}
