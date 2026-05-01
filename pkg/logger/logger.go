package logger

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	once  sync.Once
	level = new(slog.LevelVar) // Allows dynamic level changes
)

// Init initializes the global logger with a specific level (debug, info, warn, error).
func Init(lvl string) {
	once.Do(func() {
		switch strings.ToLower(lvl) {
		case "debug":
			level.Set(slog.LevelDebug)
		case "warn":
			level.Set(slog.LevelWarn)
		case "error":
			level.Set(slog.LevelError)
		default:
			level.Set(slog.LevelInfo)
		}

		opts := &slog.HandlerOptions{
			Level: level,
			// AddSource: true,
		}

		handler := slog.NewJSONHandler(os.Stdout, opts)
		slog.SetDefault(slog.New(handler))
	})
}

// --- Structured Methods (Key-Value pairs) ---

func Debug(msg string, args ...any) { slog.Debug(msg, args...) }
func Info(msg string, args ...any)  { slog.Info(msg, args...) }
func Warn(msg string, args ...any)  { slog.Warn(msg, args...) }
func Error(msg string, args ...any) { slog.Error(msg, args...) }

// --- Printf-style Methods (Formatted strings) ---

func Debugf(format string, args ...any) { slog.Debug(fmt.Sprintf(format, args...)) }
func Infof(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
func Warnf(format string, args ...any)  { slog.Warn(fmt.Sprintf(format, args...)) }
func Errorf(format string, args ...any) { slog.Error(fmt.Sprintf(format, args...)) }
