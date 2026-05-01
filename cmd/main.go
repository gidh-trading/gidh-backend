// cmd/main.go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"gidh-backend/internal/service/app"
	"gidh-backend/pkg/config"
	"gidh-backend/pkg/env"
	"gidh-backend/pkg/logger"
)

func main() {
	if err := env.Load(".env"); err != nil {
		// Just a warning, as env vars might be injected via Docker
		logger.Warnf("No .env file found: %v", err)
	}

	// Initialize global singleton
	config.Load()
	logger.Init(config.AppConfig.LogLevel)

	// 1. Initialize the Application
	application, err := app.NewApp(config.AppConfig)
	if err != nil {
		logger.Errorf("Failed to initialize app: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Start the Application
	if err := application.Start(ctx); err != nil {
		logger.Errorf("Fatal application error: %v", err)
		os.Exit(1)
	}

	// 3. Listen for OS Shutdown Signals (Ctrl+C, Docker stop)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c // Blocks the main thread until a signal is received

	logger.Info("Termination signal received, shutting down gracefully...")
	application.Stop()
}
