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

	// Load environment variables
	if err := env.Load(".env"); err != nil {
		logger.Warnf("No .env file found: %v", err)
	}

	// Initialize global configuration and logger
	config.Load()
	logger.Init(config.AppConfig.LogLevel)

	// 1. Create a context that listens for the interrupt signals (Ctrl+C, SIGTERM)
	// This replaces the manual channel management in your previous version
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 2. Initialize the Application
	application, err := app.NewApp(config.AppConfig)
	if err != nil {
		logger.Errorf("Failed to initialize app: %v", err)
		os.Exit(1)
	}

	// 3. Start the Application
	// This will now launch the HTTP server and Data Stream in a non-blocking way
	if err := application.Start(ctx); err != nil {
		logger.Errorf("Fatal application error: %v", err)
		os.Exit(1)
	}

	// 4. Block the main thread until the context is canceled (signal received)
	<-ctx.Done()

	logger.Info("Termination signal received, shutting down gracefully...")

	// 5. Trigger the graceful shutdown sequence defined in app.go
	// This will shut down the server, close WebSocket clients, and flush the DB
	application.Stop()

	logger.Info("Gidh Backend exited cleanly.")
}
