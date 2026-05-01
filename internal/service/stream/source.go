package stream

import (
	"context"
	"gidh-backend/internal/service/models"
)

// TickDataSource defines the interface for components that provide tick data.
type TickDataSource interface {
	// Connect establishes the connection to the data source.
	Connect(ctx context.Context) error

	// Subscribe registers interest in specific instruments.
	Subscribe(instrumentTokens []uint32) error

	// ReadTicks streams data to the provided channel.
	// Runs until context cancellation or EOF.
	ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error

	// Close terminates the connection and cleans up resources.
	Close() error

	// Type returns the source type (live/backtest)
	Type() SourceType
}

type SourceType string

const (
	SourceLive     SourceType = "live"
	SourceBacktest SourceType = "backtest"
)
