package app

import (
	"errors"
	"time"

	"gidh-backend/internal/service/stream"
	"gidh-backend/pkg/config" // Global Config
)

// createDataSource handles the factory logic for selecting the source type.
// It now utilizes the global AppConfig to determine the mode.
func (a *App) createDataSource() (stream.TickDataSource, error) {
	if config.AppConfig.Mode == "live" {
		return a.createLiveSource()
	}
	return a.createBacktestSource()
}

func (a *App) createLiveSource() (stream.TickDataSource, error) {
	// Passing app.orderMgr so the LiveTickSource can trigger HandleOrderUpdate on WebSocket fills
	return stream.NewLiveSource(&stream.LiveSourceConfig{
		APIKey:        config.AppConfig.KiteAPIKey,
		AccessToken:   config.AppConfig.KiteAccessToken,
		InstrumentMap: a.tokenToName,
		Instruments:   a.extractTokens(),
		OrderHandler:  a.PositionManager,
	})
}

func (a *App) createBacktestSource() (stream.TickDataSource, error) {
	// Validation using the global configuration
	if config.AppConfig.BacktestDate == "" {
		return nil, errors.New("BACKTEST_DATE is required for mode=backtest in .env")
	}

	// Parse the backtest date from the global config
	d, err := time.Parse("2006-01-02", config.AppConfig.BacktestDate)
	if err != nil {
		return nil, errors.New("invalid BACKTEST_DATE format; use YYYY-MM-DD")
	}

	// Build the instrument list from the current app state
	instruments := make([]struct {
		Name  string
		Token uint32
	}, len(a.instrumentList))

	for i, inst := range a.instrumentList {
		instruments[i].Name = inst.Name
		instruments[i].Token = inst.Token
	}

	// Initialize the backtest source with settings from config.AppConfig
	return stream.NewBacktestSource(&stream.BacktestSourceConfig{
		DataDir:     config.AppConfig.BacktestDataDir,
		Date:        d,
		SpeedFactor: config.AppConfig.BacktestSpeedFactor,
		Instruments: instruments,
		NameToToken: a.nameToToken,
	}), nil
}

func (a *App) extractTokens() []uint32 {
	tokens := make([]uint32, len(a.instrumentList))
	for i, inst := range a.instrumentList {
		tokens[i] = inst.Token
	}
	return tokens
}
