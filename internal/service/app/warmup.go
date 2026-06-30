package app

import (
	"context"
	"time"

	"gidh-backend/internal/service/reader"
	"gidh-backend/pkg/logger"
)

// WarmupEngineState hydrates the internal RAM structures of the strategy layer
func (a *App) WarmupEngineState(ctx context.Context) error {
	if a.StrategyEngine == nil || a.pool == nil {
		logger.Warn("Engine warm-up skipped: StrategyEngine or DB Pool is uninitialized.")
		return nil
	}

	logger.Infof("⚡ Initiating system RAM strategy engine warm-up sequence...")

	// 1. Establish the operational date context boundary
	var targetDate time.Time
	if a.Config.Mode == "live" {
		targetDate = time.Now()
	} else {
		var err error
		targetDate, err = time.Parse("2006-01-02", a.Config.BacktestDate)
		if err != nil {
			return err
		}
	}

	// 2. Collect symbols configured for this active session
	symbols := make([]string, 0, len(a.instrumentList))
	for _, inst := range a.instrumentList {
		symbols = append(symbols, inst.Name)
	}

	if len(symbols) == 0 {
		logger.Warn("Warmup sequence aborted: No active instruments detected.")
		return nil
	}

	// 3. Extract matching candles chronologically from the jsonb database table
	barReader := reader.NewBarReader(a.pool)
	bars, err := barReader.FetchSessionBars(ctx, targetDate, symbols)
	if err != nil {
		return err
	}

	logger.Infof("Loaded %d closed bars across %d symbols for state rehydration.", len(bars), len(symbols))

	// 4. Chronologically feed metrics sequentially into strategy engine memory
	barsIngested := 0
	for _, bar := range bars {
		a.StrategyEngine.IngestClosedBar(bar)

		if a.Pipeline != nil {
			a.Pipeline.RehydrateHistoricalBar(bar)
		}

		barsIngested++
	}

	logger.Infof("🚀 Warmup sequence accomplished successfully. Ingested %d historical candles.", barsIngested)
	return nil
}
