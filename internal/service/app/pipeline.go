package app

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
)

type Pipeline struct {
	vpStage    *pipeline.VolumeProfileStage
	enrichment *pipeline.EnrichmentStage
	analytics  *pipeline.AnalyticsEngine
	barManager *pipeline.BarManager
	dbWriter   *writer.DBWriter
}

func NewPipeline(
	vpStage *pipeline.VolumeProfileStage,
	enrichment *pipeline.EnrichmentStage,
	analytics *pipeline.AnalyticsEngine,
	barManager *pipeline.BarManager,
	dbWriter *writer.DBWriter,
) *Pipeline {
	return &Pipeline{
		vpStage:    vpStage,
		enrichment: enrichment,
		analytics:  analytics,
		barManager: barManager,
		dbWriter:   dbWriter,
	}
}

// Process implements the stream.TickProcessor interface
func (p *Pipeline) Process(rawTick models.TickData) error {
	// 1. RAW STRUCT ARCHIVE WRITER STORAGE
	if p.dbWriter != nil {
		p.dbWriter.AddTick(rawTick)
		for _, bid := range rawTick.Depth.Buy {
			p.dbWriter.AddDepth(rawTick.Timestamp, rawTick.InstrumentToken, rawTick.StockName, "buy", bid)
		}
		for _, ask := range rawTick.Depth.Sell {
			p.dbWriter.AddDepth(rawTick.Timestamp, rawTick.InstrumentToken, rawTick.StockName, "sell", ask)
		}
	}

	// 2. ENRICHMENT STAGE (Calculates real-time microstructural Z-Scores & maintains Day-Long Timeline Canvas Array)
	enrichedTick := &models.EnrichedTick{Raw: rawTick}
	if err := p.enrichment.Process(enrichedTick); err != nil {
		return err
	}

	// 3. VOLUME PROFILE STAGE (Handles dynamic session market layout structures)
	if p.vpStage != nil {
		if err := p.vpStage.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Failed to process volume profile: %v", err)
		}
	}

	// 4. ANALYTICS STAGE (Evaluates the instantaneous pure Volume Burst Threshold)
	var snapshot models.AnomalySnapshot
	if p.analytics != nil {
		// Cleaned: AnalyticsEngine queries lookback context vectors internally from Enrichment Stage
		snapshot = p.analytics.Analyze(enrichedTick)
	}

	// 5. BAR MANAGER AGGREGATION LAYER (Handles timeframes and records peak ranks)
	if p.barManager != nil {
		if err := p.barManager.Process(enrichedTick, snapshot); err != nil {
			logger.Errorf("Pipeline Error: Failed to process bar accumulation: %v", err)
		}
	}

	return nil
}

func (p *Pipeline) Reset() {
	if p.barManager != nil {
		p.barManager.ClearState()
	}
}
