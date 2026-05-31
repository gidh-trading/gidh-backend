package app

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
	"sync"
)

type Pipeline struct {
	vpStage      *pipeline.VolumeProfileStage
	enrichment   *pipeline.EnrichmentStage
	analytics    *pipeline.AnalyticsEngine
	barManager   *pipeline.BarManager
	scoutStage   *pipeline.ScoutStage
	dbWriter     *writer.DBWriter
	tickIndexMap map[uint32]int
	indexMu      sync.Mutex
}

func NewPipeline(
	vpStage *pipeline.VolumeProfileStage,
	enrichment *pipeline.EnrichmentStage,
	analytics *pipeline.AnalyticsEngine,
	barManager *pipeline.BarManager,
	scoutStage *pipeline.ScoutStage,

	dbWriter *writer.DBWriter,
) *Pipeline {
	return &Pipeline{
		vpStage:      vpStage,
		enrichment:   enrichment,
		analytics:    analytics,
		barManager:   barManager,
		scoutStage:   scoutStage,
		dbWriter:     dbWriter,
		tickIndexMap: make(map[uint32]int),
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
	if p.analytics != nil {
		p.analytics.Analyze(enrichedTick)
	}

	// 5. BAR MANAGER AGGREGATION LAYER (Handles timeframes and records peak ranks)
	if p.barManager != nil {
		if err := p.barManager.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Failed to process bar accumulation: %v", err)
		}
	}

	if p.scoutStage != nil {
		if err := p.scoutStage.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Scout observer engine stage failure: %v", err)
		}
	}

	// 6. HISTORICAL TRANSLATION VECTOR CORE STORE
	if p.dbWriter != nil && p.barManager != nil && p.enrichment != nil {
		token := rawTick.InstrumentToken

		// Safely fetch active rolling candles and core instrument properties
		rollingBars := p.barManager.GetActiveBarsSnapshot(token)
		profile, hasProfile := p.enrichment.GetInstrumentProfile(token)

		atr14 := 0.0
		if hasProfile && profile != nil {
			atr14 = profile.ATR14
		}

		// 🧠 Execute the real-time matrix flattening method
		observationVector := enrichedTick.CompileObservationVector(atr14, rollingBars)

		// Increment individual index counts safely
		p.indexMu.Lock()
		p.tickIndexMap[token]++
		currentTickIdx := p.tickIndexMap[token]
		p.indexMu.Unlock()

		// Push the final dataset matrix down into the async CopyFrom pipeline buffer
		p.dbWriter.AddHistoricalFeature(writer.FeatureRecord{
			Timestamp: enrichedTick.Raw.Timestamp,
			StockName: enrichedTick.Raw.StockName,
			TickIndex: currentTickIdx,
			LastPrice: enrichedTick.Raw.LastPrice,
			ATR14:     atr14,
			Vector:    observationVector,
		})
	}

	return nil
}

func (p *Pipeline) Reset() {
	if p.barManager != nil {
		p.barManager.ClearState()
	}
	p.indexMu.Lock()
	p.tickIndexMap = make(map[uint32]int)
	p.indexMu.Unlock()
}
