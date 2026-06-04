package app

import (
	"context"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
	"sync"
)

type Pipeline struct {
	vpStage         *pipeline.VolumeProfileStage
	enrichment      *pipeline.EnrichmentStage
	analytics       *pipeline.AnalyticsEngine
	barManager      *pipeline.BarManager
	hqEngine        *pipeline.Headquarters
	scoutStage      *pipeline.ScoutStage
	dbWriter        *writer.DBWriter
	tickIndexMap    map[uint32]int
	lastVolRankMap  map[uint32]int // ⚡ Added for velocity tracking
	lastTickRankMap map[uint32]int // ⚡ Added for velocity tracking
	indexMu         sync.Mutex
}

func NewPipeline(
	vpStage *pipeline.VolumeProfileStage,
	enrichment *pipeline.EnrichmentStage,
	analytics *pipeline.AnalyticsEngine,
	barManager *pipeline.BarManager,
	hqEngine *pipeline.Headquarters,
	scoutStage *pipeline.ScoutStage,
	dbWriter *writer.DBWriter,
) *Pipeline {
	return &Pipeline{
		vpStage:         vpStage,
		enrichment:      enrichment,
		analytics:       analytics,
		barManager:      barManager,
		hqEngine:        hqEngine,
		scoutStage:      scoutStage,
		dbWriter:        dbWriter,
		tickIndexMap:    make(map[uint32]int),
		lastVolRankMap:  make(map[uint32]int),
		lastTickRankMap: make(map[uint32]int),
	}
}

// Process implements the stream.TickProcessor interface
func (p *Pipeline) Process(rawTick models.TickData) error {
	token := rawTick.InstrumentToken

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

	// 2. ENRICHMENT STAGE
	enrichedTick := &models.EnrichedTick{Raw: rawTick}
	if err := p.enrichment.Process(enrichedTick); err != nil {
		return err
	}

	// 3. VOLUME PROFILE STAGE
	if p.vpStage != nil {
		if err := p.vpStage.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Failed to process volume profile: %v", err)
		}
	}

	if p.hqEngine != nil {
		p.hqEngine.IngestPipelineTick(context.Background(), enrichedTick)
	}

	// 4. ANALYTICS STAGE
	if p.analytics != nil {
		p.analytics.Analyze(enrichedTick)
	}

	// 5. BAR MANAGER AGGREGATION LAYER
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
		rollingBars := p.barManager.GetActiveBarsSnapshot(token)
		profile, hasProfile := p.enrichment.GetInstrumentProfile(token)

		atr14 := 0.0
		if hasProfile && profile != nil {
			atr14 = profile.ATR14
		}

		p.indexMu.Lock()
		prevVolRank := p.lastVolRankMap[token]
		prevTickRank := p.lastTickRankMap[token]

		// Fallback calibration defaults for initialization steps
		if prevVolRank == 0 {
			prevVolRank = 4
		}
		if prevTickRank == 0 {
			prevTickRank = 4
		}

		// ⚡ Pass the cached previous steps down into our new 28-dimensional pipeline structure
		observationVector := enrichedTick.CompileObservationVector(atr14, rollingBars, prevVolRank, prevTickRank)

		// Overwrite priority rank history blocks for the next step execution
		p.lastVolRankMap[token] = enrichedTick.Enrichment.VolumeRank
		p.lastTickRankMap[token] = enrichedTick.Enrichment.TickRank

		p.tickIndexMap[token]++
		currentTickIdx := p.tickIndexMap[token]
		p.indexMu.Unlock()

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
	p.lastVolRankMap = make(map[uint32]int)
	p.lastTickRankMap = make(map[uint32]int)
	p.indexMu.Unlock()
}
