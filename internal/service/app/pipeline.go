package app

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
	"sync"
)

type Pipeline struct {
	vpStage         *pipeline.VolumeProfileStage
	enrichment      *pipeline.EnrichmentStage
	barManager      *pipeline.BarManager
	scoutStage      *pipeline.ScoutStage
	dbWriter        *writer.DBWriter
	tickIndexMap    map[uint32]int
	lastVolRankMap  map[uint32]int // ⚡ Added for velocity tracking
	lastTickRankMap map[uint32]int // ⚡ Added for velocity tracking
	indexMu         sync.Mutex
	tokenLocks      [256]sync.Mutex
	AlgoAgent       interface {
		ProcessSequentialTick(enrichedTick *models.EnrichedTick)
	}
}

func NewPipeline(
	vpStage *pipeline.VolumeProfileStage,
	enrichment *pipeline.EnrichmentStage,
	barManager *pipeline.BarManager,
	scoutStage *pipeline.ScoutStage,
	dbWriter *writer.DBWriter,
) *Pipeline {
	return &Pipeline{
		vpStage:         vpStage,
		enrichment:      enrichment,
		barManager:      barManager,
		scoutStage:      scoutStage,
		dbWriter:        dbWriter,
		tickIndexMap:    make(map[uint32]int),
		lastVolRankMap:  make(map[uint32]int),
		lastTickRankMap: make(map[uint32]int),
	}
}

// Process implements the stream.TickProcessor interface
func (p *Pipeline) Process(rawTick models.TickData) error {

	shardIdx := rawTick.InstrumentToken % 256
	p.tokenLocks[shardIdx].Lock()
	defer p.tokenLocks[shardIdx].Unlock()

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

	// 4. BAR MANAGER AGGREGATION LAYER
	if p.barManager != nil {
		if err := p.barManager.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Failed to process bar accumulation: %v", err)
		}
	}

	// 5. Scout Alerts
	if p.scoutStage != nil {
		if err := p.scoutStage.Process(enrichedTick); err != nil {
			logger.Errorf("Pipeline Error: Scout observer engine stage failure: %v", err)
		}
	}

	// 6. Synchronous Agent Execution
	if p.AlgoAgent != nil {
		p.AlgoAgent.ProcessSequentialTick(enrichedTick)
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
