package app

import (
	"log"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/writer"
)

type Pipeline struct {
	vpStage    *pipeline.VolumeProfileStage
	enrichment *pipeline.EnrichmentStage
	barBuilder *pipeline.BarBuilderStage
	dbWriter   *writer.DBWriter
}

func NewPipeline(
	vpStage *pipeline.VolumeProfileStage,
	enrichment *pipeline.EnrichmentStage,
	barBuilder *pipeline.BarBuilderStage,
	dbWriter *writer.DBWriter,
) *Pipeline {
	return &Pipeline{
		vpStage:    vpStage,
		enrichment: enrichment,
		barBuilder: barBuilder,
		dbWriter:   dbWriter,
	}
}

// Process implements the stream.TickProcessor interface
func (p *Pipeline) Process(rawTick models.TickData) error {
	// 1. RAW DATA ARCHIVE
	if p.dbWriter != nil {
		p.dbWriter.AddTick(rawTick)
		for _, bid := range rawTick.Depth.Buy {
			p.dbWriter.AddDepth(rawTick.Timestamp, rawTick.InstrumentToken, rawTick.StockName, "buy", bid)
		}
		for _, ask := range rawTick.Depth.Sell {
			p.dbWriter.AddDepth(rawTick.Timestamp, rawTick.InstrumentToken, rawTick.StockName, "sell", ask)
		}
	}

	// 2. STAGE 1: ENRICHMENT ...[cite: 4]
	enrichedTick := &models.EnrichedTick{Raw: rawTick}
	if err := p.enrichment.Process(enrichedTick); err != nil {
		return err
	}

	// 3. STAGE 2: BAR BUILDER (Runs after enrichment so it has TickVolume)
	if p.barBuilder != nil {
		if err := p.barBuilder.Process(enrichedTick); err != nil {
			log.Printf("Pipeline Error: Failed to process bars: %v", err)
		}
	}

	// 4. STAGE 3: VOLUME PROFILE ...[cite: 4]
	if p.vpStage != nil {
		if err := p.vpStage.Process(enrichedTick); err != nil {
			log.Printf("Pipeline Error: Failed to process volume profile: %v", err)
		}
	}

	return nil
}
