package pipeline

import (
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	SmoothingAlpha = 0.4
)

// ContinuousLivingLedger tracks un-netted structural order flow states and compounding active heatmaps
type ContinuousLivingLedger struct {
	LastUpdated  time.Time
	VwapClosePct float64 // Locked historical percentage of bars closing above VWAP
}

type TimeframeAnalyticsHistory struct {
	BarsAboveVwap     int
	TotalBars         int
	RollingVolumeMean float64

	// --- Alpha 0.6 Historical Running Metrics ---
	RollingVolumeIntensity float64
	RollingPriceNormalized float64
	RollingTickRank        float64

	// --- Prior Closed Benchmarks to Anchors Slopes ---
	LastClosedVolumeIntensity float64
	LastClosedPriceNormalized float64
	LastClosedTickRank        float64
}

// BarAnalyticsEngine implements the stateless calculations layer across multi-thread pipelines.
type BarAnalyticsEngine struct {
	dnaMap   map[uint32]*models.MarketDNA
	profiles map[uint32]*models.InstrumentProfile
	dbWriter *writer.DBWriter
}

func NewBarAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA, profiles map[uint32]*models.InstrumentProfile, dbW *writer.DBWriter) *BarAnalyticsEngine {
	return &BarAnalyticsEngine{
		dnaMap:   dnaMap,
		profiles: profiles,
		dbWriter: dbW,
	}
}

// AnalyzeTick updates current tick boundaries and dynamically builds instantaneous bar structures
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	bar.Analytics.NormalizedVwapDistance = bae.calculateDistance(bar.Close, bar.VWAP, uint32(bar.InstrumentToken))
	bae.CalculateContinuousAndStructuralMetrics(bar)
}

// AnalyzeClose processes metrics at the close, advances isolated rolling vectors, and writes to database
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bae.CalculateContinuousAndStructuralMetrics(bar)

	h.TotalBars++
	if bar.Close > bar.VWAP {
		h.BarsAboveVwap++
	}
	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}

	// 1. Advance independent rolling averages using the clean 1-7 (and signed -7 to 7) integer ranks directly
	alpha := SmoothingAlpha
	h.RollingVolumeIntensity = (alpha * float64(bar.Analytics.VolumeRank)) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	h.RollingPriceNormalized = (alpha * float64(bar.Analytics.PriceRank)) + ((1.0 - alpha) * h.RollingPriceNormalized)
	h.RollingTickRank = (alpha * float64(bar.Analytics.TickRank)) + ((1.0 - alpha) * h.RollingTickRank)

	// 2. Map the permanent historical average parameters to the structural output
	bar.Analytics.RollingVolumeIntensity = h.RollingVolumeIntensity
	bar.Analytics.RollingPriceNormalized = h.RollingPriceNormalized
	bar.Analytics.RollingTickRank = h.RollingTickRank

	// 3. Compute 1-Bar Historical Close Slope (Current finalized average vs Prior finalized average)
	bar.Analytics.VolumeSlope = h.RollingVolumeIntensity - h.LastClosedVolumeIntensity
	bar.Analytics.PriceSlope = h.RollingPriceNormalized - h.LastClosedPriceNormalized
	bar.Analytics.TickSlope = h.RollingTickRank - h.LastClosedTickRank

	// 4. Update the tracking checkpoints for the next incoming live candle
	h.LastClosedVolumeIntensity = h.RollingVolumeIntensity
	h.LastClosedPriceNormalized = h.RollingPriceNormalized
	h.LastClosedTickRank = h.RollingTickRank

	// 5. Commit structured output to DB
	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics populates live snapshots for visual WebSocket feeds without mutating master history
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bae.CalculateContinuousAndStructuralMetrics(bar)

	// 1. Linearly project what the current forming bar's metrics look like inside the window using integer ranks
	alpha := SmoothingAlpha
	bar.Analytics.RollingVolumeIntensity = (alpha * float64(bar.Analytics.VolumeRank)) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	bar.Analytics.RollingPriceNormalized = (alpha * float64(bar.Analytics.PriceRank)) + ((1.0 - alpha) * h.RollingPriceNormalized)
	bar.Analytics.RollingTickRank = (alpha * float64(bar.Analytics.TickRank)) + ((1.0 - alpha) * h.RollingTickRank)

	// 2. Real-Time Slope: Evaluate current forming window trajectory vs last finalized block anchor
	bar.Analytics.VolumeSlope = bar.Analytics.RollingVolumeIntensity - h.LastClosedVolumeIntensity
	bar.Analytics.PriceSlope = bar.Analytics.RollingPriceNormalized - h.LastClosedPriceNormalized
	bar.Analytics.TickSlope = bar.Analytics.RollingTickRank - h.LastClosedTickRank

	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}
}

func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar) {
	// 1. Evaluates structural parameters and updates bar.Analytics.PriceRank, RangeRank, etc. (from 1 to 7)
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 2. Apply Directional Polarized Sign to PriceRank (-7 to +7 Spectrum)
	// If the bar closed below its opening price, flip the rank negative to track bearish momentum.
	if bar.Close < bar.Open && bar.Analytics.PriceRank > 0 {
		bar.Analytics.PriceRank = -bar.Analytics.PriceRank
	}
}
