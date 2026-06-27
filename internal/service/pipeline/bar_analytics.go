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
	RollingEfficiencyRank  float64 // Added
	RollingMomentumScore   float64 // Added

	// --- Prior Closed Benchmarks to Anchors Slopes ---
	LastClosedVolumeIntensity float64
	LastClosedPriceNormalized float64
	LastClosedTickRank        float64
	LastClosedEfficiencyRank  float64 // Added
	LastClosedMomentumScore   float64 // Added
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

	// --- 1. SMOOTH THE INDEPENDENT BASELINES AS USUAL ---
	alpha := SmoothingAlpha
	h.RollingVolumeIntensity = (alpha * float64(bar.Analytics.VolumeRank)) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	h.RollingPriceNormalized = (alpha * float64(bar.Analytics.PriceRank)) + ((1.0 - alpha) * h.RollingPriceNormalized)
	h.RollingTickRank = (alpha * float64(bar.Analytics.TickRank)) + ((1.0 - alpha) * h.RollingTickRank)
	h.RollingEfficiencyRank = (alpha * float64(bar.Analytics.EfficiencyRank)) + ((1.0 - alpha) * h.RollingEfficiencyRank)

	// --- 2. COMPUTE RAW FLOW INTENSITY & SIGNED EXECUTION EDGE ---
	rollingFlowIntensity := (0.55 * h.RollingVolumeIntensity) + (0.45 * h.RollingTickRank)
	signedExecutionEdge := (0.60 * h.RollingPriceNormalized) + (0.40 * h.RollingEfficiencyRank)

	// --- 3. COMPUTE FLICKER-PROOF MOMENTUM SCORE ---
	flowMultiplier := rollingFlowIntensity / 4.0
	h.RollingMomentumScore = signedExecutionEdge * flowMultiplier

	// --- 4. MAP TO STRUCT OUTPUT LAYER (FIXED: Added Flow Intensity and Execution Edge fields) ---
	bar.Analytics.RollingVolumeIntensity = h.RollingVolumeIntensity
	bar.Analytics.RollingPriceNormalized = h.RollingPriceNormalized
	bar.Analytics.RollingTickRank = h.RollingTickRank
	bar.Analytics.RollingEfficiencyRank = h.RollingEfficiencyRank
	bar.Analytics.RollingFlowIntensity = rollingFlowIntensity
	bar.Analytics.RollingExecutionEdge = signedExecutionEdge
	bar.Analytics.RollingMomentumScore = h.RollingMomentumScore

	// --- 5. COMPUTE 1-BAR DIRECTIONAL SLOPES ---
	bar.Analytics.VolumeSlope = h.RollingVolumeIntensity - h.LastClosedVolumeIntensity
	bar.Analytics.PriceSlope = h.RollingPriceNormalized - h.LastClosedPriceNormalized
	bar.Analytics.TickSlope = h.RollingTickRank - h.LastClosedTickRank
	bar.Analytics.EfficiencySlope = h.RollingEfficiencyRank - h.LastClosedEfficiencyRank
	bar.Analytics.MomentumSlope = h.RollingMomentumScore - h.LastClosedMomentumScore

	// --- 6. UPDATE HISTORICAL CLOSED STRUCTURAL CHECKPOINTS ---
	h.LastClosedVolumeIntensity = h.RollingVolumeIntensity
	h.LastClosedPriceNormalized = h.RollingPriceNormalized
	h.LastClosedTickRank = h.RollingTickRank
	h.LastClosedEfficiencyRank = h.RollingEfficiencyRank
	h.LastClosedMomentumScore = h.RollingMomentumScore

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
	bar.Analytics.RollingEfficiencyRank = (alpha * float64(bar.Analytics.EfficiencyRank)) + ((1.0 - alpha) * h.RollingEfficiencyRank)

	// 2. Real-Time Composite A: Flow Intensity (FIXED: Uses projected bar metrics, not trailing h metrics)
	bar.Analytics.RollingFlowIntensity = (0.55 * bar.Analytics.RollingVolumeIntensity) + (0.45 * bar.Analytics.RollingTickRank)

	// 3. Real-Time Composite B: Execution Edge
	bar.Analytics.RollingExecutionEdge = (0.60 * bar.Analytics.RollingPriceNormalized) + (0.40 * bar.Analytics.RollingEfficiencyRank)

	// 4. Compute Projected Master Signed Momentum Score
	flowMultiplier := bar.Analytics.RollingFlowIntensity / 4.0
	bar.Analytics.RollingMomentumScore = bar.Analytics.RollingExecutionEdge * flowMultiplier

	// 5. Real-Time Slopes
	bar.Analytics.VolumeSlope = bar.Analytics.RollingVolumeIntensity - h.LastClosedVolumeIntensity
	bar.Analytics.PriceSlope = bar.Analytics.RollingPriceNormalized - h.LastClosedPriceNormalized
	bar.Analytics.TickSlope = bar.Analytics.RollingTickRank - h.LastClosedTickRank
	bar.Analytics.EfficiencySlope = bar.Analytics.RollingEfficiencyRank - h.LastClosedEfficiencyRank
	bar.Analytics.MomentumSlope = bar.Analytics.RollingMomentumScore - h.LastClosedMomentumScore

	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}
}

func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar) {
	// 1. Evaluates all parameters and ranks
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 2. Apply Directional Polarized Sign to Price and Efficiency Spectrum
	if bar.Close < bar.Open {
		if bar.Analytics.PriceRank > 0 {
			bar.Analytics.PriceRank = -bar.Analytics.PriceRank
		}
		if bar.Analytics.EfficiencyRank > 0 {
			bar.Analytics.EfficiencyRank = -bar.Analytics.EfficiencyRank
		}
	}
}
