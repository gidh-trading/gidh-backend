package pipeline

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	SmoothingAlpha = 0.6 // 4-bar rolling micro-window alpha boundary
)

// ContinuousLivingLedger tracks un-netted structural order flow states and compounding active heatmaps
type ContinuousLivingLedger struct {
	LastUpdated               time.Time
	ContinuousVolumeIntensity float64 // Smooth, compounding accumulation of market volume pressure
	ContinuousPriceNormalized float64 // Smooth, compounding accumulation of directional momentum
	VwapClosePct              float64 // Locked historical percentage of bars closing above VWAP
}

type TimeframeAnalyticsHistory struct {
	BarsAboveVwap     int
	TotalBars         int
	RollingVolumeMean float64

	// --- Alpha 0.4 Historical Running Metrics ---
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

	// 1. Advance independent rolling averages using 0.4 alpha
	alpha := SmoothingAlpha
	h.RollingVolumeIntensity = (alpha * bar.Analytics.VolumeIntensity) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	h.RollingPriceNormalized = (alpha * bar.Analytics.PriceNormalizedChange) + ((1.0 - alpha) * h.RollingPriceNormalized)
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

	// 1. Linearly project what the current forming bar's metrics look like inside the window
	alpha := SmoothingAlpha
	bar.Analytics.RollingVolumeIntensity = (alpha * bar.Analytics.VolumeIntensity) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	bar.Analytics.RollingPriceNormalized = (alpha * bar.Analytics.PriceNormalizedChange) + ((1.0 - alpha) * h.RollingPriceNormalized)
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
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 1. RAW VOLUME INTENSITY
	var volumeIntensity float64 = 0.0
	profile, exists := bae.profiles[uint32(bar.InstrumentToken)]
	if exists && profile.ADV30d > 0 {
		expectedBars := bae.getExpectedBarsForTimeframe(bar.Timeframe)
		expectedBarVolumeBaseline := float64(profile.ADV30d) / expectedBars

		rawVolImpact := float64(bar.Volume) / expectedBarVolumeBaseline
		abnormalityVolMultiplier := float64(bar.Analytics.VolumeRank) / 4.0

		volumeIntensity = rawVolImpact * abnormalityVolMultiplier
	}
	bar.Analytics.VolumeIntensity = volumeIntensity

	// 2. RAW PRICE NORMALIZED CHANGE
	var priceIntensity float64 = 0.0
	liveBodyMovement := math.Abs(bar.Close - bar.Open)

	var bodySign float64 = 0.0
	if bar.Close > bar.Open {
		bodySign = 1.0
	} else if bar.Close < bar.Open {
		bodySign = -1.0
	}

	dna, dnaExists := bae.dnaMap[uint32(bar.InstrumentToken)]
	var tierBaseline float64 = 0.0

	if dnaExists && dna.IntervalPercentiles != nil {
		if tfMetrics, tfExists := dna.IntervalPercentiles[bar.Timeframe]; tfExists {
			switch bar.Analytics.PriceRank {
			case 7:
				tierBaseline = tfMetrics.PriceP97
			case 6:
				tierBaseline = tfMetrics.PriceP90
			case 5:
				tierBaseline = tfMetrics.PriceP75
			case 4:
				tierBaseline = tfMetrics.PriceP50
			default:
				tierBaseline = tfMetrics.PriceP25
			}
		}
	}

	if tierBaseline > 0 {
		rawPriceImpact := liveBodyMovement / tierBaseline
		priceIntensity = rawPriceImpact * float64(bar.Analytics.PriceRank) * bodySign
	} else {
		priceIntensity = float64(bar.Analytics.PriceRank) * bodySign
	}
	bar.Analytics.PriceNormalizedChange = priceIntensity
}
