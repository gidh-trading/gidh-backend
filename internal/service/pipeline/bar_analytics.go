package pipeline

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	StandardDecayConstant = 0.80 // Baseline decay rate for trend bars
)

// ContinuousLivingLedger tracks un-netted structural order flow states and compounding active heatmaps
type ContinuousLivingLedger struct {
	LastUpdated               time.Time
	ContinuousVolumeIntensity float64 // Smooth, compounding accumulation of market volume pressure
	ContinuousPriceNormalized float64 // Smooth, compounding accumulation of directional momentum
	VwapClosePct              float64 // Locked historical percentage of bars closing above VWAP
}

type TimeframeAnalyticsHistory struct {
	Ledger            ContinuousLivingLedger
	BarsAboveVwap     int
	TotalBars         int
	RollingVolumeMean float64
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

// AnalyzeClose processes the metrics at the close of the bar, advances the continuous ledger, and commits to DB
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	// 1. Finalize current raw structural parameters
	bae.CalculateContinuousAndStructuralMetrics(bar)

	// 2. Increment historical закрытие session tracking states
	h.TotalBars++
	if bar.Close > bar.VWAP {
		h.BarsAboveVwap++
	}

	// 3. Compute locked, stateful indicators strictly on historical close data
	if h.TotalBars > 0 {
		h.Ledger.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}

	// 4. Advance the compounding Continuous Living Ledger state
	decay := StandardDecayConstant
	h.Ledger.ContinuousVolumeIntensity = (h.Ledger.ContinuousVolumeIntensity * decay) + bar.Analytics.VolumeIntensity
	h.Ledger.ContinuousPriceNormalized = (h.Ledger.ContinuousPriceNormalized * decay) + bar.Analytics.PriceNormalizedChange
	h.Ledger.LastUpdated = bar.Timestamp

	// 5. Ingest the permanent ledger states into the finalized bar mapping
	bar.Analytics.ContinuousVolumeIntensity = h.Ledger.ContinuousVolumeIntensity
	bar.Analytics.ContinuousPriceNormalized = h.Ledger.ContinuousPriceNormalized
	bar.Analytics.VwapClosePct = h.Ledger.VwapClosePct

	// 6. Commit out to persistent database pipeline
	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics provides real-time population of metrics for live UI streams without mutating history
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	// Process instant raw calculations on current forming tick states
	bae.CalculateContinuousAndStructuralMetrics(bar)

	// Projected look-ahead blend to feed the WebSocket without polluting the master history state
	decay := StandardDecayConstant
	bar.Analytics.ContinuousVolumeIntensity = (h.Ledger.ContinuousVolumeIntensity * decay) + bar.Analytics.VolumeIntensity
	bar.Analytics.ContinuousPriceNormalized = (h.Ledger.ContinuousPriceNormalized * decay) + bar.Analytics.PriceNormalizedChange

	// Map the stable historical percentage to the active visual streaming asset
	bar.Analytics.VwapClosePct = h.Ledger.VwapClosePct
}

// CalculateContinuousAndStructuralMetrics coordinates raw, isolated mathematical metrics for the asset bar
func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar) {
	// Re-rank thresholds based on statistical distributions loaded in memory
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 1. COMPUTE CONTINUOUS VOLUME INTENSITY
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

	// 2. COMPUTE PRICE NORMALIZED CHANGE (Using Existing DNA Interval Percentiles)
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

	// 3. COMPUTE STRUCTURAL RANK BLENDS
	bar.Analytics.Convergence = float64(bar.Analytics.VolumeRank+bar.Analytics.PriceRank) / 2.0
	bar.Analytics.Divergence = float64(bar.Analytics.VolumeRank-bar.Analytics.PriceRank) / 2.0
}
