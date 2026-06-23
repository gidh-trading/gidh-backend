package pipeline

import (
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	StandardDecayConstant   = 0.80  // Baseline decay rate for trend bars
	AbsorptionDecayConstant = 0.60  // Accelerated decay to forget past states faster during absorption
	TheoreticalMaxCeiling   = 5.0   // 1.0 / (1.0 - 0.80)
	ExpectedBarsPerSession  = 390.0 // Standard 6.5 hour equity session baseline (e.g., US Equities)
)

// ContinuousLivingLedger tracks un-netted structural order flow states and compounding active heatmaps
type ContinuousLivingLedger struct {
	LastUpdated               time.Time
	ContinuousVolumeIntensity float64 // Smooth, compounding accumulation of market volume pressure
	ContinuousPriceNormalized float64 // Smooth, compounding accumulation of directional momentum
}

type TimeframeAnalyticsHistory struct {
	Ledger            ContinuousLivingLedger
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

// AnalyzeTick updates continuous peak transaction intensities and real-time distance metrics
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
	// 1. Calculate structural ranks, direction boundaries, and current bar metrics
	bae.CalculateContinuousAndStructuralMetrics(bar)

	// Increment total bars tracked in history
	h.TotalBars++

	// 2. Advance the state of the Continuous Living Ledger using the compounding formula
	decay := StandardDecayConstant
	h.Ledger.ContinuousVolumeIntensity = (h.Ledger.ContinuousVolumeIntensity * decay) + bar.Analytics.VolumeIntensity
	h.Ledger.ContinuousPriceNormalized = (h.Ledger.ContinuousPriceNormalized * decay) + bar.Analytics.PriceNormalizedChange
	h.Ledger.LastUpdated = bar.Timestamp

	// 3. Bind the running state to the finalized bar container for down-stream applications and persistence
	bar.Analytics.ContinuousVolumeIntensity = h.Ledger.ContinuousVolumeIntensity
	bar.Analytics.ContinuousPriceNormalized = h.Ledger.ContinuousPriceNormalized

	// 4. Persist bar to DB
	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics provides real-time population of metrics for live UI streams without mutating history
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	// Calculate base metrics on current raw forming bar state
	bae.CalculateContinuousAndStructuralMetrics(bar)

	// Projected look-ahead calculation for real-time visualization (does not modify the ledger struct state)
	decay := StandardDecayConstant
	bar.Analytics.ContinuousVolumeIntensity = (h.Ledger.ContinuousVolumeIntensity * decay) + bar.Analytics.VolumeIntensity
	bar.Analytics.ContinuousPriceNormalized = (h.Ledger.ContinuousPriceNormalized * decay) + bar.Analytics.PriceNormalizedChange
}

// CalculateContinuousAndStructuralMetrics maps continuous values for the heatmap and processes matrix blends
func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar) {
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 1. COMPUTE CONTINUOUS VOLUME INTENSITY (Dynamically scales by Timeframe)
	var volumeIntensity float64 = 0.0
	profile, exists := bae.profiles[uint32(bar.InstrumentToken)]
	if exists && profile.ADV30d > 0 {
		expectedBars := bae.getExpectedBarsForTimeframe(bar.Timeframe)
		expectedBarVolumeBaseline := float64(profile.ADV30d) / expectedBars
		volumeIntensity = float64(bar.Volume) / expectedBarVolumeBaseline
	}
	bar.Analytics.VolumeIntensity = volumeIntensity

	// 2. COMPUTE PRICE NORMALIZED CHANGE (-1.0 to +1.0)
	bar.Analytics.PriceNormalizedChange = (float64(bar.Analytics.PriceRank) - 4.0) / 3.0

	// 3. COMPUTE STRUCTURAL RANK BLENDS
	bar.Analytics.Convergence = float64(bar.Analytics.VolumeRank+bar.Analytics.PriceRank) / 2.0
	bar.Analytics.Divergence = float64(bar.Analytics.VolumeRank-bar.Analytics.PriceRank) / 2.0
}
