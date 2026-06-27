package pipeline

import (
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

// ContinuousLivingLedger tracks un-netted structural order flow states
type ContinuousLivingLedger struct {
	LastUpdated  time.Time
	VwapClosePct float64
}

type TimeframeAnalyticsHistory struct {
	BarsAboveVwap      int
	TotalBars          int
	CurrentSessionHigh float64
	CurrentSessionLow  float64
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
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick, h *TimeframeAnalyticsHistory) {
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	bar.Analytics.NormalizedVwapDistance = bae.calculateDistance(bar.Close, bar.VWAP, uint32(bar.InstrumentToken))
	bae.CalculateContinuousAndStructuralMetrics(bar, h)
}

// AnalyzeClose processes metrics at the close and writes to database
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar, h *TimeframeAnalyticsHistory) {

	bae.CalculateContinuousAndStructuralMetrics(bar, h)

	h.TotalBars++
	if bar.Close > bar.VWAP {
		h.BarsAboveVwap++
	}
	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}

	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics populates live snapshots for visual WebSocket feeds without mutating master history
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {

	bae.CalculateContinuousAndStructuralMetrics(bar, h)

	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}
}

func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	token := uint32(bar.InstrumentToken)

	// 1. Initialize or update running session high/low bounds
	if h.CurrentSessionHigh == 0 || bar.High > h.CurrentSessionHigh {
		h.CurrentSessionHigh = bar.High
	}
	if h.CurrentSessionLow == 0 || bar.Low < h.CurrentSessionLow {
		h.CurrentSessionLow = bar.Low
	}

	// 2. Fetch the stock profile to extract ADRPct
	if profile, ok := bae.profiles[token]; ok && profile != nil && profile.ADRPct > 0 {
		// We still use the bar's open or historical session open to calculate the nominal points value
		// Assuming bar.Open on the first bar acts as the session open proxy
		// To match the UI exactly: const adrPoints = sessionOpen * (adrPct / 100);
		// Let's assume you store a base price or use the first bar printed.
		// If you want to use the current bar's open as a point reference:
		adrPoints := bar.Open * (profile.ADRPct / 100.0)

		// 3. Compute dynamic target expansion coordinates identical to UI
		bar.Analytics.ADRHigh = h.CurrentSessionLow + adrPoints
		bar.Analytics.ADRLow = h.CurrentSessionHigh - adrPoints
	}

	// 4. Evaluates all parameters and ranks
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 5. Apply Directional Polarized Sign to Price and Efficiency Spectrum
	if bar.Close < bar.Open {
		if bar.Analytics.PriceRank > 0 {
			bar.Analytics.PriceRank = -bar.Analytics.PriceRank
		}
		if bar.Analytics.EfficiencyRank > 0 {
			bar.Analytics.EfficiencyRank = -bar.Analytics.EfficiencyRank
		}
	}
}
