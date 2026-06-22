package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"sync"
	"time"
)

const (
	DecayConstant         = 0.90
	TheoreticalMaxCeiling = 15.0 // Evaluated as Max Premium (1.5) / (1.0 - DecayConstant)
)

// ContinuousLivingLedger tracks un-netted structural order flow states
type ContinuousLivingLedger struct {
	// State 1: Pure Price Vector Balances
	BullPriceState float64
	BearPriceState float64

	// State 2: Pure Volume Vector Balances
	BullVolumeState float64
	BearVolumeState float64

	LastUpdated time.Time
}

type TimeframeAnalyticsHistory struct {
	Ledger            ContinuousLivingLedger
	TotalBars         int
	TimePctAboveVwap  float64
	RollingVolumeMean float64
}

type BarAnalyticsEngine struct {
	dnaMap   map[uint32]*models.MarketDNA
	profiles map[uint32]*models.InstrumentProfile
	history  map[string]map[string]*TimeframeAnalyticsHistory // stock -> timeframe -> history
	dbWriter *writer.DBWriter
	mu       sync.Mutex
}

func NewBarAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA, profiles map[uint32]*models.InstrumentProfile, dbW *writer.DBWriter) *BarAnalyticsEngine {
	return &BarAnalyticsEngine{
		dnaMap:   dnaMap,
		profiles: profiles,
		history:  make(map[string]map[string]*TimeframeAnalyticsHistory),
		dbWriter: dbW,
	}
}

// AnalyzeTick updates continuous peak transaction intensities and real-time distance metrics
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	bar.Analytics.NormalizedVwapDistance = bae.calculateDistance(bar.Close, bar.VWAP, uint32(bar.InstrumentToken))
	bae.computeMacroTimeframeRanksAndDirection(bar)
}

// AnalyzeClose processes the macro metrics ledger steps on bar expiration
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	bae.computeMacroTimeframeRanksAndDirection(bar)

	tf := bar.Timeframe
	h := bae.getOrInitTimeframeHistory(bar.StockName, tf)

	h.TotalBars++
	totalTfBars := float64(h.TotalBars)

	if h.RollingVolumeMean == 0 {
		h.RollingVolumeMean = bar.Volume
	} else {
		h.RollingVolumeMean = ((h.RollingVolumeMean * (totalTfBars - 1)) + bar.Volume) / totalTfBars
	}

	previousBarsAbove := (h.TimePctAboveVwap / 100.0) * (totalTfBars - 1.0)
	if bar.Close > bar.VWAP {
		previousBarsAbove += 1.0
	}
	h.TimePctAboveVwap = (previousBarsAbove / totalTfBars) * 100.0
	bar.Analytics.TimePctAboveVwap = h.TimePctAboveVwap

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 1: DECAY THE LIVING LEDGER BALANCES
	// -------------------------------------------------------------
	h.Ledger.BullPriceState *= DecayConstant
	h.Ledger.BearPriceState *= DecayConstant
	h.Ledger.BullVolumeState *= DecayConstant
	h.Ledger.BearVolumeState *= DecayConstant
	h.Ledger.LastUpdated = bar.Timestamp

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 2: METRIC EFFICIENCY (VIA SHARED HELPER)
	// -------------------------------------------------------------
	rawVolEff, rawPriceEff := bae.calculateProfileBlendedEfficiencies(bar)

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 3: DECOUPLED MATRIX INJECTIONS
	// -------------------------------------------------------------
	switch bar.Analytics.Direction {
	case models.DirStrongBullish:
		h.Ledger.BullVolumeState += 1.5 * rawVolEff
		h.Ledger.BullPriceState += 1.5 * rawPriceEff

	case models.DirBullish:
		h.Ledger.BullVolumeState += 1.0 * rawVolEff
		h.Ledger.BullPriceState += 1.0 * rawPriceEff

	case models.DirStrongBearish:
		h.Ledger.BearVolumeState += 1.5 * rawVolEff
		h.Ledger.BearPriceState += 1.5 * rawPriceEff

	case models.DirBearish:
		h.Ledger.BearVolumeState += 1.0 * rawVolEff
		h.Ledger.BearPriceState += 1.0 * rawPriceEff

	case models.DirBullishAbsorption:
		h.Ledger.BullVolumeState += 1.0 * rawVolEff
		h.Ledger.BearVolumeState += 0.5 * rawVolEff
		h.Ledger.BullPriceState += 1.0 * rawPriceEff

	case models.DirBearishAbsorption:
		h.Ledger.BearVolumeState += 1.0 * rawVolEff
		h.Ledger.BullVolumeState += 0.5 * rawVolEff
		h.Ledger.BearPriceState += 1.0 * rawPriceEff
	}

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 4: RESOLVE STRUCTURAL SCORES (-100 to +100)
	// -------------------------------------------------------------
	netPriceEff := bae.calculateNetEfficiency(h.Ledger.BullPriceState, h.Ledger.BearPriceState, TheoreticalMaxCeiling)
	netVolEff := bae.calculateNetEfficiency(h.Ledger.BullVolumeState, h.Ledger.BearVolumeState, TheoreticalMaxCeiling)

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 5: ASSIGN OUTBOUND DATA
	// -------------------------------------------------------------
	bar.Analytics.NetPriceMood = netPriceEff
	bar.Analytics.NetVolumeMood = netVolEff

	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics evaluates real-time, intermediate ranks, directions, and continuous moods
// for an active unclosed bar without modifying permanent historical analytics tracking history.
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	// 1. Calculate standard real-time percentile ranks and directions based on current state parameters
	bae.computeMacroTimeframeRanksAndDirection(bar)

	tf := bar.Timeframe
	h := bae.getOrInitTimeframeHistory(bar.StockName, tf)

	// 2. Compute a real-time estimation of TimePctAboveVwap
	totalTfBars := float64(h.TotalBars + 1)
	previousBarsAbove := (h.TimePctAboveVwap / 100.0) * float64(h.TotalBars)
	if bar.Close > bar.VWAP {
		previousBarsAbove += 1.0
	}
	bar.Analytics.TimePctAboveVwap = (previousBarsAbove / totalTfBars) * 100.0

	// 3. Extract efficiency parameters using the shared context calculation helper
	rawVolEff, rawPriceEff := bae.calculateProfileBlendedEfficiencies(bar)

	// -------------------------------------------------------------
	// LIVE ESTIMATION: SIMULATE LEDGER INJECTION ON DECAYED BLOCKS
	// -------------------------------------------------------------
	liveBullPrice := h.Ledger.BullPriceState * DecayConstant
	liveBearPrice := h.Ledger.BearPriceState * DecayConstant
	liveBullVol := h.Ledger.BullVolumeState * DecayConstant
	liveBearVol := h.Ledger.BearVolumeState * DecayConstant

	switch bar.Analytics.Direction {
	case models.DirStrongBullish:
		liveBullVol += 1.5 * rawVolEff
		liveBullPrice += 1.5 * rawPriceEff

	case models.DirBullish:
		liveBullVol += 1.0 * rawVolEff
		liveBullPrice += 1.0 * rawPriceEff

	case models.DirStrongBearish:
		liveBearVol += 1.5 * rawVolEff
		liveBearPrice += 1.5 * rawPriceEff

	case models.DirBearish:
		liveBearVol += 1.0 * rawVolEff
		liveBearPrice += 1.0 * rawPriceEff

	case models.DirBullishAbsorption:
		liveBullVol += 1.0 * rawVolEff
		liveBearVol += 0.5 * rawVolEff
		liveBullPrice += 1.0 * rawPriceEff

	case models.DirBearishAbsorption:
		liveBearVol += 1.0 * rawVolEff
		liveBullVol += 0.5 * rawVolEff
		liveBearPrice += 1.0 * rawPriceEff
	}

	// -------------------------------------------------------------
	// LIVE ESTIMATION: CALCULATE INTERMEDIATE OUTPUTS
	// -------------------------------------------------------------
	bar.Analytics.NetPriceMood = bae.calculateNetEfficiency(liveBullPrice, liveBearPrice, TheoreticalMaxCeiling)
	bar.Analytics.NetVolumeMood = bae.calculateNetEfficiency(liveBullVol, liveBearVol, TheoreticalMaxCeiling)
}
