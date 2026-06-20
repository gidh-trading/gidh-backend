package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"math"
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

func (bae *BarAnalyticsEngine) getOrInitTimeframeHistory(stockName string, timeframe string) *TimeframeAnalyticsHistory {
	if bae.history[stockName] == nil {
		bae.history[stockName] = make(map[string]*TimeframeAnalyticsHistory)
	}

	if h, exists := bae.history[stockName][timeframe]; exists {
		return h
	}

	h := &TimeframeAnalyticsHistory{
		TotalBars:        0,
		TimePctAboveVwap: 0.0,
	}
	bae.history[stockName][timeframe] = h
	return h
}

// calculateNetEfficiency scales structural states cleanly relative to a maximum ceiling limit
func (bae *BarAnalyticsEngine) calculateNetEfficiency(bull, bear, maxCap float64) float64 {
	if maxCap <= 0 {
		maxCap = 1.0
	}

	netEff := ((bull - bear) / maxCap) * 100.0

	// Safe mathematical protection boundaries
	if netEff > 100.0 {
		return 100.0
	}
	if netEff < -100.0 {
		return -100.0
	}
	return netEff
}

// 📐 CORE EXTRACTED MATHEMATICAL FUNCTION 2: Trailing Linear Regression Slope Resolver
func (bae *BarAnalyticsEngine) calculateTrajectorySlope(history []float64, livePoint float64, lookbackLimit int) float64 {
	projectedSeries := make([]float64, len(history), len(history)+1)
	copy(projectedSeries, history)
	projectedSeries = append(projectedSeries, livePoint)

	if len(projectedSeries) > lookbackLimit {
		projectedSeries = projectedSeries[1:]
	}

	return bae.calculateLinearRegressionSlope(projectedSeries)
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
	// CONTINUOUS STATE STEP 2: INSTANTANEOUS NORMALIZED METRIC CONSTANTS
	// -------------------------------------------------------------
	// Un-netted baseline efficiency ratios normalized out of peak ranking 7
	rawVolEff := float64(bar.Analytics.VolumeRank) / 7.0
	rawPriceEff := float64(bar.Analytics.PriceRank) / 7.0

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 3: DECOUPLED MATRIX INJECTIONS (NO CANCELLATION)
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
		// Passive Buyer Tracking: Credit full weight to bull vol, 0.5 premium to bear vol, retain price space
		h.Ledger.BullVolumeState += 1.0 * rawVolEff
		h.Ledger.BearVolumeState += 0.5 * rawVolEff
		h.Ledger.BullPriceState += 1.0 * rawPriceEff

	case models.DirBearishAbsorption:
		// Passive Seller Tracking: Credit full weight to bear vol, 0.5 premium to bull vol, retain price space
		h.Ledger.BearVolumeState += 1.0 * rawVolEff
		h.Ledger.BullVolumeState += 0.5 * rawVolEff
		h.Ledger.BearPriceState += 1.0 * rawPriceEff

	case models.DirSideways:
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

func (bae *BarAnalyticsEngine) computeMacroTimeframeRanksAndDirection(bar *models.Bar) {
	token := uint32(bar.InstrumentToken)
	dna, exists := bae.dnaMap[token]
	if !exists || dna == nil || dna.IntervalPercentiles == nil {
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		bar.Analytics.Direction = models.DirSideways
		return
	}

	baseline, hasTimeframeBaseline := dna.IntervalPercentiles[bar.Timeframe]
	if !hasTimeframeBaseline {
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		bar.Analytics.Direction = models.DirSideways
		return
	}

	candleBody := math.Abs(bar.Close - bar.Open)
	candleRange := bar.High - bar.Low

	switch {
	case candleBody >= baseline.PriceP97:
		bar.Analytics.PriceRank = 7
	case candleBody >= baseline.PriceP90:
		bar.Analytics.PriceRank = 6
	case candleBody >= baseline.PriceP75:
		bar.Analytics.PriceRank = 5
	case candleBody >= baseline.PriceP50:
		bar.Analytics.PriceRank = 4
	case candleBody >= baseline.PriceP25:
		bar.Analytics.PriceRank = 3
	case candleBody >= baseline.PriceP10:
		bar.Analytics.PriceRank = 2
	default:
		bar.Analytics.PriceRank = 1
	}

	switch {
	case candleRange >= baseline.RangeP97:
		bar.Analytics.RangeRank = 7
	case candleRange >= baseline.RangeP90:
		bar.Analytics.RangeRank = 6
	case candleRange >= baseline.RangeP75:
		bar.Analytics.RangeRank = 5
	case candleRange >= baseline.RangeP50:
		bar.Analytics.RangeRank = 4
	case candleRange >= baseline.RangeP25:
		bar.Analytics.RangeRank = 3
	case candleRange >= baseline.RangeP10:
		bar.Analytics.RangeRank = 2
	default:
		bar.Analytics.RangeRank = 1
	}

	if candleRange <= 0 {
		bar.Analytics.Direction = models.DirSideways
		return
	}

	candleBodyTop := math.Max(bar.Open, bar.Close)
	candleBodyBottom := math.Min(bar.Open, bar.Close)

	upperWick := bar.High - candleBodyTop
	lowerWick := candleBodyBottom - bar.Low

	upperWickRatio := upperWick / candleRange
	lowerWickRatio := lowerWick / candleRange

	positionRatio := (bar.Close - bar.Low) / candleRange
	isHigherThanOpen := bar.Close > bar.Open
	isLowerThanOpen := bar.Close < bar.Open

	bar.Analytics.UpperWickRank = bae.getWickRank(upperWickRatio)
	bar.Analytics.LowerWickRank = bae.getWickRank(lowerWickRatio)

	if bar.Analytics.VolumeRank >= 6 && bar.Analytics.PriceRank <= 4 {
		if positionRatio >= 0.50 {
			bar.Analytics.Direction = models.DirBullishAbsorption
			return
		} else {
			bar.Analytics.Direction = models.DirBearishAbsorption
			return
		}
	}

	switch {
	case positionRatio >= 0.85 && isHigherThanOpen:
		bar.Analytics.Direction = models.DirStrongBullish
	case positionRatio > 0.55 && isHigherThanOpen:
		bar.Analytics.Direction = models.DirBullish
	case positionRatio <= 0.15 && isLowerThanOpen:
		bar.Analytics.Direction = models.DirStrongBearish
	case positionRatio < 0.45 && isLowerThanOpen:
		bar.Analytics.Direction = models.DirBearish
	default:
		bar.Analytics.Direction = models.DirSideways
	}
}

func (bae *BarAnalyticsEngine) getWickRank(ratio float64) int {
	switch {
	case ratio >= 0.45:
		return 7
	case ratio >= 0.35:
		return 6
	case ratio >= 0.25:
		return 5
	case ratio >= 0.18:
		return 4
	case ratio >= 0.12:
		return 3
	case ratio >= 0.05:
		return 2
	default:
		return 1
	}
}

func (bae *BarAnalyticsEngine) calculateLinearRegressionSlope(series []float64) float64 {
	n := float64(len(series))
	if n < 2 {
		return 0.0
	}
	var sumX, sumY, sumXY, sumXX float64
	for i, y := range series {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	denominator := (n * sumXX) - (sumX * sumX)
	if denominator == 0 {
		return 0.0
	}
	return ((n * sumXY) - (sumX * sumY)) / denominator
}

func (bae *BarAnalyticsEngine) calculateDistance(price, vwap float64, token uint32) float64 {
	if vwap <= 0 {
		return 0.0
	}
	rawPct := ((price - vwap) / vwap) * 100.0
	if profile, ok := bae.profiles[token]; ok && profile != nil && profile.ADRPct > 0 {
		return rawPct / profile.ADRPct
	}
	return rawPct
}

func (bae *BarAnalyticsEngine) getSlopeLookback(timeframe string) int {
	return 5
}

// getRankWeight Helper method for underlying weighting distributions if required elsewhere
func (bae *BarAnalyticsEngine) getRankWeight(rank int) float64 {
	switch rank {
	case 7:
		return 15.0
	case 6:
		return 10.0
	case 5:
		return 8.0
	case 4:
		return 2.0
	case 3:
		return 1.0
	case 2:
		return 0.5
	default:
		return 0.1
	}
}
