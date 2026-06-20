package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"math"
	"sync"
	"time"
)

const (
	DecayConstant    = 0.90 // 🔄 Updated to 50% decay (faster decay/higher decay factor)
	StructuralMaxCap = 75.0
	MeanRevMaxCap    = 75.0
)

// EfficiencyLedger tracks structural buy/sell absorption metrics per timeframe
type ContinuousLivingLedger struct {
	// State 1: Price Vector Balances
	BullPriceState float64
	BearPriceState float64

	// State 2: Volume Vector Balances
	BullVolumeState float64
	BearVolumeState float64

	// State 3: Mean Reversion Vector Balance
	MeanReversionState float64

	// State 4: Absorption/Rejection Vector Balance
	AbsorptionState float64

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

// calculateNetEfficiency safely handles continuous scaling against a specified capacity baseline
func (bae *BarAnalyticsEngine) calculateNetEfficiency(bull, bear, maxCap float64) float64 {
	if maxCap <= 0 {
		maxCap = 1.0 // Prevent division by zero panic anomalies
	}

	netEff := ((bull - bear) / maxCap) * 100.0

	// Strict mathematical boundary capping
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
	// Construct a temporary window projection to include the live tracking point without mutating stored history
	projectedSeries := make([]float64, len(history), len(history)+1)
	copy(projectedSeries, history)
	projectedSeries = append(projectedSeries, livePoint)

	if len(projectedSeries) > lookbackLimit {
		projectedSeries = projectedSeries[1:]
	}

	return bae.calculateLinearRegressionSlope(projectedSeries)
}

// GetLiveSnapshot safely exposes current real-time continuous mathematical states for live streaming egress/UI
func (bae *BarAnalyticsEngine) GetLiveSnapshot(stockName string, timeframe string) (netPriceEff float64, netVolEff float64, meanRev float64, absForce float64) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	// Guard condition: If history doesn't exist yet, return neutral standing states
	if bae.history[stockName] == nil || bae.history[stockName][timeframe] == nil {
		return 0.0, 0.0, 0.0, 0.0
	}

	h := bae.history[stockName][timeframe]

	// -------------------------------------------------------------
	// RESOLVE CURRENT REAL-TIME STANDARDIZED SCORES
	// -------------------------------------------------------------
	netPriceEff = bae.calculateNetEfficiency(h.Ledger.BullPriceState, h.Ledger.BearPriceState, StructuralMaxCap)
	netVolEff = bae.calculateNetEfficiency(h.Ledger.BullVolumeState, h.Ledger.BearVolumeState, StructuralMaxCap)
	meanRev = bae.calculateNetEfficiency(h.Ledger.MeanReversionState, 0.0, MeanRevMaxCap)

	absForce = (h.Ledger.AbsorptionState / 400.0) * 100.0
	if absForce > 100.0 {
		absForce = 100.0
	} else if absForce < 0.0 {
		absForce = 0.0
	}

	return netPriceEff, netVolEff, meanRev, absForce
}

// AnalyzeTick updates continuous peak transaction intensities and real-time distance metrics
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	// TICK LEVEL: Continuous distance evaluation
	bar.Analytics.NormalizedVwapDistance = bae.calculateDistance(bar.Close, bar.VWAP, uint32(bar.InstrumentToken))

	bae.computeMacroTimeframeRanksAndDirection(bar)
}

// AnalyzeClose isolates features, runs momentum ledgers, constructs regressions, and triggers DB writing
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	bae.computeMacroTimeframeRanksAndDirection(bar)

	tf := bar.Timeframe
	h := bae.getOrInitTimeframeHistory(bar.StockName, tf)

	// VWAP Basic Tracker maintenance
	h.TotalBars++
	totalTfBars := float64(h.TotalBars)

	if h.RollingVolumeMean == 0 {
		h.RollingVolumeMean = bar.Volume
	} else {
		// Simple running moving average update
		h.RollingVolumeMean = ((h.RollingVolumeMean * (totalTfBars - 1)) + bar.Volume) / totalTfBars
	}

	volumeStrength := 1.0
	if h.RollingVolumeMean > 0 {
		volumeStrength = bar.Volume / h.RollingVolumeMean
	}
	bar.Analytics.VolumeStrength = volumeStrength

	previousBarsAbove := (h.TimePctAboveVwap / 100.0) * (totalTfBars - 1.0)
	if bar.Close > bar.VWAP {
		previousBarsAbove += 1.0
	}
	h.TimePctAboveVwap = (previousBarsAbove / totalTfBars) * 100.0
	bar.Analytics.TimePctAboveVwap = h.TimePctAboveVwap

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 1: DECAY THE LIVING LEDGER
	// -------------------------------------------------------------
	h.Ledger.BullPriceState *= DecayConstant
	h.Ledger.BearPriceState *= DecayConstant
	h.Ledger.BullVolumeState *= DecayConstant
	h.Ledger.BearVolumeState *= DecayConstant
	h.Ledger.MeanReversionState *= DecayConstant
	h.Ledger.AbsorptionState *= DecayConstant
	h.Ledger.LastUpdated = bar.Timestamp

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 2: CALCULATE INSTANTANEOUS INPUT PULSES
	// -------------------------------------------------------------
	candleRange := bar.High - bar.Low
	bodyToRangeRatio := 0.0
	upperWickRatio := 0.0
	lowerWickRatio := 0.0

	if candleRange > 0 {
		bodyToRangeRatio = math.Abs(bar.Close-bar.Open) / candleRange
		upperWickRatio = (bar.High - math.Max(bar.Open, bar.Close)) / candleRange
		lowerWickRatio = (math.Min(bar.Open, bar.Close) - bar.Low) / candleRange
	}

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 3: INJECT KINETIC PULSES WITH NON-LINEAR WEIGHTS
	// -------------------------------------------------------------
	priceWeight := bae.getRankWeight(bar.Analytics.PriceRank) * bodyToRangeRatio
	volumeWeight := bae.getRankWeight(bar.Analytics.VolumeRank) * volumeStrength

	dir := bar.Analytics.Direction
	switch {
	// 🟢 Standard Bullish Trend: Add strictly to the Bull vectors
	case dir == models.DirStrongBullish || dir == models.DirBullish:
		h.Ledger.BullPriceState += priceWeight
		h.Ledger.BullVolumeState += volumeWeight
		h.Ledger.MeanReversionState += priceWeight
		h.Ledger.AbsorptionState += upperWickRatio * 100.0

	// 🔴 Standard Bearish Trend: Add strictly to the Bear vectors
	case dir == models.DirStrongBearish || dir == models.DirBearish:
		h.Ledger.BearPriceState += priceWeight
		h.Ledger.BearVolumeState += volumeWeight
		h.Ledger.MeanReversionState -= priceWeight
		h.Ledger.AbsorptionState += lowerWickRatio * 100.0

	// 🩵 BULLISH ABSORPTION: Iceberg selling halts upward price progress
	// Action: No efficiency is added. Drop price and volume contributions completely.
	case dir == models.DirBullishAbsorption:
		h.Ledger.AbsorptionState += upperWickRatio * 100.0

	// 🩷 BEARISH ABSORPTION: Iceberg buying halts downward price progress
	// Action: No efficiency is added. Drop price and volume contributions completely.
	case dir == models.DirBearishAbsorption:
		h.Ledger.AbsorptionState += lowerWickRatio * 100.0

	default: // True Sideways / Neutral Dojis
		h.Ledger.AbsorptionState += ((upperWickRatio + lowerWickRatio) * 0.5) * 100.0
	}

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 4: RESOLVE STRUCTURAL SCORES (-100 to +100)
	// -------------------------------------------------------------
	netPriceEff := bae.calculateNetEfficiency(h.Ledger.BullPriceState, h.Ledger.BearPriceState, StructuralMaxCap)
	netVolEff := bae.calculateNetEfficiency(h.Ledger.BullVolumeState, h.Ledger.BearVolumeState, StructuralMaxCap)
	meanRevPressure := bae.calculateNetEfficiency(h.Ledger.MeanReversionState, 0.0, MeanRevMaxCap)

	absorptionForce := (h.Ledger.AbsorptionState / 400.0) * 100.0
	if absorptionForce > 100.0 {
		absorptionForce = 100.0
	} else if absorptionForce < 0.0 {
		absorptionForce = 0.0
	}

	// -------------------------------------------------------------
	// CONTINUOUS STATE STEP 5: OUTBOUND ASSIGNMENT
	// -------------------------------------------------------------
	bar.Analytics.NetPriceEfficiency = netPriceEff
	bar.Analytics.NetVolumeEfficiency = netVolEff
	bar.Analytics.MeanReversionPressure = meanRevPressure
	bar.Analytics.AbsorptionForce = absorptionForce

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

	if bar.Analytics.VolumeRank >= 7 && bar.Analytics.PriceRank <= 4 {
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

// getRankWeight Helper method to resolve exponential breakout mapping parameters
func (bae *BarAnalyticsEngine) getRankWeight(rank int) float64 {
	switch rank {
	case 7:
		return 15.0 // 🚀 P97 Extreme Volume Breakout / Shock Block
	case 6:
		return 10.0 // 🔥 P90 Strong Momentum Velocity Expansion
	case 5:
		return 8.0 // P75 Above Average Flow
	case 4:
		return 2.0 // P50 Median Line Baseline
	case 3:
		return 1.0 // P25 Mild Flow Contraction
	case 2:
		return 0.5 // P10 Deep Value Compression
	default:
		return 0.1 // Negligible Structural Noise
	}
}
