package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"math"
	"sync"
	"time"
)

const (
	EfficiencySlopeLookback = 10
	DecayConstant           = 0.90
)

// EfficiencyLedger tracks structural buy/sell absorption metrics per timeframe
type EfficiencyLedger struct {
	BullEfficient float64
	BearEfficient float64
	LastUpdated   time.Time
}

// TimeframeAnalyticsHistory isolates historical indicators and tracking memory per timeframe
type TimeframeAnalyticsHistory struct {
	Ledger               EfficiencyLedger
	NetEfficiencyHistory []float64
	TotalBars            int
	TimePctAboveVwap     float64
}

type BarAnalyticsEngine struct {
	dnaMap   map[uint32]*models.MarketDNA
	history  map[string]map[string]*TimeframeAnalyticsHistory // stock -> timeframe -> history
	dbWriter *writer.DBWriter
	mu       sync.Mutex
}

func NewBarAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA, dbW *writer.DBWriter) *BarAnalyticsEngine {
	return &BarAnalyticsEngine{
		dnaMap:   dnaMap,
		history:  make(map[string]map[string]*TimeframeAnalyticsHistory),
		dbWriter: dbW,
	}
}

// getOrInitTimeframeHistory guarantees deep metric isolation per stock and timeframe safely
func (bae *BarAnalyticsEngine) getOrInitTimeframeHistory(stockName string, timeframe string) *TimeframeAnalyticsHistory {
	if bae.history[stockName] == nil {
		bae.history[stockName] = make(map[string]*TimeframeAnalyticsHistory)
	}

	if h, exists := bae.history[stockName][timeframe]; exists {
		return h
	}

	h := &TimeframeAnalyticsHistory{
		NetEfficiencyHistory: make([]float64, 0),
		TotalBars:            0,
		TimePctAboveVwap:     0.0,
	}
	bae.history[stockName][timeframe] = h
	return h
}

// GetLiveSnapshot safely exposes current real-time mathematical states for live streaming egress/UI
func (bae *BarAnalyticsEngine) GetLiveSnapshot(stockName string, timeframe string) (netEff float64, slope float64) {
	bae.mu.Lock()
	defer bae.mu.Unlock()

	if bae.history[stockName] == nil || bae.history[stockName][timeframe] == nil {
		return 0.0, 0.0
	}

	h := bae.history[stockName][timeframe]
	netEff = h.Ledger.BullEfficient - h.Ledger.BearEfficient
	slope = bae.calculateLinearRegressionSlope(h.NetEfficiencyHistory)
	return netEff, slope
}

// AnalyzeTick updates the candle analytics layer on every single live streaming tick update
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	// 1. Accumulate PEAK Intensities for cumulative transaction metrics
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	// 2. Calculate the specific bar's internal tracking context (Scaled 0-100)
	if bar.VWAP > 0 && bar.TickCount > 0 {
		previousTotalTicks := float64(bar.TickCount - 1)
		previousTicksAbove := (bar.Analytics.TimePctAboveVwap / 100.0) * previousTotalTicks

		if tick.Raw.LastPrice > bar.VWAP {
			previousTicksAbove++
		}

		bar.Analytics.TimePctAboveVwap = (previousTicksAbove / float64(bar.TickCount)) * 100.0
	}

	bae.computeMacroTimeframeRanksAndDirection(bar)
}

// AnalyzeClose isolates features, runs momentum ledgers, constructs regressions, and triggers DB writing
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {
	bae.mu.Lock()

	// 1. Calculate macro baseline ranks and direction classification
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 2. Fetch the isolated timeframe tracking state history
	tf := bar.Timeframe
	h := bae.getOrInitTimeframeHistory(bar.StockName, tf)

	// 3. Update isolated time percentages above VWAP
	h.TotalBars++
	totalTfBars := float64(h.TotalBars)

	previousBarsAbove := (h.TimePctAboveVwap / 100.0) * (totalTfBars - 1.0)
	if bar.Close > bar.VWAP {
		previousBarsAbove += 1.0
	}
	h.TimePctAboveVwap = (previousBarsAbove / totalTfBars) * 100.0
	bar.Analytics.TimePctAboveVwap = h.TimePctAboveVwap

	// 4. TIMEFRAME ISOLATED MOMENTUM LEDGER PROCESSING
	h.Ledger.BullEfficient *= DecayConstant
	h.Ledger.BearEfficient *= DecayConstant
	h.Ledger.LastUpdated = bar.Timestamp

	energy := float64(bar.Analytics.VolumeRank * bar.Analytics.PriceRank)
	delta := bar.Analytics.PriceRank - bar.Analytics.VolumeRank

	switch bar.Analytics.Direction {
	case models.DirStrongBullish, models.DirBullish:
		if math.Abs(float64(delta)) <= 1 {
			h.Ledger.BullEfficient += energy
		} else {
			h.Ledger.BullEfficient += energy * 0.5
		}
	case models.DirStrongBearish, models.DirBearish:
		if math.Abs(float64(delta)) <= 1 {
			h.Ledger.BearEfficient += energy
		} else {
			h.Ledger.BearEfficient += energy * 0.5
		}
	}

	// 5. Calculate Linear Regression Slope for this specific timeframe history slice
	netEff := h.Ledger.BullEfficient - h.Ledger.BearEfficient
	h.NetEfficiencyHistory = append(h.NetEfficiencyHistory, netEff)

	if len(h.NetEfficiencyHistory) > EfficiencySlopeLookback {
		h.NetEfficiencyHistory = h.NetEfficiencyHistory[1:]
	}

	// Stamp structural assignments cleanly onto the outbound bar object frame
	bar.Analytics.NetEfficiency = netEff
	bar.Analytics.NetEfficiencySlope = bae.calculateLinearRegressionSlope(h.NetEfficiencyHistory)
	bae.mu.Unlock()

	// 6. Automatically pass through to Database Writer Archive Layer
	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}

	// NOTE: Your streaming/WebSocket broadcast server hook can be fired directly from here as well:
	// ws.BroadcastBarUpdate(bar)
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
