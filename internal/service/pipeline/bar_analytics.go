package pipeline

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

// ContinuousLivingLedger tracks un-netted structural order flow states
type ContinuousLivingLedger struct {
	LastUpdated  time.Time
	VwapClosePct float64
}

type TrackedAnchor struct {
	IsActive  bool
	CumPV     float64 // Cumulative Price * Volume
	CumVolume float64 // Cumulative Volume
}

type TimeframeAnalyticsHistory struct {
	BarsAboveVwap      int
	TotalBars          int
	CurrentSessionHigh float64
	CurrentSessionLow  float64

	// Stateful extensions for VWAP metrics
	VWAPHistory []float64

	// The 4 Dynamic Anchored VWAP trackers
	AnchorADRHigh TrackedAnchor
	AnchorADRLow  TrackedAnchor
	AnchorDistGt  TrackedAnchor // Distance >= 0.5%
	AnchorDistLt  TrackedAnchor // Distance < 0.5%
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
	bae.CalculateContinuousAndStructuralMetrics(bar, h, false) // false = intermediate snapshot
}

// AnalyzeClose processes metrics at the close and writes to database
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bae.CalculateContinuousAndStructuralMetrics(bar, h, true) // true = finalize and commit state

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
	bae.CalculateContinuousAndStructuralMetrics(bar, h, false)

	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}
}

func (bae *BarAnalyticsEngine) CalculateContinuousAndStructuralMetrics(bar *models.Bar, h *TimeframeAnalyticsHistory, isBarClose bool) {
	token := uint32(bar.InstrumentToken)

	// 1. Initialize or update running session high/low bounds
	if h.CurrentSessionHigh == 0 || bar.High > h.CurrentSessionHigh {
		h.CurrentSessionHigh = bar.High
	}
	if h.CurrentSessionLow == 0 || bar.Low < h.CurrentSessionLow {
		h.CurrentSessionLow = bar.Low
	}

	// 2. Fetch stock profile and construct structural ADR boundaries
	var adrPoints float64 = 0.0
	if profile, ok := bae.profiles[token]; ok && profile != nil && profile.ADRPct > 0 {
		adrPoints = bar.Open * (profile.ADRPct / 100.0)
		bar.Analytics.ADRHigh = h.CurrentSessionLow + adrPoints
		bar.Analytics.ADRLow = h.CurrentSessionHigh - adrPoints
	}

	// -------------------------------------------------------------
	// 3. SIMPLIFIED NORMALIZED VWAP CHANGE (NO RANK)
	// -------------------------------------------------------------
	var normalizedVwapChange float64 = 0.0

	if len(h.VWAPHistory) > 0 && adrPoints > 0 {
		prevVWAP := h.VWAPHistory[len(h.VWAPHistory)-1]

		// 1. Calculate raw change relative to ADR
		rawRatio := (bar.VWAP - prevVWAP) / adrPoints

		// 2. Amplify by 10,000 to convert into clean Basis Points (bps) of ADR
		normalizedVwapChange = rawRatio * 10000.0
	}

	// Store the clean, scaled value directly (e.g., 21.90 instead of 0.00219)
	bar.Analytics.VWAPSlope = normalizedVwapChange

	// Append to history ONLY when the bar is fully closing
	if isBarClose {
		h.VWAPHistory = append(h.VWAPHistory, bar.VWAP)
		if len(h.VWAPHistory) > 5 {
			h.VWAPHistory = h.VWAPHistory[1:]
		}
	}

	// -------------------------------------------------------------
	// 4. ANCHORED VWAP STATE ENGINE (Keeps the -7 to 7 ranks for heatmap)
	// -------------------------------------------------------------
	var currentVwapDistPct float64 = 0.0
	if bar.VWAP > 0 {
		currentVwapDistPct = (math.Abs(bar.Close-bar.VWAP) / bar.VWAP) * 100.0
	}

	// Check dynamic trigger thresholds
	if bar.High >= bar.Analytics.ADRHigh {
		h.AnchorADRHigh = TrackedAnchor{IsActive: true}
	}
	if bar.Low <= bar.Analytics.ADRLow {
		h.AnchorADRLow = TrackedAnchor{IsActive: true}
	}
	if currentVwapDistPct >= 0.5 {
		h.AnchorDistGt = TrackedAnchor{IsActive: true}
	}
	if currentVwapDistPct < 0.5 {
		h.AnchorDistLt = TrackedAnchor{IsActive: true}
	}

	// Accumulate context parameters securely on close
	if isBarClose {
		if h.AnchorADRHigh.IsActive {
			h.AnchorADRHigh.CumPV += bar.Close * bar.Volume
			h.AnchorADRHigh.CumVolume += bar.Volume
		}
		if h.AnchorADRLow.IsActive {
			h.AnchorADRLow.CumPV += bar.Close * bar.Volume
			h.AnchorADRLow.CumVolume += bar.Volume
		}
		if h.AnchorDistGt.IsActive {
			h.AnchorDistGt.CumPV += bar.Close * bar.Volume
			h.AnchorDistGt.CumVolume += bar.Volume
		}
		if h.AnchorDistLt.IsActive {
			h.AnchorDistLt.CumPV += bar.Close * bar.Volume
			h.AnchorDistLt.CumVolume += bar.Volume
		}
	}

	// Process dynamic snapshot targets for visual layers
	bar.Analytics.AnchorADRHighRank = bae.computeAnchorRank(&h.AnchorADRHigh, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorADRLowRank = bae.computeAnchorRank(&h.AnchorADRLow, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistHighRank = bae.computeAnchorRank(&h.AnchorDistGt, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistLowRank = bae.computeAnchorRank(&h.AnchorDistLt, bar.Close, bar.Volume, adrPoints)

	// 5. Evaluate all macro metrics and ranks
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 6. Apply Directional Polarized Sign to Price and Efficiency Spectrum
	if bar.Close < bar.Open {
		if bar.Analytics.PriceRank > 0 {
			bar.Analytics.PriceRank = -bar.Analytics.PriceRank
		}

	}
}
