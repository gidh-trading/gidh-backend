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
	// 3. NATURAL BOUNDED VWAP VELOCITY INDICATOR (-1 to 1)
	// -------------------------------------------------------------
	var boundedVwapSlope float64 = 0.0

	if len(h.VWAPHistory) > 0 && adrPoints > 0 {
		prevVWAP := h.VWAPHistory[len(h.VWAPHistory)-1]

		// 1. Calculate raw change relative to ADR
		rawRatio := (bar.VWAP - prevVWAP) / adrPoints

		// 2. Squash into a natural (-1, 1) range using hyperbolic tangent (tanh).
		// A scaling factor of 20.0 means a VWAP shift equal to 5% of your ADR
		// pushes the indicator to ~0.76, while a 10% ADR shift pushes it to ~0.96.
		const scalingFactor = 20.0
		boundedVwapSlope = math.Tanh(rawRatio * scalingFactor)
	}

	// Overwrite the old unbounded slope field with the clean, squashed metric
	bar.Analytics.VWAPSlope = boundedVwapSlope

	// Append to history ONLY when the bar is fully closing
	if isBarClose {
		h.VWAPHistory = append(h.VWAPHistory, bar.VWAP)
		if len(h.VWAPHistory) > 5 {
			h.VWAPHistory = h.VWAPHistory[1:]
		}
	}

	// -------------------------------------------------------------
	// 4. ANCHORED VWAP STATE ENGINE (Updated to Continuous Tanh)
	// -------------------------------------------------------------

	// ✅ FIX: Only initialize if the anchor is NOT already active to prevent volume resetting
	if bar.High >= bar.Analytics.ADRHigh && !h.AnchorADRHigh.IsActive {
		h.AnchorADRHigh = TrackedAnchor{IsActive: true}
	}
	if bar.Low <= bar.Analytics.ADRLow && !h.AnchorADRLow.IsActive {
		h.AnchorADRLow = TrackedAnchor{IsActive: true}
	}
	if bar.Analytics.NormalizedVwapDistance >= 0.5 && !h.AnchorDistGt.IsActive {
		h.AnchorDistGt = TrackedAnchor{IsActive: true}
	}
	if bar.Analytics.NormalizedVwapDistance <= -0.5 && !h.AnchorDistLt.IsActive {
		h.AnchorDistLt.IsActive = true // Directly flipping state flags to preserve continuity
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

	// 🚀 CALCULATE CONTINUOUS DISTANCES (-1.0 to 1.0) instead of integer ranks
	// Note: If your models.Bar struct expects an int for these fields, update those fields to float64!
	bar.Analytics.AnchorADRHigh = bae.computeAnchorContinuousDistance(&h.AnchorADRHigh, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorADRLow = bae.computeAnchorContinuousDistance(&h.AnchorADRLow, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistHigh = bae.computeAnchorContinuousDistance(&h.AnchorDistGt, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistLow = bae.computeAnchorContinuousDistance(&h.AnchorDistLt, bar.Close, bar.Volume, adrPoints)

	// 5. Evaluate all macro metrics and ranks
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 6. Apply Directional Polarized Sign to Price and Efficiency Spectrum
	if bar.Close < bar.Open {
		if bar.Analytics.PriceRank > 0 {
			bar.Analytics.PriceRank = -bar.Analytics.PriceRank
		}

	}
}

func (bae *BarAnalyticsEngine) computeAnchorContinuousDistance(anchor *TrackedAnchor, currentPrice, currentVolume, adrPoints float64) float64 {
	if !anchor.IsActive || adrPoints <= 0 {
		return 0.0
	}

	// Dynamic temporary calculation incorporating current intra-bar tick metrics
	tempPV := anchor.CumPV + (currentPrice * currentVolume)
	tempVol := anchor.CumVolume + currentVolume

	if tempVol <= 0 {
		return 0.0
	}

	anchoredVwap := tempPV / tempVol
	rawDivergence := currentPrice - anchoredVwap

	// Normalize divergence against the asset's structural ADR points
	standardizedDivergence := rawDivergence / adrPoints

	// Squash into (-1, 1). A scaling factor of 10.0 means that if price runs away
	// from the AVWAP by 20% of its total ADR, the indicator will read a stable ~0.76
	const scalingFactor = 10.0
	return math.Tanh(standardizedDivergence * scalingFactor)
}
