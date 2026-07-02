package pipeline

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const SmoothingAlpha = 0.4

// ContinuousLivingLedger tracks un-netted structural order flow states
type ContinuousLivingLedger struct {
	LastUpdated  time.Time
	VwapClosePct float64
}

type TrackedAnchor struct {
	IsActive        bool
	CumPV           float64 // Cumulative Price * Volume
	CumVolume       float64 // Cumulative Volume
	PrevAVWAP       float64 // Stores the AVWAP from the previous closed bar
	CurrentBarAVWAP float64 // Temporary placeholder for the active bar's AVWAP
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

	// --- Rolling State Vectors ---
	RollingVolumeIntensity float64
	RollingTickRank        float64
	RollingPriceNormalized float64
	RollingEfficiencyRank  float64
	RollingMomentumScore   float64
	RollingVwapVelocity    float64
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
	// 1. Process and lock anchor flags strictly using final closed state parameters
	bae.EvaluateAndLockAnchors(bar, h)

	// 2. Generate metrics (this reads the newly updated anchor statuses seamlessly)
	bae.CalculateContinuousAndStructuralMetrics(bar, h, true) // true = finalize and commit state

	// 3. Commit this bar's volume and price data into the active anchor history for future bars
	bae.AccumulateAnchorContext(bar, h)

	h.TotalBars++
	if bar.Close > bar.VWAP {
		h.BarsAboveVwap++
	}
	if h.TotalBars > 0 {
		bar.Analytics.VwapClosePct = (float64(h.BarsAboveVwap) / float64(h.TotalBars)) * 100
	}

	// --- 4. SMOOTH INDEPENDENT BASELINES ---
	alpha := SmoothingAlpha
	h.RollingVolumeIntensity = (alpha * float64(bar.Analytics.VolumeRank)) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	h.RollingTickRank = (alpha * float64(bar.Analytics.TickRank)) + ((1.0 - alpha) * h.RollingTickRank)
	h.RollingPriceNormalized = (alpha * float64(bar.Analytics.PriceRank)) + ((1.0 - alpha) * h.RollingPriceNormalized)
	h.RollingEfficiencyRank = (alpha * float64(bar.Analytics.EfficiencyRank)) + ((1.0 - alpha) * h.RollingEfficiencyRank)

	currentSlope := bar.Analytics.VWAPSlope
	if math.Abs(currentSlope) > 0.05 {
		h.RollingVwapVelocity = (alpha * currentSlope) + ((1.0 - alpha) * h.RollingVwapVelocity)
	} else {
		h.RollingVwapVelocity *= 0.98 // Decay gently only when flatlined
	}

	// --- 5. COMPUTE COMPOSITES ---
	rollingFlowIntensity := (0.75 * h.RollingVolumeIntensity) + (0.25 * h.RollingTickRank)
	signedExecutionEdge := (0.60 * h.RollingPriceNormalized) + (0.40 * h.RollingEfficiencyRank)

	// --- 6. COMPUTE MOMENTUM SCORE ---
	flowMultiplier := rollingFlowIntensity / 4.5
	h.RollingMomentumScore = signedExecutionEdge * flowMultiplier

	// --- 7. MAP TO PAYLOAD FOR UI / DB ---
	bar.Analytics.RollingVolumeIntensity = h.RollingVolumeIntensity
	bar.Analytics.RollingTickRank = h.RollingTickRank
	bar.Analytics.RollingFlowIntensity = rollingFlowIntensity
	bar.Analytics.RollingMomentumScore = h.RollingMomentumScore
	bar.Analytics.RollingVwapVelocity = h.RollingVwapVelocity

	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

// PopulateLiveAnalytics populates live snapshots for visual WebSocket feeds without mutating master history
func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bar.Analytics.NormalizedVwapDistance = bae.calculateDistance(bar.Close, bar.VWAP, uint32(bar.InstrumentToken))
	bae.CalculateContinuousAndStructuralMetrics(bar, h, false)

	// 1. Linearly project the forming bar's indicators
	alpha := SmoothingAlpha
	projectedVolIntensity := (alpha * float64(bar.Analytics.VolumeRank)) + ((1.0 - alpha) * h.RollingVolumeIntensity)
	projectedTickRank := (alpha * float64(bar.Analytics.TickRank)) + ((1.0 - alpha) * h.RollingTickRank)
	projectedPriceNorm := (alpha * float64(bar.Analytics.PriceRank)) + ((1.0 - alpha) * h.RollingPriceNormalized)
	projectedEffRank := (alpha * float64(bar.Analytics.EfficiencyRank)) + ((1.0 - alpha) * h.RollingEfficiencyRank)

	var projectedVwapVelocity float64
	if math.Abs(bar.Analytics.VWAPSlope) > 0.05 {
		projectedVwapVelocity = (alpha * bar.Analytics.VWAPSlope) + ((1.0 - alpha) * h.RollingVwapVelocity)
	} else {
		projectedVwapVelocity = h.RollingVwapVelocity * 0.98
	}

	// 2. Real-Time Composite Compositions
	projectedFlowIntensity := (0.75 * projectedVolIntensity) + (0.25 * projectedTickRank)
	projectedExecutionEdge := (0.60 * projectedPriceNorm) + (0.40 * projectedEffRank)

	// 3. Dynamic Real-Time Momentum Score Projection
	flowMultiplier := projectedFlowIntensity / 4.5

	// 4. Map back onto struct targets for serialization
	bar.Analytics.RollingVolumeIntensity = projectedVolIntensity
	bar.Analytics.RollingTickRank = projectedTickRank
	bar.Analytics.RollingFlowIntensity = projectedFlowIntensity
	bar.Analytics.RollingMomentumScore = projectedExecutionEdge * flowMultiplier
	bar.Analytics.RollingVwapVelocity = projectedVwapVelocity

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

	// 3. NATURAL BOUNDED VWAP VELOCITY INDICATOR (-1 to 1)
	var boundedVwapSlope float64 = 0.0
	if len(h.VWAPHistory) > 0 && adrPoints > 0 {
		prevVWAP := h.VWAPHistory[len(h.VWAPHistory)-1]
		rawRatio := (bar.VWAP - prevVWAP) / adrPoints
		const scalingFactor = 20.0
		boundedVwapSlope = math.Tanh(rawRatio * scalingFactor)
	}
	bar.Analytics.VWAPSlope = boundedVwapSlope

	if isBarClose {
		h.VWAPHistory = append(h.VWAPHistory, bar.VWAP)
		if len(h.VWAPHistory) > 5 {
			h.VWAPHistory = h.VWAPHistory[1:]
		}
	}

	// 4. CALCULATE CONTINUOUS DISTANCES (-1.0 to 1.0) FROM ANCHORS
	bar.Analytics.AnchorADRHigh = bae.computeAnchorSlope(&h.AnchorADRHigh, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorADRLow = bae.computeAnchorSlope(&h.AnchorADRLow, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistHigh = bae.computeAnchorSlope(&h.AnchorDistGt, bar.Close, bar.Volume, adrPoints)
	bar.Analytics.AnchorDistLow = bae.computeAnchorSlope(&h.AnchorDistLt, bar.Close, bar.Volume, adrPoints)

	// 5. Evaluate all macro metrics and ranks
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// 6. Apply Directional Polarized Sign to Price and Efficiency Spectrum
	if bar.Close < bar.Open {
		if bar.Analytics.PriceRank > 0 {
			bar.Analytics.PriceRank = -bar.Analytics.PriceRank
		}
	}
}

func (bae *BarAnalyticsEngine) computeAnchorSlope(anchor *TrackedAnchor, currentPrice, currentVolume, adrPoints float64) float64 {
	if !anchor.IsActive || adrPoints <= 0 {
		return 0.0
	}

	// 1. Calculate the instantaneous AVWAP for the active bar
	tempPV := anchor.CumPV + (currentPrice * currentVolume)
	tempVol := anchor.CumVolume + currentVolume

	if tempVol <= 0 {
		return 0.0
	}
	currentAVWAP := tempPV / tempVol

	// Cache it temporarily so we can lock it into PrevAVWAP when the bar closes
	anchor.CurrentBarAVWAP = currentAVWAP

	// 2. If this is the very first bar of the anchor, there is no previous slope yet
	if anchor.PrevAVWAP <= 0 {
		return 0.0
	}

	// 3. Mirror the exact VWAP Slope velocity logic
	rawRatio := (currentAVWAP - anchor.PrevAVWAP) / adrPoints
	const scalingFactor = 20.0 // Kept at 20.0 to match your main VWAP slope

	return math.Tanh(rawRatio * scalingFactor)
}
