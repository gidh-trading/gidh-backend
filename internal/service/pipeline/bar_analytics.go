package pipeline

import (
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	StandardDecayConstant   = 0.80 // Baseline decay rate for trend bars
	AbsorptionDecayConstant = 0.60 // Accelerated decay to forget past states faster during absorption
	TheoreticalMaxCeiling   = 5.0  // 1.0 / (1.0 - 0.80)
)

// ContinuousLivingLedger tracks un-netted structural order flow states
type ContinuousLivingLedger struct {
	BullPriceState  float64
	BearPriceState  float64
	BullVolumeState float64
	BearVolumeState float64
	LastUpdated     time.Time
}

type TimeframeAnalyticsHistory struct {
	Ledger            ContinuousLivingLedger
	TotalBars         int
	TimePctAboveVwap  float64
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
	bae.computeMacroTimeframeRanksAndDirection(bar)
}

func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bae.computeMacroTimeframeRanksAndDirection(bar)

	// Determine decay speed based on absorption structure
	currentDecay := StandardDecayConstant
	isAbsorption := bar.Analytics.Direction == models.DirBullishAbsorption || bar.Analytics.Direction == models.DirBearishAbsorption
	if isAbsorption {
		currentDecay = AbsorptionDecayConstant
	}

	// 1. DECAY BALANCES
	h.Ledger.BullPriceState *= currentDecay
	h.Ledger.BearPriceState *= currentDecay
	h.Ledger.BullVolumeState *= currentDecay
	h.Ledger.BearVolumeState *= currentDecay
	h.Ledger.LastUpdated = bar.Timestamp

	// 2. COMPUTE CLEAN RANK EFFICIENCIES
	rawVolEff, rawPriceEff := bae.calculateProfileBlendedEfficiencies(bar)

	// 3. MATRIX INJECTION WITH CONDITIONS
	switch bar.Analytics.Direction {
	case models.DirBullish, models.DirStrongBullish:
		h.Ledger.BullVolumeState += rawVolEff
		h.Ledger.BullPriceState += rawPriceEff

	case models.DirBearish, models.DirStrongBearish:
		h.Ledger.BearVolumeState += rawVolEff
		h.Ledger.BearPriceState += rawPriceEff

	case models.DirBullishAbsorption:
		// Zero volume efficiency added during absorption periods
		h.Ledger.BullPriceState += rawPriceEff

	case models.DirBearishAbsorption:
		// Zero volume efficiency added during absorption periods
		h.Ledger.BearPriceState += rawPriceEff
	}

	// 4. RESOLVE MOOD
	bar.Analytics.NetPriceMood = bae.calculateNetEfficiency(h.Ledger.BullPriceState, h.Ledger.BearPriceState, TheoreticalMaxCeiling)
	bar.Analytics.NetVolumeMood = bae.calculateNetEfficiency(h.Ledger.BullVolumeState, h.Ledger.BearVolumeState, TheoreticalMaxCeiling)

	if bae.dbWriter != nil {
		bae.dbWriter.AddBar(*bar)
	}
}

func (bae *BarAnalyticsEngine) PopulateLiveAnalytics(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	bae.computeMacroTimeframeRanksAndDirection(bar)

	currentDecay := StandardDecayConstant
	isAbsorption := bar.Analytics.Direction == models.DirBullishAbsorption || bar.Analytics.Direction == models.DirBearishAbsorption
	if isAbsorption {
		currentDecay = AbsorptionDecayConstant
	}

	rawVolEff, rawPriceEff := bae.calculateProfileBlendedEfficiencies(bar)

	liveBullPrice := h.Ledger.BullPriceState * currentDecay
	liveBearPrice := h.Ledger.BearPriceState * currentDecay
	liveBullVol := h.Ledger.BullVolumeState * currentDecay
	liveBearVol := h.Ledger.BearVolumeState * currentDecay

	switch bar.Analytics.Direction {
	case models.DirBullish, models.DirStrongBullish:
		liveBullVol += rawVolEff
		liveBullPrice += rawPriceEff
	case models.DirBearish, models.DirStrongBearish:
		liveBearVol += rawVolEff
		liveBearPrice += rawPriceEff
	case models.DirBullishAbsorption:
		liveBullPrice += rawPriceEff
	case models.DirBearishAbsorption:
		liveBearPrice += rawPriceEff
	}

	bar.Analytics.NetPriceMood = bae.calculateNetEfficiency(liveBullPrice, liveBearPrice, TheoreticalMaxCeiling)
	bar.Analytics.NetVolumeMood = bae.calculateNetEfficiency(liveBullVol, liveBearVol, TheoreticalMaxCeiling)
}
