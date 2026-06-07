package scalper

type MorningRankStrategy struct{}

func NewMorningRankStrategy() *MorningRankStrategy { return &MorningRankStrategy{} }

func (s *MorningRankStrategy) Name() string { return "Morning_Rank_Momentum" }

func (s *MorningRankStrategy) CheckEntry(state *InstrumentState) string {
	// Rule: High volume velocity early in the day
	if state.LatestVolumeRank > 6 && state.LatestPriceRank > 6 {
		if state.LatestDirection == "BULLISH" {
			return "GO_LONG"
		}
		if state.LatestDirection == "BEARISH" {
			return "GO_SHORT"
		}
	}
	return "HOLD"
}

func (s *MorningRankStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	if state.LatestVolumeRank <= 3 {
		if currentSide == "LONG" {
			return "EXIT_LONG"
		}
		if currentSide == "SHORT" {
			return "EXIT_SHORT"
		}
	}
	return "HOLD"
}
