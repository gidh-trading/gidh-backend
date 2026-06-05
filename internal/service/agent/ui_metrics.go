package agent

import (
	"gidh-backend/internal/service/models"
	"math"
)

// ========================================================================
// 📊 UI METRICS & REPORTING
// ========================================================================

type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

func (rm *RiskManager) GetUIContractNote() UIContractNotePayload {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	trades := rm.executedTrades
	if trades == nil {
		trades = []models.BacktestExecutedTrade{}
	}

	return UIContractNotePayload{
		Summary: rm.globalSummary,
		Trades:  trades,
	}
}

func computeItemizedCharges(quantity int, price float64) models.ItemizedCharges {
	turnoverSingleLeg := float64(quantity) * price
	totalTurnoverRoundTrip := turnoverSingleLeg * 2.0

	buyBrokerage := math.Min(turnoverSingleLeg*0.0003, 20.0)
	sellBrokerage := math.Min(turnoverSingleLeg*0.0003, 20.0)
	totalBrokerage := buyBrokerage + sellBrokerage

	stt := turnoverSingleLeg * 0.00025
	exchangeFees := totalTurnoverRoundTrip * 0.0000322
	sebiFees := totalTurnoverRoundTrip * 0.0000001
	stampDuty := turnoverSingleLeg * 0.00003
	gst := (totalBrokerage + exchangeFees + sebiFees) * 0.18
	total := totalBrokerage + stt + exchangeFees + sebiFees + stampDuty + gst

	return models.ItemizedCharges{
		Brokerage:              buyBrokerage + sellBrokerage,
		STT:                    stt,
		StampDuty:              stampDuty,
		ExchangeTurnoverCharge: exchangeFees,
		SebiTurnoverCharge:     sebiFees,
		GST:                    gst,
		TotalCharges:           total,
	}
}
