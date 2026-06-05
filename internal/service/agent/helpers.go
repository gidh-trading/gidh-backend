package agent

import (
	"gidh-backend/internal/service/models"
	"math"
)

func (rm *RiskManager) IngestClosedBar(bar *models.Bar) {
	rm.scalper.IngestClosedBar(bar)
}

// VCNGlobalMetrics represents the total state of the single shared backtest account
type VCNGlobalMetrics struct {
	TotalChargesPaid float64  `json:"total_charges_paid"`
	TotalRealizedPnL float64  `json:"total_realized_pnl"`
	ActiveSymbols    []string `json:"active_symbols"`
	PositionsCount   int      `json:"positions_count"`
}

// GetGlobalVCNMetrics extracts the single ledger state under a Read Lock
func (rm *RiskManager) GetGlobalVCNMetrics() VCNGlobalMetrics {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	symbols := make([]string, 0, len(rm.agentPositions))
	for sym := range rm.agentPositions {
		symbols = append(symbols, sym)
	}

	return VCNGlobalMetrics{
		TotalChargesPaid: rm.dailyChargesPaid, // Accumulated via PredictRoundTripCharges
		TotalRealizedPnL: rm.dailyRealized,    // Tracked across all automated actions
		ActiveSymbols:    symbols,
		PositionsCount:   len(symbols),
	}
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
