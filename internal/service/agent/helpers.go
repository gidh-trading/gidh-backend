package agent

import "gidh-backend/internal/service/models"

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
