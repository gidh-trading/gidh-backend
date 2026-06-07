package risk

import "gidh-backend/internal/service/models"

// GetUIContractNote delivers a deep copy of performance archives to feed visualization dashboards.
func (rm *RiskManager) GetUIContractNote() UIContractNotePayload {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Defend against read/write thread races by cloning slice variables inside a Read-Lock
	tradesCopy := make([]models.BacktestExecutedTrade, len(rm.executedTrades))
	copy(tradesCopy, rm.executedTrades)

	if tradesCopy == nil {
		tradesCopy = []models.BacktestExecutedTrade{}
	}

	return UIContractNotePayload{
		Summary: rm.globalSummary,
		Trades:  tradesCopy,
	}
}

// CalculatePositionSizeAndFees calculates permitted order sizes based on leverage and margin.
func (rm *RiskManager) CalculatePositionSizeAndFees(symbol string, price float64) (int, float64) {
	// Allocate 25% of total account capital per asset allocation configuration
	allowedCapitalAllocation := InitialCapital * MaxCapitalPerStockPct
	leveragedBuyingPower := allowedCapitalAllocation * MaxLeverage

	qty := int(leveragedBuyingPower / price)
	if qty <= 0 {
		return 0, 0.0
	}

	// Rough forecasted fee calculation to audit account viability
	totalCharges := rm.CalculateItemizedCharges(qty, price)
	return qty, totalCharges
}

// CalculateItemizedCharges tracks standard NSE contract transaction costs.
func (rm *RiskManager) CalculateItemizedCharges(qty int, price float64) float64 {
	turnover := float64(qty) * price

	// Cap intraday brokerage at standard limits
	brokerage := turnover * 0.0003
	if brokerage > 20.0 {
		brokerage = 20.0
	}

	stt := turnover * 0.00025
	exchangeFees := turnover * 0.0000345
	sebiOverhead := turnover * 0.000001
	gst := (brokerage + exchangeFees + sebiOverhead) * 0.18
	total := brokerage + stt + exchangeFees + sebiOverhead + gst

	charges := models.ItemizedCharges{
		Brokerage:              brokerage,
		STT:                    stt,
		ExchangeTurnoverCharge: exchangeFees,
		SebiTurnoverCharge:     sebiOverhead,
		GST:                    gst,
		TotalCharges:           total,
	}

	rm.recordTransactionCosts(charges)

	return total
}
