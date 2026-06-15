package risk

import (
	"fmt"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/strategy"
)

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

// HandleManualAndBrokerStateSync forces localized memory maps to snap to actual execution realities
func (rm *RiskManager) HandleManualAndBrokerStateSync(symbol string, netQty int, side string, avgPrice float64, realizedPnL float64) {
	key := fmt.Sprintf("%s:MIS", symbol)

	rm.mu.Lock()
	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// Capture previous side to detect unexpected termination
	oldSide := pos.Side

	// Overwrite Risk Manager's internal state directly with Broker absolute truth
	pos.NetQuantity = netQty
	pos.Side = side
	pos.AveragePrice = avgPrice
	rm.mu.Unlock()

	// 🚨 CRITICAL STRATEGY OPTIMIZATION SYNC
	// If the position dropped to FLAT but the strategy engine thinks it is still in an active trade,
	// a manual user intervention took place. We must force-terminate the active Optimization Trade Log.
	if side == "FLAT" && oldSide != "FLAT" {
		rm.strategyEngine.LogOptimizationExit(symbol, "MANUAL_USER_INTERVENTION_SQUARE_OFF", &strategy.InstrumentState{
			LatestPrice: avgPrice, // fallback coordinate
		})
	}
}
