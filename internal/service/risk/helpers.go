package risk

import (
	"fmt"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/strategy"
	"gidh-backend/pkg/logger"
	"time"
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

// HandleManualAndBrokerStateSync forces localized memory maps to snap to actual execution realities.
// It intercepts unexpected flat transitions to cleanly close strategy optimization logging records.
func (rm *RiskManager) HandleManualAndBrokerStateSync(symbol string, netQty int, side string, avgPrice float64, realizedPnL float64) {
	key := fmt.Sprintf("%s:MIS", symbol)

	rm.mu.Lock()
	pos, exists := rm.agentPositions[key]
	if !exists {
		// Initialize the tracking instance if it does not yet exist in risk manager memory
		pos = &models.Position{
			Symbol:      symbol,
			Product:     "MIS",
			NetQuantity: 0,
			Side:        "FLAT",
		}
		rm.agentPositions[key] = pos
	}

	// 1. Capture snapshots of historical metrics inside the lock boundary to evaluate anomalies
	oldSide := pos.Side
	oldQty := pos.NetQuantity

	// 2. Overwrite Risk Manager's internal state with verified execution absolute truth
	pos.NetQuantity = netQty
	pos.Side = side
	pos.AveragePrice = avgPrice

	// Update systemic realized metrics for global account drawdown boundaries
	// Note: If you want to accumulate or directly assign session PnL, map it safely here.

	// 🔓 CRITICAL STEP: Release the lock immediately before entering outer package layers!
	rm.mu.Unlock()

	// 3. Evaluate State Transitions for External Strategy Sync
	// If the asset dropped to "FLAT" but our memory tracks it as having active market exposure,
	// an external manual liquidation, bracket fill, or panic square-off occurred.
	if (side == "FLAT" || side == "") && (oldSide != "FLAT" && oldSide != "") {
		logger.Warnf("⚠️ Asynchronous State Sync: Position for %s closed externally (Previous: %s Qty: %d). Forcing Strategy Engine Optimization Exit...", symbol, oldSide, oldQty)

		// Build a placeholder InstrumentState to pass the closing price coordinate down to database log routines
		fallbackState := &strategy.InstrumentState{
			LatestPrice: avgPrice,
		}
		if avgPrice <= 0 {
			// If closed flat at 0 entry, fallback context to standard market tick logic values
			fallbackState.LatestPrice = pos.AveragePrice
		}

		// Cleanly close out the optimization logs so performance tracking remains mathematically correct
		rm.strategyEngine.LogOptimizationExit(symbol, "MANUAL_USER_INTERVENTION_SQUARE_OFF", fallbackState, time.Now())
	} else {
		logger.Infof("🔄 System Sync: Internal Risk mapping updated for %s | Qty: %d | Side: %s | AvgPrice: %.2f", symbol, netQty, side, avgPrice)
	}
}
