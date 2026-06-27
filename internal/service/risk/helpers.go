package risk

import (
	"fmt"
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

// CalculateItemizedCharges tracks standard NSE contract transaction costs for tracking total loss thresholds.
func (rm *RiskManager) CalculateItemizedCharges(qty int, price float64) float64 {
	turnover := float64(qty) * price

	brokerage := turnover * 0.0003
	if brokerage > 20.0 {
		brokerage = 20.0
	}

	stt := turnover * 0.00025
	exchangeFees := turnover * 0.0000345
	sebiOverhead := turnover * 0.000001
	gst := (brokerage + exchangeFees + sebiOverhead) * 0.18
	total := brokerage + stt + exchangeFees + sebiOverhead + gst

	return total
}

// CalculatePositionSizeAndFees calculates permitted order sizes based on leverage and margin without mutating records.
func (rm *RiskManager) CalculatePositionSizeAndFees(symbol string, price float64) (int, float64) {
	allowedCapitalAllocation := InitialCapital * MaxCapitalPerStockPct
	leveragedBuyingPower := allowedCapitalAllocation * MaxLeverage

	qty := int(leveragedBuyingPower / price)
	if qty <= 0 {
		return 0, 0.0
	}

	total := rm.CalculateItemizedCharges(qty, price)
	return qty, total
}

// HandleManualAndBrokerStateSync updates internal state if a user manually modifies a position via the broker's native interface.
func (rm *RiskManager) HandleManualAndBrokerStateSync(symbol string, netQty int, side string, avgPrice float64, realizedPnL float64) {
	key := fmt.Sprintf("%s:MIS", symbol)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{
			Symbol:       symbol,
			Product:      "MIS",
			NetQuantity:  netQty,
			Side:         side,
			AveragePrice: avgPrice,
		}
		rm.agentPositions[key] = pos
		return
	}

	oldSide := pos.Side

	pos.NetQuantity = netQty
	pos.Side = side
	pos.AveragePrice = avgPrice

	if (side == "FLAT" || side == "") && (oldSide != "FLAT" && oldSide != "") {
		logger.Warnf("⚠️ Asynchronous State Sync: Position for %s closed externally. Strategy will auto-heal on next tick.", symbol)
	} else {
		logger.Infof("🔄 System Sync: Internal Risk mapping updated for %s | Qty: %d | Side: %s | AvgPrice: %.2f", symbol, netQty, side, avgPrice)
	}
}
