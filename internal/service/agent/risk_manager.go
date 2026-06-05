package agent

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/pkg/logger"
)

const FixedTargetINR = 1000.0

type RiskManager struct {
	mu               sync.RWMutex
	orderManager     order.PositionManager
	scalper          *ScalperAgent
	agentPositions   map[string]*models.Position
	circuitBroken    bool
	lastExitTime     map[string]time.Time
	dailyRealized    float64
	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

// ProcessSequentialTick monitors live price changes for fast Take Profit hits
func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName
	key := fmt.Sprintf("%s:MIS", symbol)

	pos, exists := rm.agentPositions[key]
	if !exists {
		rm.mu.Unlock()
		return
	}

	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier

		// Take Profit Sniper (Triggers instantly mid-bar if target hit)
		if pos.UnrealizedPnL >= FixedTargetINR {
			rm.executeFullLiquidation(symbol, pos, "Fixed ₹1000 Target Hit", rawTick.Timestamp, rawTick.LastPrice)
			rm.mu.Unlock()
			return
		}

		// Hard Stop Drawdown Guard
		totalNetPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid
		if totalNetPnL <= -MaxDailyLossAllowed { // Breach guard[cite: 4]
			rm.circuitBroken = true
			rm.executeFullLiquidation(symbol, pos, "Daily Drawdown Breached", rawTick.Timestamp, rawTick.LastPrice)
			rm.mu.Unlock()
			return
		}
	}
	rm.mu.Unlock()
}

// ProcessBarSignal coordinates entry and structural stop-loss signals from rolling bars
func (rm *RiskManager) ProcessBarSignal(symbol string, timeframe string, analytics models.BarAnalytics, currentPrice float64, timestamp time.Time) {
	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	key := fmt.Sprintf("%s:MIS", symbol)
	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// 1. Cooldown Protection
	if pos.NetQuantity == 0 {
		if exitTime, ok := rm.lastExitTime[symbol]; ok && timestamp.Sub(exitTime) < 10*time.Second {
			rm.mu.Unlock()
			return
		}
	}
	rm.mu.Unlock()

	// 2. Delegate evaluation to Rolling Bar engine
	decision, triggered := rm.scalper.ProcessRollingBar(symbol, timeframe, analytics, currentPrice)
	if !triggered {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 3. Execution Processing
	switch decision {
	case "GO_LONG", "GO_SHORT":
		if pos.NetQuantity == 0 {
			allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / currentPrice)) // Size capital allocation[cite: 4]
			if allowedQty <= 0 {
				rm.scalper.ResetPositionState(symbol)
				return
			}

			// Fee Gatekeeper check
			predictedCharges := computeItemizedCharges(allowedQty, currentPrice)
			if FixedTargetINR < (3.0 * predictedCharges.TotalCharges) {
				logger.Warnf("Trade Vetoed [%s]: Target unviable for fee friction", symbol)
				rm.scalper.ResetPositionState(symbol)
				return
			}

			side := "BUY"
			if decision == "GO_SHORT" {
				side = "SELL"
			}

			req := models.OrderRequest{Symbol: symbol, Product: "MIS", TransactionType: side, OrderType: "MARKET", Quantity: allowedQty}
			_, _ = rm.orderManager.PlaceOrder(context.Background(), req)

			pos.NetQuantity = allowedQty
			pos.Side = "LONG"
			if side == "SELL" {
				pos.Side = "SHORT"
			}
			pos.AveragePrice = currentPrice

			rm.accumulateAuditCharges(predictedCharges)
			rm.dailyChargesPaid += predictedCharges.TotalCharges
			rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
				Timestamp: timestamp, Side: side, Symbol: symbol, Exchange: "NSE", Quantity: allowedQty, AveragePrice: currentPrice, AllocatedCharge: predictedCharges.TotalCharges,
			})
		}

	case "EXIT_LONG", "EXIT_SHORT":
		if pos.NetQuantity > 0 {
			rm.executeFullLiquidation(symbol, pos, "Structural Bar Stop Loss Hit", timestamp, currentPrice)
		}
	}
}

// executeFullLiquidation handles clean structural execution teardown[cite: 4]
func (rm *RiskManager) executeFullLiquidation(symbol string, pos *models.Position, reason string, timestamp time.Time, executionPrice float64) {
	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	req := models.OrderRequest{Symbol: symbol, Product: "MIS", TransactionType: exitSide, OrderType: "MARKET", Quantity: pos.NetQuantity}
	_, _ = rm.orderManager.PlaceOrder(context.Background(), req)

	multiplier := 1.0
	if pos.Side == "SHORT" {
		multiplier = -1.0
	}

	realizedPnL := (executionPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
	rm.dailyRealized += realizedPnL

	charges := computeItemizedCharges(pos.NetQuantity, executionPrice)
	rm.accumulateAuditCharges(charges)
	rm.dailyChargesPaid += charges.TotalCharges
	rm.lastExitTime[symbol] = timestamp

	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp: timestamp, Side: exitSide, Symbol: symbol, Exchange: "NSE", Quantity: pos.NetQuantity, AveragePrice: executionPrice, AllocatedCharge: charges.TotalCharges,
	})

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0

	rm.scalper.ResetPositionState(symbol)
}

func (rm *RiskManager) accumulateAuditCharges(charges models.ItemizedCharges) {
	rm.globalSummary.Brokerage += charges.Brokerage
	rm.globalSummary.STT += charges.STT
	rm.globalSummary.StampDuty += charges.StampDuty
	rm.globalSummary.ExchangeTurnoverCharge += charges.ExchangeTurnoverCharge
	rm.globalSummary.SebiTurnoverCharge += charges.SebiTurnoverCharge
	rm.globalSummary.GST += charges.GST
	rm.globalSummary.TotalCharges += charges.TotalCharges
}
