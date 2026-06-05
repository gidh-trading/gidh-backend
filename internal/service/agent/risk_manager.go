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

type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	scalper        *ScalperAgent
	agentPositions map[string]*models.Position
	circuitBroken  bool
	lastExitTime   map[string]time.Time

	// Metrics & UI Ledger
	dailyRealized    float64
	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:   om,
		scalper:        sa,
		agentPositions: make(map[string]*models.Position),
		lastExitTime:   make(map[string]time.Time),
		executedTrades: make([]models.BacktestExecutedTrade, 0),
	}
}

// ========================================================================
// 🏛️ MAIN PIPELINE INTERCEPTOR
// ========================================================================

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
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// 1. Cooldown Period (10 seconds)
	if pos.NetQuantity == 0 {
		if exitTime, ok := rm.lastExitTime[symbol]; ok && rawTick.Timestamp.Sub(exitTime) < 10*time.Second {
			rm.mu.Unlock()
			return
		}
	}

	// 2. Global Safety Nets
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
		totalNetPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		// Circuit Breaker
		if totalNetPnL <= -MaxDailyLossAllowed {
			rm.circuitBroken = true
			rm.executeFullLiquidation(symbol, pos, "Daily Drawdown Breached", rawTick)
			rm.mu.Unlock()
			return
		}

		// Intraday Cut-off Time (15:15)
		loc, _ := time.LoadLocation("Asia/Kolkata")
		simTime := rawTick.Timestamp.In(loc)
		if simTime.Hour() == 15 && simTime.Minute() >= 15 {
			rm.executeFullLiquidation(symbol, pos, "EOD Force Square-off", rawTick)
			rm.mu.Unlock()
			return
		}
	}
	rm.mu.Unlock()

	// 3. Ask the Scalper
	decision, triggered := rm.scalper.AnalyzeMarket(enrichedTick)
	if !triggered {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 4. Execution & Fee Gatekeeper
	switch decision {
	case "GO_LONG", "GO_SHORT":
		if pos.NetQuantity == 0 {
			allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / rawTick.LastPrice))
			if allowedQty <= 0 {
				rm.scalper.ResetPositionState(symbol)
				return
			}

			// --- THE FEE GATEKEEPER ---
			predictedCharges := computeItemizedCharges(allowedQty, rawTick.LastPrice)
			predictedFees := predictedCharges.TotalCharges
			scalperPlan := rm.scalper.GetPositionState(symbol)

			expectedProfitPerShare := math.Abs(scalperPlan.Target - rawTick.LastPrice)
			totalExpectedGross := expectedProfitPerShare * float64(allowedQty)

			// VETO: If expected profit isn't at least 3x the tax drag, reject it.
			if totalExpectedGross < (3.0 * predictedFees) {
				logger.Warnf("Trade Vetoed [%s]: Gross (₹%.2f) does not justify tax drag (₹%.2f)", symbol, totalExpectedGross, predictedFees)
				rm.scalper.ResetPositionState(symbol)
				return
			}
			// --------------------------

			side := "BUY"
			if decision == "GO_SHORT" {
				side = "SELL"
			}

			orderReq := models.OrderRequest{
				Symbol: symbol, Product: "MIS", TransactionType: side, OrderType: "MARKET", Quantity: allowedQty,
			}
			_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)

			// Update Position
			pos.NetQuantity = allowedQty
			pos.Side = "LONG"
			if side == "SELL" {
				pos.Side = "SHORT"
			}
			pos.AveragePrice = rawTick.LastPrice

			// Record Ledger
			rm.accumulateAuditCharges(predictedCharges)
			rm.dailyChargesPaid += predictedFees
			rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
				Timestamp:       rawTick.Timestamp,
				Side:            side,
				Symbol:          symbol,
				Exchange:        "NSE",
				Quantity:        allowedQty,
				AveragePrice:    rawTick.LastPrice,
				AllocatedCharge: predictedFees,
			})
		}

	case "EXIT_LONG", "EXIT_SHORT":
		if pos.NetQuantity > 0 {
			rm.executeFullLiquidation(symbol, pos, "Scalper Target/SL Hit", rawTick)
		}
	}
}

// ========================================================================
// 🛠️ EXECUTION CORES
// ========================================================================

func (rm *RiskManager) executeFullLiquidation(symbol string, pos *models.Position, reason string, tick models.TickData) {
	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	req := models.OrderRequest{
		Symbol: symbol, Product: "MIS", TransactionType: exitSide, OrderType: "MARKET", Quantity: pos.NetQuantity,
	}
	_, _ = rm.orderManager.PlaceOrder(context.Background(), req)

	// Calculate PnL
	multiplier := 1.0
	if pos.Side == "SHORT" {
		multiplier = -1.0
	}
	realizedPnL := (tick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
	rm.dailyRealized += realizedPnL

	// Record Ledger
	charges := computeItemizedCharges(pos.NetQuantity, tick.LastPrice)
	rm.accumulateAuditCharges(charges)
	rm.dailyChargesPaid += charges.TotalCharges

	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       tick.Timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        pos.NetQuantity,
		AveragePrice:    tick.LastPrice,
		AllocatedCharge: charges.TotalCharges,
	})

	rm.lastExitTime[symbol] = tick.Timestamp

	// Reset State
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
