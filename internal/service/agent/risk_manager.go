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
	dailyRealized  float64
	circuitBroken  bool

	// 🔥 Simulated Cooldown: Tracks the historical timestamp of the last liquidation
	lastExitTime map[string]time.Time

	// Money Management metrics for performance auditing
	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:     om,
		scalper:          sa,
		agentPositions:   make(map[string]*models.Position),
		lastExitTime:     make(map[string]time.Time),
		dailyRealized:    0.0,
		dailyChargesPaid: 0.0,
		circuitBroken:    false,
		executedTrades:   make([]models.BacktestExecutedTrade, 0),
	}
}

// ========================================================================
// 🏛️ MAIN PIPELINE INTERCEPTOR (HISTORICAL TICK TIME SPEED)
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

	// 1. 🛡️ Simulated Front Gateway Cooldown: Uses backtest tick time context
	// If the net quantity is flat, check if 10 simulated seconds have passed since the last square-off.
	if pos.NetQuantity == 0 {
		if exitTimestamp, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTimestamp) < 10*time.Second {
				rm.mu.Unlock()
				return
			}
		}
	}

	// 2. Dynamic Intraday Session Accounting & Force Liquidations
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		// Asymmetric PnL tracking
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier

		totalNetSessionPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		// Max Drawdown Hard Stop
		if totalNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Money Manager] True Capital Drawdown Breached (₹%.2f). Freezing Agent.", totalNetSessionPnL)
			rm.circuitBroken = true
			rm.executeFullLiquidationBrokerOrder(symbol, pos, "Net Session Risk Floor Breach", rawTick.Timestamp, rawTick.LastPrice)
			rm.mu.Unlock()
			return
		}

		// Intraday Time Limit Square-off (Calculated against historical location context)
		loc, _ := time.LoadLocation("Asia/Kolkata")
		simulatedTimeInKolkata := rawTick.Timestamp.In(loc)
		if simulatedTimeInKolkata.Hour() == 15 && simulatedTimeInKolkata.Minute() >= 15 {
			rm.executeFullLiquidationBrokerOrder(symbol, pos, "Intraday 15:15 Force Square-off", rawTick.Timestamp, rawTick.LastPrice)
			rm.mu.Unlock()
			return
		}
	}
	rm.mu.Unlock() // Unlock cleanly before passing data downstream

	// 3. Dispatch tick to the autonomous state-window engine
	decision, triggered := rm.scalper.AnalyzeMarket(enrichedTick)
	if !triggered {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 4. Process Advanced Synchronized Directives
	switch decision {

	// ==========================================
	// 🛫 LONG SPREAD LIFECYCLE MANAGEMENT
	// ==========================================
	case "GO_LONG":
		if pos.NetQuantity == 0 {
			allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / rawTick.LastPrice))
			if allowedQty <= 0 {
				return
			}

			predictedFees := PredictRoundTripCharges(allowedQty, rawTick.LastPrice)
			projectedNetSessionPnL := rm.dailyRealized - (rm.dailyChargesPaid + predictedFees)
			if projectedNetSessionPnL <= -MaxDailyLossAllowed {
				logger.Warnf("[Money Manager] Vetoed Long Setup for %s. Tax drag breaches risk limits.", symbol)
				return
			}

			orderReq := models.OrderRequest{
				Symbol:          symbol,
				Product:         "MIS",
				TransactionType: "BUY",
				OrderType:       "MARKET",
				Quantity:        allowedQty,
				UserEmail:       AgentEmail,
			}

			logger.Infof("[Money Manager] Approved Long Setup. Executing BUY for %s", symbol)

			charges := computeItemizedCharges(allowedQty, rawTick.LastPrice)
			rm.accumulateAuditCharges(charges)

			rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
				Timestamp:       rawTick.Timestamp,
				Side:            "BUY",
				Symbol:          symbol,
				Exchange:        "NSE",
				Quantity:        allowedQty,
				AveragePrice:    rawTick.LastPrice,
				AllocatedCharge: charges.TotalCharges,
			})

			rm.dailyChargesPaid += charges.TotalCharges
			pos.NetQuantity = allowedQty
			pos.Side = "LONG"
			pos.AveragePrice = rawTick.LastPrice
			pos.UnrealizedPnL = 0.0

			_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)
		}

	case "SLICE_50_PERCENT_LONG":
		if pos.NetQuantity > 0 && pos.Side == "LONG" {
			sliceQty := pos.NetQuantity / 2
			if sliceQty > 0 {
				logger.Warnf("[Money Manager] Milestone 1 (P75) Reached for %s. Slicing 50%% long position (%d shares) to lock in profit.", symbol, sliceQty)
				rm.executePartialSliceBrokerOrder(symbol, pos, "SELL", sliceQty, rawTick.Timestamp, rawTick.LastPrice, "Scalper Milestone 1 Partial Profit Take")
			}
		}

	case "LIQUIDATE_ALL_LONG":
		if pos.NetQuantity > 0 && pos.Side == "LONG" {
			rm.executeFullLiquidationBrokerOrder(symbol, pos, "Scalper Long Full Exit", rawTick.Timestamp, rawTick.LastPrice)
		}

	// ==========================================
	// 🛬 SHORT SPREAD LIFECYCLE MANAGEMENT
	// ==========================================
	case "GO_SHORT":
		if pos.NetQuantity == 0 {
			allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / rawTick.LastPrice))
			if allowedQty <= 0 {
				return
			}

			predictedFees := PredictRoundTripCharges(allowedQty, rawTick.LastPrice)
			projectedNetSessionPnL := rm.dailyRealized - (rm.dailyChargesPaid + predictedFees)
			if projectedNetSessionPnL <= -MaxDailyLossAllowed {
				logger.Warnf("[Money Manager] Vetoed Short Setup for %s. Tax drag breaches risk limits.", symbol)
				return
			}

			orderReq := models.OrderRequest{
				Symbol:          symbol,
				Product:         "MIS",
				TransactionType: "SELL",
				OrderType:       "MARKET",
				Quantity:        allowedQty,
				UserEmail:       AgentEmail,
			}

			logger.Infof("[Money Manager] Approved Short Setup. Executing SELL for %s", symbol)

			charges := computeItemizedCharges(allowedQty, rawTick.LastPrice)
			rm.accumulateAuditCharges(charges)

			rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
				Timestamp:       rawTick.Timestamp,
				Side:            "SELL",
				Symbol:          symbol,
				Exchange:        "NSE",
				Quantity:        allowedQty,
				AveragePrice:    rawTick.LastPrice,
				AllocatedCharge: charges.TotalCharges,
			})

			rm.dailyChargesPaid += charges.TotalCharges
			pos.NetQuantity = allowedQty
			pos.Side = "SHORT"
			pos.AveragePrice = rawTick.LastPrice
			pos.UnrealizedPnL = 0.0

			_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)
		}

	case "SLICE_50_PERCENT_SHORT":
		if pos.NetQuantity > 0 && pos.Side == "SHORT" {
			sliceQty := pos.NetQuantity / 2
			if sliceQty > 0 {
				logger.Warnf("[Money Manager] Milestone 1 (P75) Reached for short %s. Slicing 50%% position (%d shares) to cover.", symbol, sliceQty)
				rm.executePartialSliceBrokerOrder(symbol, pos, "BUY", sliceQty, rawTick.Timestamp, rawTick.LastPrice, "Scalper Milestone 1 Partial Short Cover")
			}
		}

	case "LIQUIDATE_ALL_SHORT":
		if pos.NetQuantity > 0 && pos.Side == "SHORT" {
			rm.executeFullLiquidationBrokerOrder(symbol, pos, "Scalper Short Full Exit", rawTick.Timestamp, rawTick.LastPrice)
		}
	}
}

// ========================================================================
// 🛠️ SUB-SYSTEM INTERFACE EXECUTION CORES
// ========================================================================

func (rm *RiskManager) executePartialSliceBrokerOrder(symbol string, pos *models.Position, exitSide string, qty int, timestamp time.Time, executionPrice float64, reason string) {
	orderReq := models.OrderRequest{
		Symbol:          symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        qty,
		UserEmail:       AgentEmail,
	}

	_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)

	multiplier := 1.0
	if pos.Side == "SHORT" {
		multiplier = -1.0
	}
	realizedSlicePnL := (executionPrice - pos.AveragePrice) * float64(qty) * multiplier
	rm.dailyRealized += realizedSlicePnL

	pos.NetQuantity -= qty

	charges := computeItemizedCharges(qty, executionPrice)
	rm.accumulateAuditCharges(charges)
	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        qty,
		AveragePrice:    executionPrice,
		AllocatedCharge: charges.TotalCharges,
	})
	rm.dailyChargesPaid += charges.TotalCharges
}

func (rm *RiskManager) executeFullLiquidationBrokerOrder(symbol string, pos *models.Position, reason string, timestamp time.Time, executionPrice float64) {
	if pos.NetQuantity == 0 {
		return
	}

	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	exitReq := models.OrderRequest{
		Symbol:          symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        pos.NetQuantity,
		UserEmail:       AgentEmail,
	}

	logger.Warnf("[Money Manager] Executing Full Square-Off for %s (%s). Reason: %s", symbol, pos.Side, reason)
	_, _ = rm.orderManager.PlaceOrder(context.Background(), exitReq)

	multiplier := 1.0
	if pos.Side == "SHORT" {
		multiplier = -1.0
	}
	pos.UnrealizedPnL = (executionPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
	rm.dailyRealized += pos.UnrealizedPnL

	charges := computeItemizedCharges(pos.NetQuantity, executionPrice)
	rm.accumulateAuditCharges(charges)
	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        pos.NetQuantity,
		AveragePrice:    executionPrice,
		AllocatedCharge: charges.TotalCharges,
	})
	rm.dailyChargesPaid += charges.TotalCharges

	// 🔥 Core Fix: Save the historical timestamp parsed from the data file!
	rm.lastExitTime[symbol] = timestamp

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

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
