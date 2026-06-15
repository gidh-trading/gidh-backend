package risk

import (
	"context"
	"gidh-backend/internal/service/strategy"
	"gidh-backend/pkg/logger"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

const (
	MaxDailyLossAllowed   = 3000.0
	InitialCapital        = 70000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.25
	AgentEmail            = "algo.trader@gidh.tech"
	AutoSquareOffHour     = 15 // 3 PM
	AutoSquareOffMinute   = 0  // 00 minutes
	MaxConcurrentTrades   = 4
)

type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	strategyEngine *strategy.Engine
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
	lastExitTime   map[string]time.Time

	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

func NewRiskManager(om order.PositionManager, se *strategy.Engine) *RiskManager {
	return &RiskManager{
		orderManager:   om,
		strategyEngine: se,
		agentPositions: make(map[string]*models.Position),
		lastExitTime:   make(map[string]time.Time),
		dailyRealized:  0.0,
		circuitBroken:  false,
		executedTrades: make([]models.BacktestExecutedTrade, 0),
	}
}

// ProcessSequentialTick coordinates data collection updates safely across package layers.
func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName
	tickTime := rawTick.Timestamp

	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	// 1. TIME CUTOFF CHECK (3:00 PM Auto-Square Off)
	currentHM := (tickTime.Hour() * 100) + tickTime.Minute()
	cutoffHM := (AutoSquareOffHour * 100) + AutoSquareOffMinute

	if currentHM >= cutoffHM {
		logger.Warnf("🕒 Intraday Cutoff [3:00 PM] breached. Engaging Auto-Square Off across all instruments...")
		rm.circuitBroken = true

		for mapKey, pos := range rm.agentPositions {
			if pos.NetQuantity != 0 && pos.Side != "FLAT" {
				var exitSide string
				if pos.Side == "LONG" {
					exitSide = "SELL"
				} else {
					exitSide = "BUY"
				}

				exitReq := rm.buildExitOrderRequest(pos, exitSide)

				totalCharges := rm.CalculateItemizedCharges(pos.NetQuantity, pos.AveragePrice)
				rm.globalSummary.TotalCharges += totalCharges
				rm.dailyChargesPaid += totalCharges

				rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
					Timestamp:       tickTime,
					Side:            exitSide,
					Symbol:          pos.Symbol,
					Exchange:        "NSE",
					Quantity:        pos.NetQuantity,
					AveragePrice:    rawTick.LastPrice,
					AllocatedCharge: totalCharges,
				})

				rm.lastExitTime[mapKey] = tickTime

				pos.NetQuantity = 0
				pos.Side = "FLAT"
				pos.AveragePrice = 0.0

				go rm.orderManager.PlaceOrder(context.Background(), exitReq)
			}
		}
		rm.mu.Unlock()
		return
	}

	pos, exists := rm.agentPositions[symbol]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[symbol] = pos
	}

	if pos.NetQuantity == 0 && pos.Side != "FLAT" {
		pos.Side = "FLAT"
		pos.AveragePrice = 0.0
	}

	totalNetSessionPnL := rm.dailyRealized - rm.dailyChargesPaid
	if totalNetSessionPnL <= -MaxDailyLossAllowed {
		rm.circuitBroken = true
		rm.executeBrokerOrder(symbol, pos, "Global Account Drawdown Cap Triggered", rawTick.Timestamp)
		rm.mu.Unlock()
		return
	}

	currentSide := pos.Side
	avgPrice := pos.AveragePrice
	netQty := pos.NetQuantity
	rm.mu.Unlock()

	// real-time tick context pipeline execution
	tickSignal := rm.strategyEngine.UpdateContext(enrichedTick, currentSide, avgPrice, netQty)

	if tickSignal == "EXIT_LONG" || tickSignal == "EXIT_SHORT" {
		rm.mu.Lock()
		if pos.NetQuantity != 0 {
			rm.executeBrokerOrder(symbol, pos, "Intelligent Volatility Profit Lock Triggered", rawTick.Timestamp)
		}
		rm.mu.Unlock()
		return
	}

	barSignal := rm.strategyEngine.GenerateSignal(symbol, currentSide, avgPrice, netQty)

	var engineEff float64
	var engineVwap float64
	if state, exists := rm.strategyEngine.Registry[symbol]; exists {
		engineEff = state.NetEfficiency
		engineVwap = state.LiveSessionVWAP
	}

	logger.Debugf("🔍 STRATEGY BRIDGE | Symbol: %s | Signal: %s | NetEff: %.2f | Price: %.2f | VWAP: %.2f",
		symbol, barSignal, engineEff, rawTick.LastPrice, engineVwap)

	if barSignal == "HOLD" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Entry Order logic with optimized dynamic sizing calculations
	if (barSignal == "GO_SHORT" || barSignal == "GO_LONG") && pos.NetQuantity == 0 {

		activeTradesCount := 0
		for _, p := range rm.agentPositions {
			if p.NetQuantity != 0 && p.Side != "FLAT" {
				activeTradesCount++
			}
		}

		if activeTradesCount >= MaxConcurrentTrades {
			logger.Debugf("⚠️ RISK MANAGER BLOCKED ENTRY: Total active trades cap reached (%d/%d). Skipping entry for %s",
				activeTradesCount, MaxConcurrentTrades, symbol)
			return
		}

		if exitTime, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
				return
			}
		}

		if rawTick.LastPrice <= 0 {
			return
		}

		// ✅ FIX: Dynamic Capital Allocation Calculation Engine
		// Max allowed risk capital per trade based on portfolio weight restrictions
		capitalAllocationForStock := InitialCapital * MaxCapitalPerStockPct
		// Total purchasing buying power augmented by 5x MIS Intraday Leverage
		totalBuyingPowerLeveraged := capitalAllocationForStock * MaxLeverage

		// Determine exact dynamic share quantities to buy/sell
		calculatedQty := int(math.Floor(totalBuyingPowerLeveraged / rawTick.LastPrice))

		if calculatedQty <= 0 {
			logger.Warnf("⚠️ Risk Allocation Blocked Size: Calculated Qty for %s at %.2f is 0", symbol, rawTick.LastPrice)
			return
		}

		txType := "BUY"
		posSide := "LONG"
		if barSignal == "GO_SHORT" {
			txType = "SELL"
			posSide = "SHORT"
		}

		pos.NetQuantity = calculatedQty
		pos.Side = posSide
		pos.AveragePrice = rawTick.LastPrice

		logger.Infof("🚀 DYNAMIC RISK MANAGER DISPATCHING EXECUTION ORDER: %s %s Qty: %d (Leveraged Capital Invested: %.2f INR)",
			txType, symbol, calculatedQty, float64(calculatedQty)*rawTick.LastPrice)

		go rm.orderManager.PlaceOrder(context.Background(), models.OrderRequest{
			Symbol:          symbol,
			Product:         "MIS",
			TransactionType: txType,
			OrderType:       "MARKET",
			Quantity:        calculatedQty,
			UserEmail:       AgentEmail,
		})

	} else if (barSignal == "EXIT_LONG" || barSignal == "EXIT_SHORT") && pos.NetQuantity != 0 {
		if (barSignal == "EXIT_LONG" && pos.Side == "LONG") || (barSignal == "EXIT_SHORT" && pos.Side == "SHORT") {
			rm.executeBrokerOrder(symbol, pos, "Strategy Interface Mandated Direction Flip", rawTick.Timestamp)
		}
	}
}

func (rm *RiskManager) executeBrokerOrder(symbol string, pos *models.Position, reason string, timestamp time.Time) {
	if pos.NetQuantity == 0 {
		return
	}

	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	exitReq := models.OrderRequest{
		Symbol:          pos.Symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        pos.NetQuantity,
		UserEmail:       AgentEmail,
	}

	logger.Warnf("[Execution Gate] Dispatching Liquidation Ticket: %s | Reason: %s", symbol, reason)

	totalCharges := rm.CalculateItemizedCharges(pos.NetQuantity, pos.AveragePrice)
	rm.globalSummary.TotalCharges += totalCharges
	rm.dailyChargesPaid += totalCharges

	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        pos.NetQuantity,
		AveragePrice:    pos.AveragePrice,
		AllocatedCharge: totalCharges,
	})

	rm.lastExitTime[symbol] = timestamp

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0

	go rm.orderManager.PlaceOrder(context.Background(), exitReq)
}

func (rm *RiskManager) buildExitOrderRequest(pos *models.Position, exitSide string) models.OrderRequest {
	return models.OrderRequest{
		Symbol:          pos.Symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        pos.NetQuantity,
		UserEmail:       AgentEmail,
	}
}

func (rm *RiskManager) recordTransactionCosts(charges models.ItemizedCharges) {
	rm.globalSummary.Brokerage += charges.Brokerage
	rm.globalSummary.STT += charges.STT
	rm.globalSummary.ExchangeTurnoverCharge += charges.ExchangeTurnoverCharge
	rm.globalSummary.SebiTurnoverCharge += charges.SebiTurnoverCharge
	rm.globalSummary.GST += charges.GST
	rm.globalSummary.StampDuty += charges.StampDuty
	rm.globalSummary.TotalCharges += charges.TotalCharges
	rm.dailyChargesPaid += charges.TotalCharges
}
