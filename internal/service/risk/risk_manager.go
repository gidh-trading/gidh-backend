package risk

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/strategy"
	"gidh-backend/pkg/logger"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

const (
	MaxDailyLossAllowed   = 1000.0
	InitialCapital        = 50000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.50
	AgentEmail            = "algo.trader@gidh.tech"
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
	key := fmt.Sprintf("%s:MIS", symbol)

	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// ⚡ STATE RECOVERY: MANUAL INTERVENTION & OVER-THE-AIR LIMIT FILL SAFETY SYNC
	// If you manually squared off the asset, or a resting order filled silently,
	// pos.NetQuantity will be 0, but our tracking memory might still show "LONG" or "SHORT".
	// We force a localized memory sync here to prevent stale signals or trade collisions.
	if pos.NetQuantity == 0 && pos.Side != "FLAT" {
		pos.Side = "FLAT"
		pos.AveragePrice = 0.0
	}

	// Account-Level Global Drawdown Circuit Breaker
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

	// 🚨 STEP 1: Process real-time tick calculations and check trailing profit locks
	tickSignal := rm.strategyEngine.UpdateContext(enrichedTick, currentSide, avgPrice, netQty)

	// ⚡ CRITICAL TRAILING HIERARCHY OVERRIDE
	// If the real-time tick says the profit lock was breached, we liquidate instantly.
	// We return early to ensure bar-close indicator flips can never overwrite this exit.
	if tickSignal == "EXIT_LONG" || tickSignal == "EXIT_SHORT" {
		rm.mu.Lock()
		if pos.NetQuantity != 0 {
			// Explicitly passing the exit reason lets your database logs categorize this perfectly
			rm.executeBrokerOrder(symbol, pos, "Intelligent Volatility Profit Lock Triggered", rawTick.Timestamp)
		}
		rm.mu.Unlock()
		return // Block any further processing for this tick packet!
	}

	// STEP 2: Evaluate standard bar-close triggers only if trailing locks are clear
	barSignal := rm.strategyEngine.GenerateSignal(symbol, currentSide, avgPrice, netQty)
	if barSignal == "HOLD" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Entry Order Logic
	if (barSignal == "GO_SHORT" || barSignal == "GO_LONG") && pos.NetQuantity == 0 {
		if exitTime, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
				return
			}
		}

		allowedQty, _ := rm.CalculatePositionSizeAndFees(symbol, rawTick.LastPrice)
		if allowedQty <= 0 {
			return
		}

		txType := "BUY"
		posSide := "LONG"
		if barSignal == "GO_SHORT" {
			txType = "SELL"
			posSide = "SHORT"
		}

		pos.NetQuantity = allowedQty
		pos.Side = posSide
		pos.AveragePrice = rawTick.LastPrice

		go rm.orderManager.PlaceOrder(context.Background(), models.OrderRequest{
			Symbol:          symbol,
			Product:         "MIS",
			TransactionType: txType,
			OrderType:       "MARKET",
			Quantity:        allowedQty,
			UserEmail:       AgentEmail,
		})

		// Closed Bar Structural Exit Logic
	} else if (barSignal == "EXIT_LONG" || barSignal == "EXIT_SHORT") && pos.NetQuantity != 0 {
		if (barSignal == "EXIT_LONG" && pos.Side == "LONG") || (barSignal == "EXIT_SHORT" && pos.Side == "SHORT") {
			rm.executeBrokerOrder(symbol, pos, "Strategy Interface Mandated Direction Flip", rawTick.Timestamp)
		}
	}
}

// executeBrokerOrder clears local memory entries and tracks systemic itemized friction.
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

	// Synchronously close local properties immediately while inside the mutex loop
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
