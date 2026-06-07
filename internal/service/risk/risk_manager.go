package risk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/internal/service/scalper" // Pointing to your clean, new package
	"gidh-backend/pkg/logger"
)

const (
	MaxDailyLossAllowed   = 5000.0
	InitialCapital        = 100000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.25
	AgentEmail            = "agent@gidh.trading"
	FixedProfitTargetINR  = 1000.0 // Hardcoded cash win limit
)

// UIContractNotePayload aggregates session summaries for front-end charts.
type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

// RiskManager serves as an isolated financial guardrail container.
type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	scalperEngine  *scalper.Engine // Interacts strictly with your modular scalper interface
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
	lastExitTime   map[string]time.Time

	// UI Telemetry and Strategy Performance Variables
	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

// NewRiskManager instantiates your brand-new detached finance department component.
func NewRiskManager(om order.PositionManager, se *scalper.Engine) *RiskManager {
	return &RiskManager{
		orderManager:     om,
		scalperEngine:    se,
		agentPositions:   make(map[string]*models.Position),
		lastExitTime:     make(map[string]time.Time),
		dailyRealized:    0.0,
		dailyChargesPaid: 0.0,
		circuitBroken:    false,
		executedTrades:   make([]models.BacktestExecutedTrade, 0),
		globalSummary:    models.ItemizedCharges{},
	}
}

// ProcessSequentialTick coordinates data collection updates safely across package layers.
func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName
	key := fmt.Sprintf("%s:MIS", symbol)

	// Step 1: Push context straight into the Scalper's data layer cache instantly
	rm.scalperEngine.UpdateContext(
		symbol,
		rawTick.LastPrice,
		rawTick.Timestamp,
		enrichedTick.Enrichment.Direction,
		enrichedTick.Enrichment.VolumeRank,
		enrichedTick.Enrichment.PriceRank,
	)

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

	// ------------------------------------------------------------------------
	// STANDALONE CASH MONITORING & AUTO-LIQUIDATION RECOGNITION LOCK
	// ------------------------------------------------------------------------
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}

		// Map net cash profits right now independently of stock asset types
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
		totalNetSessionPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		// FINANCE CORE EXIT RULE: Check if open trade hit your targeted cash milestone
		if pos.UnrealizedPnL >= FixedProfitTargetINR {
			logger.Infof("[Finance Dept] TARGET REACHED: %s hit the ₹%.2f profit threshold limit. Liquidating.",
				symbol, FixedProfitTargetINR)

			rm.executeBrokerOrder(symbol, pos, "Hardcoded ₹1000 Cash Profit Target Met", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}

		// Account-Wide Global Protective Drawdown Boundary
		if totalNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Finance Dept] EMERGENCY STOP: Session drawdown limits breached (₹%.2f). Hard locking engine.", totalNetSessionPnL)
			rm.circuitBroken = true
			rm.executeBrokerOrder(symbol, pos, "Global Capital Drawdown Veto Actuated", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}

		// Forced Settlement Cut-off Boundary (3:15 PM IST)
		loc, _ := time.LoadLocation("Asia/Kolkata")
		if rawTick.Timestamp.In(loc).Hour() == 15 && rawTick.Timestamp.In(loc).Minute() >= 15 {
			rm.executeBrokerOrder(symbol, pos, "Intraday Force Auto-Squareoff Threshold Reached", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}
	}

	currentSide := pos.Side
	rm.mu.Unlock()

	// ------------------------------------------------------------------------
	// INTERROGATING INTERCHANGEABLE STRATEGY ROSTERS FOR ACTION CODES
	// ------------------------------------------------------------------------
	signal := rm.scalperEngine.GenerateSignal(symbol, currentSide)
	if signal == "HOLD" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// ------------------------------------------------------------------------
	// SEALS EXECUTION & ENFORCES LOGGING Overheads
	// ------------------------------------------------------------------------
	if (signal == "GO_SHORT" || signal == "GO_LONG") && pos.NetQuantity == 0 {
		// Defend against rapid sequential execution whipsaws
		if exitTime, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
				return
			}
		}

		// Calculate capital availability and statutory cost projections
		allowedQty, _ := rm.CalculatePositionSizeAndFees(symbol, rawTick.LastPrice)
		if allowedQty <= 0 {
			return
		}

		txType := "BUY"
		posSide := "LONG"
		if signal == "GO_SHORT" {
			txType = "SELL"
			posSide = "SHORT"
		}

		orderReq := models.OrderRequest{
			Symbol:          symbol,
			Product:         "MIS",
			TransactionType: txType,
			OrderType:       "MARKET",
			Quantity:        allowedQty,
			UserEmail:       AgentEmail,
		}

		pos.NetQuantity = allowedQty
		pos.Side = posSide
		pos.AveragePrice = rawTick.LastPrice
		pos.UnrealizedPnL = 0.0

		// UI Plotter Sync: Calculate mathematical visual targets for your chart scripts
		var visualTargetPrice float64
		priceOffsetNeeded := FixedProfitTargetINR / float64(allowedQty)
		if posSide == "LONG" {
			visualTargetPrice = rawTick.LastPrice + priceOffsetNeeded
		} else {
			visualTargetPrice = rawTick.LastPrice - priceOffsetNeeded
		}

		// Notify paper pipelines or visualization panels to plot profit brackets smoothly
		go func(sym string, target float64) {
			time.Sleep(10 * time.Millisecond)
			rm.orderManager.UpdatePositionMetadata(sym, "MIS", target, 0.0)
		}(symbol, visualTargetPrice)

		go rm.orderManager.PlaceOrder(context.Background(), orderReq)

	} else if (signal == "EXIT_LONG" || signal == "EXIT_SHORT") && pos.NetQuantity != 0 {
		// SCALPER GLOBAL EXIT: If the trading desk notes technical setup breakdowns, liquidate early
		if (signal == "EXIT_LONG" && pos.Side == "LONG") || (signal == "EXIT_SHORT" && pos.Side == "SHORT") {
			rm.executeBrokerOrder(symbol, pos, "Scalper Technical Context Exit Triggered", rawTick.Timestamp)
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

	exitReq := rm.buildExitOrderRequest(pos, exitSide)

	// Dynamically track post-trade itemized exchange fees to capture accurate metrics reports
	totalCharges := rm.CalculateItemizedCharges(pos.NetQuantity, pos.AveragePrice)

	// Log complete execution archives for backtest reviews
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
	rm.dailyRealized += pos.UnrealizedPnL

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

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
