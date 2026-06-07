package risk

import (
	"context"
	"fmt"
	"gidh-backend/pkg/logger"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/internal/service/scalper" // Pointing to your clean, new package
)

const (
	MaxDailyLossAllowed   = 5000.0
	InitialCapital        = 100000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.25
	AgentEmail            = "agent@gidh.trading"
)

type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	scalperEngine  *scalper.Engine
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
	lastExitTime   map[string]time.Time

	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

func NewRiskManager(om order.PositionManager, se *scalper.Engine) *RiskManager {
	return &RiskManager{
		orderManager:   om,
		scalperEngine:  se,
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

	// Step 1: Push context straight into the Scalper's data layer cache instantly
	rm.scalperEngine.UpdateContext(enrichedTick)

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

	// Ask the Sequential 4-Method Core for its definitive response string
	signal := rm.scalperEngine.GenerateSignal(symbol, currentSide, avgPrice, netQty)
	if signal == "HOLD" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Entry Router Track
	if (signal == "GO_SHORT" || signal == "GO_LONG") && pos.NetQuantity == 0 {
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
		if signal == "GO_SHORT" {
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

		// Exit Router Track
	} else if (signal == "EXIT_LONG" || signal == "EXIT_SHORT") && pos.NetQuantity != 0 {
		if (signal == "EXIT_LONG" && pos.Side == "LONG") || (signal == "EXIT_SHORT" && pos.Side == "SHORT") {
			rm.executeBrokerOrder(symbol, pos, "Strategy Interface Mandated Exit Triggered", rawTick.Timestamp)
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
