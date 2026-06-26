package risk

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/internal/service/strategy"
	"gidh-backend/pkg/logger"
)

const (
	MaxDailyLossAllowed   = 3000.0
	InitialCapital        = 60000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.3
	AgentEmail            = "algo.trader@gidh.tech"
	MaxConcurrentTrades   = 10
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

// ProcessSequentialTick coordinates multi-strategy analytical evaluations and dispatches broker orders safely
func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName

	rm.mu.Lock()
	if rm.circuitBroken {
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

	// 🔍 MULTI-STRATEGY CONTEXT STREAM EVALUATION STEP
	// Evaluates the tick across all strategy sandboxes and returns a mapping: strategyName -> TickResult
	strategyResults := rm.strategyEngine.UpdateContext(enrichedTick, currentSide, avgPrice, netQty)

	// Loop through each strategy's evaluation results sequentially
	for strategyName, result := range strategyResults {
		tickSignal := result.Signal
		proposedState := result.State

		// 1. Evaluate Exit Signals
		if strings.HasPrefix(tickSignal, "EXIT_") {
			rm.mu.Lock()
			if pos.NetQuantity != 0 {
				rm.strategyEngine.CommitTransaction(symbol, strategyName, proposedState, tickSignal, "Intelligent_Volatility_Profit_Lock_Triggered", pos.NetQuantity)
				rm.executeBrokerOrder(symbol, pos, "Intelligent Volatility Profit Lock Triggered by "+strategyName, rawTick.Timestamp)
			}
			rm.mu.Unlock()
			return // First execution wins; halts iteration loop for this precise tick sequence
		}

		// 2. Evaluate Atomic Entry Signals
		if (tickSignal == "GO_SHORT" || tickSignal == "GO_LONG") && netQty == 0 {
			rm.mu.Lock()

			// Recalculate position entry validation parameters
			if pos.NetQuantity != 0 {
				rm.mu.Unlock()
				continue
			}

			activeTradesCount := 0
			for _, p := range rm.agentPositions {
				if p.NetQuantity != 0 && p.Side != "FLAT" {
					activeTradesCount++
				}
			}

			if activeTradesCount >= MaxConcurrentTrades {
				logger.Debugf("⚠️ RISK MANAGER BLOCKED ENTRY: Total active trades cap reached (%d/%d). Skipping entry for %s via %s",
					activeTradesCount, MaxConcurrentTrades, symbol, strategyName)
				rm.mu.Unlock()
				return
			}

			if exitTime, ok := rm.lastExitTime[symbol]; ok {
				if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
					rm.mu.Unlock()
					return
				}
			}

			if rawTick.LastPrice <= 0 {
				rm.mu.Unlock()
				return
			}

			capitalAllocationForStock := InitialCapital * MaxCapitalPerStockPct
			totalBuyingPowerLeveraged := capitalAllocationForStock * MaxLeverage
			calculatedQty := int(math.Floor(totalBuyingPowerLeveraged / rawTick.LastPrice))

			if calculatedQty <= 0 {
				logger.Warnf("⚠️ Risk Allocation Blocked Size: Calculated Qty for %s at %.2f is 0", symbol, rawTick.LastPrice)
				rm.mu.Unlock()
				return
			}

			txType := "BUY"
			posSide := "LONG"
			if tickSignal == "GO_SHORT" {
				txType = "SELL"
				posSide = "SHORT"
			}

			pos.NetQuantity = calculatedQty
			pos.Side = posSide
			pos.AveragePrice = rawTick.LastPrice

			// ✅ COMMIT STRATEGY-SPECIFIC TRANSACTION Lifecycle Update
			rm.strategyEngine.CommitTransaction(symbol, strategyName, proposedState, tickSignal, "Strategy_Entry_Condition_Met", calculatedQty)

			logger.Infof("🚀 DYNAMIC RISK MANAGER DISPATCHING EXECUTION ORDER: [%s] %s %s Qty: %d (Leveraged Capital Invested: %.2f INR)",
				strategyName, txType, symbol, calculatedQty, float64(calculatedQty)*rawTick.LastPrice)

			go rm.orderManager.PlaceOrder(context.Background(), models.OrderRequest{
				Symbol:          symbol,
				Product:         "MIS",
				TransactionType: txType,
				OrderType:       "MARKET",
				Quantity:        calculatedQty,
				UserEmail:       AgentEmail,
			})

			rm.mu.Unlock()
			return // Order dispatched successfully, cease evaluating alternative tracks on this explicit tick
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
