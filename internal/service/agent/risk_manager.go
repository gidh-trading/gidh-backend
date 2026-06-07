package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/pkg/logger"
)

const (
	MaxDailyLossAllowed   = 5000.0
	InitialCapital        = 100000.0
	MaxLeverage           = 5.0
	MaxCapitalPerStockPct = 0.25
	AgentEmail            = "agent@gidh.trading"
)

type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	scalper        *ScalperAgent
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
	lastExitTime   map[string]time.Time
	takeProfitHit  map[string]bool

	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:     om,
		scalper:          sa,
		agentPositions:   make(map[string]*models.Position),
		lastExitTime:     make(map[string]time.Time),
		takeProfitHit:    make(map[string]bool),
		dailyRealized:    0.0,
		dailyChargesPaid: 0.0,
		circuitBroken:    false,
		executedTrades:   make([]models.BacktestExecutedTrade, 0),
	}
}

func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rm.scalper.UpdateMicroContext(enrichedTick)

	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName
	key := fmt.Sprintf("%s:MIS", symbol)

	if rm.takeProfitHit[symbol] {
		rm.mu.Unlock()
		return
	}

	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// ------------------------------------------------------------------------
	// REAL-TIME ACCOUNT-WIDE CAPITAL AUDITING
	// ------------------------------------------------------------------------
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier
		totalNetSessionPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		// Global Maximum Protective Drawdown Cap
		if totalNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Finance Dept] CRITICAL: Max Session Drawdown Breached (₹%.2f). Liquidating assets.", totalNetSessionPnL)
			rm.circuitBroken = true
			rm.executeBrokerOrder(symbol, pos, "Global Capital Drawdown Veto Actuated", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}

		// Exchange-Forced Settlement Cut-off Time (3:15 PM IST)
		loc, _ := time.LoadLocation("Asia/Kolkata")
		if rawTick.Timestamp.In(loc).Hour() == 15 && rawTick.Timestamp.In(loc).Minute() >= 15 {
			rm.executeBrokerOrder(symbol, pos, "Intraday Force Auto-Squareoff Threshold Reached", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}
	}

	currentSide := pos.Side
	entryPrice := pos.AveragePrice
	rm.mu.Unlock()

	// ------------------------------------------------------------------------
	// CONSULT ENGINEERING FOR DIRECTION SIGNALS (ENTRIES AND EXITS)
	// ------------------------------------------------------------------------
	signal := rm.scalper.GenerateSignal(symbol, currentSide, entryPrice)
	if signal == "HOLD" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// ------------------------------------------------------------------------
	// ORDER EXECUTION PIPELINE
	// ------------------------------------------------------------------------
	if (signal == "GO_SHORT" || signal == "GO_LONG") && pos.NetQuantity == 0 {
		if exitTime, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
				return
			}
		}

		allowedQty, predictedFees := rm.CalculatePositionSizeAndFees(symbol, rawTick.LastPrice)
		if allowedQty <= 0 {
			return
		}

		if rm.dailyRealized-(rm.dailyChargesPaid+predictedFees) <= -MaxDailyLossAllowed {
			logger.Warnf("[Finance Dept] Vetoed %s entry. Costs push account beyond drawdown allowance.", symbol)
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

		rm.LogTransactionFees(allowedQty, rawTick.LastPrice, txType, predictedFees, rawTick.Timestamp)

		pos.NetQuantity = allowedQty
		pos.Side = posSide
		pos.AveragePrice = rawTick.LastPrice
		pos.UnrealizedPnL = 0.0

		go rm.orderManager.PlaceOrder(context.Background(), orderReq)

	} else if (signal == "EXIT_LONG" || signal == "EXIT_SHORT") && pos.NetQuantity != 0 {
		// Verify signal side corresponds with active position state before clearing
		if (signal == "EXIT_LONG" && pos.Side == "LONG") || (signal == "EXIT_SHORT" && pos.Side == "SHORT") {
			rm.executeBrokerOrder(symbol, pos, "Scalper Technical Context Exit Triggered", rawTick.Timestamp)
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

	logger.Warnf("[Finance Dept] ORDER DISPATCHED: %s | Reason: %s", symbol, reason)

	charges := rm.CalculateItemizedCharges(pos.NetQuantity, pos.AveragePrice)
	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        pos.NetQuantity,
		AveragePrice:    pos.AveragePrice,
		AllocatedCharge: charges.TotalCharges,
	})

	if rm.scalper != nil {
		rm.scalper.RegisterPositionClosure(symbol, timestamp)
	}

	rm.lastExitTime[symbol] = timestamp
	rm.dailyRealized += pos.UnrealizedPnL

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

	go rm.orderManager.PlaceOrder(context.Background(), exitReq)
}
