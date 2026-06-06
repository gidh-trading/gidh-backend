package agent

import (
	"gidh-backend/pkg/logger"
	"sync"
)

type TargetCoordinates struct {
	Symbol          string
	Side            string
	StopLossPrice   float64
	TakeProfitPrice float64
}

var (
	vaultMutex   sync.RWMutex
	TargetLedger = make(map[string]*TargetCoordinates)
)

// StoreTargets puts the raw numbers provided by Engineering straight into the memory vault
func (rm *RiskManager) StoreTargets(symbol string, side string, stopLoss float64, takeProfit float64) {
	vaultMutex.Lock()
	defer vaultMutex.Unlock()

	TargetLedger[symbol] = &TargetCoordinates{
		Symbol:          symbol,
		Side:            side,
		StopLossPrice:   stopLoss,
		TakeProfitPrice: takeProfit,
	}

	logger.Infof("[Vault Clerk] Logged Execution Targets for %s. Stop-Loss: ₹%.2f | Take-Profit: ₹%.2f",
		symbol, stopLoss, takeProfit)
}

// CheckBracketTriggers audits the live price against the stored numbers
func (rm *RiskManager) CheckBracketTriggers(symbol string, currentPrice float64) bool {
	vaultMutex.RLock()
	targets, exists := TargetLedger[symbol]
	vaultMutex.RUnlock()

	if !exists || targets == nil {
		return false
	}

	if targets.Side == "LONG" {
		if currentPrice <= targets.StopLossPrice {
			logger.Warnf("[Vault Clerk] STOP LOSS BREACHED for %s. Price (₹%.2f) dropped below target (₹%.2f)", symbol, currentPrice, targets.StopLossPrice)
			rm.ClearTargetMemory(symbol)
			return true
		}
		if currentPrice >= targets.TakeProfitPrice {
			logger.Infof("[Vault Clerk] TAKE PROFIT ACHIEVED for %s. Price (₹%.2f) broke past target (₹%.2f)", symbol, currentPrice, targets.TakeProfitPrice)
			rm.ClearTargetMemory(symbol)
			return true
		}
	} else if targets.Side == "SHORT" {
		if currentPrice >= targets.StopLossPrice {
			logger.Warnf("[Vault Clerk] SHORT STOP BREACHED for %s. Squeeze Price (₹%.2f) rose above target (₹%.2f)", symbol, currentPrice, targets.StopLossPrice)
			rm.ClearTargetMemory(symbol)
			return true
		}
		if currentPrice <= targets.TakeProfitPrice {
			logger.Infof("[Vault Clerk] SHORT PROFIT MET for %s. Target price achieved (₹%.2f <= ₹%.2f)", symbol, currentPrice, targets.TakeProfitPrice)
			rm.ClearTargetMemory(symbol)
			return true
		}
	}

	return false
}

func (rm *RiskManager) ClearTargetMemory(symbol string) {
	vaultMutex.Lock()
	delete(TargetLedger, symbol)
	vaultMutex.Unlock()
}
