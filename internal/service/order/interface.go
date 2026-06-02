package order

import (
	"context"
	"gidh-backend/internal/service/models"
	"time"
)

type PositionManager interface {
	// Pure Entry/Routing Layer
	PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error)
	ModifyOrder(orderID string, newPrice float64, userEmail string) error
	CancelOrder(orderID string, userEmail string) error

	// Real-Time Evaluation Loop
	OnPriceUpdate(symbol string, ltp float64, ts time.Time)

	// Local Memory Position Risk Mutators
	UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error
	ExitPosition(ctx context.Context, symbol string, product string, quantity int, userEmail string) error

	// State Inspection Getters
	GetPosition(symbol string, product string) (*models.Position, bool)
	GetOrders(symbol string) []models.OrderBookEntry
	GetAllPositions() []models.Position
	ClearPositions()
}
