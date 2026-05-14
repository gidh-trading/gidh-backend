package order

import (
	"context"
	"gidh-backend/internal/service/models"
)

type PositionManager interface {
	PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error)
	GetPosition(symbol string, product string) (*models.Position, bool)
	OnPriceUpdate(symbol string, ltp float64)
	GetOrders(symbol string) []models.OrderBookEntry
	GetAllPositions() []models.Position
	ClearPositions()

	UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error
	ModifyOrder(orderID string, newPrice float64, newTP float64, newSL float64) error
	CancelOrder(orderID string) error
	ExitPosition(ctx context.Context, symbol string, product string, quantity int) error
}
