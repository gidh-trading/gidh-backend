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
}
