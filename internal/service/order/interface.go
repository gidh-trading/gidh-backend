package order

import (
	"context"
	"gidh-backend/internal/service/models"
)

type PositionManager interface {
	PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error)
}
