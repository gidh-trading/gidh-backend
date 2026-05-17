package analytic

import (
	"context"
	"sync"
	"time"

	gidhproto "gidh-backend/grpc"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	addr     string
	tickChan chan *models.EnrichedTick
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewClient(addr string, bufferSize int) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		addr:     addr,
		tickChan: make(chan *models.EnrichedTick, bufferSize),
		ctx:      ctx,
		cancel:   cancel,
	}

	c.wg.Add(1)
	go c.workerLoop()

	return c
}

// Forward places the tick into the channel buffer without blocking the pipeline execution path
func (c *Client) Forward(tick *models.EnrichedTick) {
	select {
	case c.tickChan <- tick:
	default:
		logger.Warnf("Analytic gRPC client buffer full, dropping tick for %s", tick.Raw.StockName)
	}
}

func (c *Client) workerLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			logger.Infof("Connecting to Python gRPC analysis server at %s...", c.addr)
			conn, err := grpc.DialContext(c.ctx, c.addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err != nil {
				logger.Errorf("gRPC connection failed: %v. Retrying in 5s...", err)
				time.Sleep(5 * time.Second)
				continue
			}

			client := gidhproto.NewAnalyticIngestorClient(conn)
			stream, err := client.StreamEnrichedTicks(c.ctx)
			if err != nil {
				logger.Errorf("Failed to open gRPC data stream: %v. Reconnecting...", err)
				conn.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			logger.Info("gRPC streaming connection to Python successfully established")

			// Push data loop
			err = c.streamTicks(stream)

			// Cleanup current connection on failure
			stream.CloseSend()
			conn.Close()

			if err != nil {
				logger.Errorf("gRPC stream interrupted: %v. Initiating reconnect...", err)
				time.Sleep(2 * time.Second)
			}
		}
	}
}

func (c *Client) streamTicks(stream gidhproto.AnalyticIngestor_StreamEnrichedTicksClient) error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case tick, ok := <-c.tickChan:
			if !ok {
				return nil
			}

			var poc, vah, val float64
			if tick.VolProfile != nil {
				poc = tick.VolProfile.POC
				vah = tick.VolProfile.VAH
				val = tick.VolProfile.VAL
			}

			msg := &gidhproto.EnrichedTickMessage{
				Timestamp:       timestamppb.New(tick.Raw.Timestamp),
				InstrumentToken: tick.Raw.InstrumentToken,
				StockName:       tick.Raw.StockName,
				LastPrice:       tick.Raw.LastPrice,
				TickVolume:      tick.TickVolume,
				Vwap:            tick.Raw.AverageTradedPrice,
				Poc:             poc,
				Vah:             vah,
				Val:             val,
			}

			if err := stream.Send(msg); err != nil {
				// Put back the item into channel if possible to preserve sequential continuity
				select {
				case c.tickChan <- tick:
				default:
				}
				return err
			}
		}
	}
}

func (c *Client) Close() {
	c.cancel()
	close(c.tickChan)
	c.wg.Wait()
	logger.Info("Analytic gRPC client stopped completely")
}
