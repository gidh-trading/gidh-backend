package analytic

import (
	"context"
	"io"
	"sync"
	"time"

	gidhproto "gidh-backend/grpc"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
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

	// Dependency Injection to forward anomalies right back down to system channels
	dbWriter    *writer.DBWriter
	wsHub       *ws.Hub
	tokenToName map[uint32]string
}

func NewClient(addr string, bufferSize int, db *writer.DBWriter, hub *ws.Hub, tokenMap map[uint32]string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		addr:        addr,
		tickChan:    make(chan *models.EnrichedTick, bufferSize),
		ctx:         ctx,
		cancel:      cancel,
		dbWriter:    db,
		wsHub:       hub,
		tokenToName: tokenMap,
	}
}

func (c *Client) Start() {
	c.wg.Add(1)
	go c.workerLoop()
}

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

			logger.Info("gRPC bidirectional streaming connection successfully established")

			// Deploy a dedicated reading context loop to capture generator yields from Python
			readWg := sync.WaitGroup{}
			readWg.Add(1)
			go func() {
				defer readWg.Done()
				c.receiveAnomaliesLoop(stream)
			}()

			// Run data write push loop on the main worker context thread
			_ = c.streamTicks(stream)

			// Clean tear-down on connection break signals
			stream.CloseSend()
			readWg.Wait() // Ensure reader thread terminates cleanly
			conn.Close()

			select {
			case <-c.ctx.Done():
				return
			default:
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

			buyDepthMsg := make([]*gidhproto.DepthLevel, len(tick.Raw.Depth.Buy))
			for i, lvl := range tick.Raw.Depth.Buy {
				buyDepthMsg[i] = &gidhproto.DepthLevel{Price: lvl.Price, Quantity: lvl.Quantity, Orders: int32(lvl.Orders)}
			}

			sellDepthMsg := make([]*gidhproto.DepthLevel, len(tick.Raw.Depth.Sell))
			for i, lvl := range tick.Raw.Depth.Sell {
				sellDepthMsg[i] = &gidhproto.DepthLevel{Price: lvl.Price, Quantity: lvl.Quantity, Orders: int32(lvl.Orders)}
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
				BuyDepth:        buyDepthMsg,
				SellDepth:       sellDepthMsg,
			}

			if err := stream.Send(msg); err != nil {
				select {
				case c.tickChan <- tick:
				default:
				}
				return err
			}
		}
	}
}

// Asynchronous listener method parsing responses streamed back from Python
func (c *Client) receiveAnomaliesLoop(stream gidhproto.AnalyticIngestor_StreamEnrichedTicksClient) {
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			logger.Info("Python analytics channel closed stream loop normally.")
			return
		}
		if err != nil {
			logger.Errorf("Error receiving payload from bidirectional stream pipe: %v", err)
			return
		}

		c.processIncomingAnomaly(response)
	}
}

func (c *Client) processIncomingAnomaly(res *gidhproto.AnomalyResponse) {
	ts := res.Timestamp.AsTime().Local()
	symbol := c.tokenToName[res.InstrumentToken]
	if symbol == "" {
		symbol = "UNKNOWN"
	}

	// 1. UI Broadcasting layer across WebSocket channels
	if c.wsHub != nil {
		payload := map[string]any{
			"type": "anomaly_alert",
			"data": map[string]any{
				"anomaly_type":     res.AnomalyType,
				"timestamp":        ts.Format("2006-01-02 15:04:05"),
				"instrument_token": res.InstrumentToken,
				"stock_name":       symbol,
				"price":            res.Price,
				"buy_volume":       res.BuyVolume,
				"sell_volume":      res.SellVolume,
				"total_volume":     res.TotalVolume,
				"peak_z_score":     res.PeakZScore,
				"tick_count":       res.TickCount,
				"cluster_vwap":     res.ClusterVwap,
			},
		}
		// Broadcast onto the main operational trading feed layout
		c.wsHub.BroadcastJSON("global:trading", payload)
		// Mirror down onto specific asset tracking channels
		c.wsHub.BroadcastJSON(symbol+":1m", payload)
	}

	// 2. Database layer persistence routing based on message layout type
	if c.dbWriter != nil {
		if res.AnomalyType == "WHALE_BLOCK" {
			// Calculate side flag parameters
			side := "BUY"
			if res.SellVolume > res.BuyVolume {
				side = "SELL"
			}

			c.dbWriter.AddWhaleBlock(models.WhaleBlockRecord{
				Timestamp:       ts,
				InstrumentToken: res.InstrumentToken,
				Price:           res.Price,
				Volume:          res.TotalVolume,
				Side:            side,
				VExpected:       1.0, // Baseline modifier value
			})
		} else if res.AnomalyType == "GRID_CLUSTER" {
			c.dbWriter.AddAnomalyGrid(models.AnomalyGridRecord{
				TimeBin:         ts,
				InstrumentToken: res.InstrumentToken,
				PriceBin:        res.Price,
				BuyVolume:       res.BuyVolume,
				SellVolume:      res.SellVolume,
				TotalVolume:     res.TotalVolume,
				PeakZScore:      res.PeakZScore,
				TickCount:       res.TickCount,
				ClusterVWAP:     res.ClusterVwap,
			})
		}
	}
}

func (c *Client) Close() {
	c.cancel()
	close(c.tickChan)
	c.wg.Wait()
	logger.Info("Analytic gRPC client stopped completely")
}
