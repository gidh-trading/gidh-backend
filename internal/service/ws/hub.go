package ws

import (
	"gidh-backend/pkg/logger"
	"sync"
)

type Hub struct {
	// Registered clients
	clients map[*Client]bool

	// Subscriptions: map["symbol:interval"] -> map[client]bool
	subscriptions map[string]map[*Client]bool

	// Inbound messages from the pipeline to be broadcasted
	Broadcast chan Message

	// Register/Unregister requests from clients
	register   chan *Client
	unregister chan *Client

	// Subscription changes from clients
	subscribe   chan subRequest
	unsubscribe chan subRequest

	mu sync.RWMutex
}

type Message struct {
	Key     string // "symbol:interval"
	Payload []byte
}

type subRequest struct {
	client   *Client
	symbol   string
	interval string
}

func NewHub() *Hub {
	return &Hub{
		Broadcast:     make(chan Message),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		subscribe:     make(chan subRequest),
		unsubscribe:   make(chan subRequest),
		clients:       make(map[*Client]bool),
		subscriptions: make(map[string]map[*Client]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				h.removeClient(client)
			}

		case req := <-h.subscribe:
			key := req.symbol + ":" + req.interval
			h.mu.Lock()
			if h.subscriptions[key] == nil {
				h.subscriptions[key] = make(map[*Client]bool)
			}
			h.subscriptions[key][req.client] = true
			h.mu.Unlock()

		case msg := <-h.Broadcast:
			h.mu.RLock()
			if clients, ok := h.subscriptions[msg.Key]; ok {
				for client := range clients {
					select {
					case client.send <- msg.Payload:
					default:
						h.removeClient(client)
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) removeClient(c *Client) {
	delete(h.clients, c)
	close(c.send)
}

func (h *Hub) Stop() {
	// 1. Close the register/unregister channels to stop new activity
	// 2. Loop through active clients and close their send channels
	h.mu.Lock() // Ensure you have a mutex on your client map
	defer h.mu.Unlock()

	for client := range h.clients {
		close(client.send)
		delete(h.clients, client)
	}
	logger.Info("WebSocket Hub stopped: all client connections closed.")
}
