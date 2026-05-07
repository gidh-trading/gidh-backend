package ws

import (
	"encoding/json"
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

// internal/service/ws/hub.go

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			// No need for 'if ok' check here, removeClient handles it
			h.removeClient(client)

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
			clients, ok := h.subscriptions[msg.Key]
			if !ok {
				h.mu.RUnlock()
				continue
			}

			var slowClients []*Client
			for client := range clients {
				select {
				case client.send <- msg.Payload:
					// Successfully sent
				default:
					// Buffer is full, mark for removal
					slowClients = append(slowClients, client)
				}
			}
			h.mu.RUnlock()

			// Remove slow clients after releasing the RLock to avoid deadlock
			for _, client := range slowClients {
				h.removeClient(client)
			}
		}
	}
}

func (h *Hub) removeClient(c *Client) {
	h.mu.Lock() // Ensure we have a write lock for cleanup
	defer h.mu.Unlock()

	// Check if already removed to avoid double-closing
	if _, ok := h.clients[c]; !ok {
		return
	}

	delete(h.clients, c)
	close(c.send)

	// Remove the client from all subscription buckets
	for key, subs := range h.subscriptions {
		delete(subs, c)
		// Clean up empty subscription keys to save memory
		if len(subs) == 0 {
			delete(h.subscriptions, key)
		}
	}
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

func (h *Hub) BroadcastJSON(key string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Errorf("WS Marshal Error: %v", err)
		return
	}

	// Send to the broadcast channel defined in Hub
	h.Broadcast <- Message{
		Key:     key,
		Payload: data,
	}
}
