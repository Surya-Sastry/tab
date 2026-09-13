package httpapi

import (
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu     sync.RWMutex
	groups map[string]map[*websocket.Conn]struct{}
}

func NewHub() *Hub {
	return &Hub{groups: make(map[string]map[*websocket.Conn]struct{})}
}

func (h *Hub) Join(groupID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.groups[groupID] == nil {
		h.groups[groupID] = make(map[*websocket.Conn]struct{})
	}
	h.groups[groupID][conn] = struct{}{}
}

func (h *Hub) Leave(groupID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.groups[groupID], conn)
	if len(h.groups[groupID]) == 0 {
		delete(h.groups, groupID)
	}
}

func (h *Hub) Broadcast(groupID string, value any) {
	body, _ := json.Marshal(value)
	h.mu.RLock()
	connections := make([]*websocket.Conn, 0, len(h.groups[groupID]))
	for conn := range h.groups[groupID] {
		connections = append(connections, conn)
	}
	h.mu.RUnlock()
	for _, conn := range connections {
		_ = conn.WriteMessage(websocket.TextMessage, body)
	}
}
