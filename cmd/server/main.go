package main

import (
	"crypto/subtle"
	"net/http"
	"sync"

	"chimi/internal/protocol"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan protocol.Message
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan protocol.Message),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.add(conn)
		case conn := <-h.unregister:
			h.remove(conn)
		case msg := <-h.broadcast:
			h.broadcastMessage(msg)
		}
	}
}

func (h *Hub) add(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = true
}

func (h *Hub) remove(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[conn]; !ok {
		return
	}
	delete(h.clients, conn)
	_ = conn.Close()
}

func (h *Hub) broadcastMessage(msg protocol.Message) {
	h.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for conn := range h.clients {
		conns = append(conns, conn)
	}
	h.mu.Unlock()

	var failed []*websocket.Conn
	for _, conn := range conns {
		if err := conn.WriteJSON(msg); err != nil {
			failed = append(failed, conn)
		}
	}
	for _, conn := range failed {
		h.remove(conn)
	}
}

func (h *Hub) serveWS(pass string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !passOK(r.URL.Query().Get("pass"), pass) {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		h.register <- conn
		defer func() { h.unregister <- conn }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
}

func passOK(got, want string) bool {
	if want == "" || len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
