package main

import (
	"sync"

	"github.com/dodo-md/chimi/internal/protocol"
	"github.com/gorilla/websocket"
)

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan protocol.Message
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}
