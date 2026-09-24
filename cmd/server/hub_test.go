package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"chimi/internal/protocol"
	"github.com/gorilla/websocket"
)

func TestHubLifecycle(t *testing.T) {
	h := newHub()
	go h.run()

	const pass = "secret"
	srv := httptest.NewServer(h.serveWS(pass))
	defer srv.Close()

	wsURL := "ws" + srv.URL[len("http"):] + "?pass=" + pass
	dial := func() *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return conn
	}

	a := dial()
	defer a.Close()
	b := dial()
	defer b.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		n := len(h.clients)
		h.mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	msg := protocol.NewMessage("a", "selam")
	h.broadcast <- msg

	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got protocol.Message
	if err := b.ReadJSON(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Content != "selam" || got.Sender != "a" {
		t.Fatalf("got %+v", got)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.unregister <- a
	}()
	wg.Wait()
}

func TestServeWSAuth(t *testing.T) {
	h := newHub()
	go h.run()

	const pass = "secret"
	srv := httptest.NewServer(h.serveWS(pass))
	defer srv.Close()

	base := "ws" + srv.URL[len("http"):]

	_, resp, err := websocket.DefaultDialer.Dial(base+"?pass=wrong", nil)
	if err == nil {
		t.Fatal("wrong password was accepted")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %v", resp)
	}

	_, resp, err = websocket.DefaultDialer.Dial(base, nil)
	if err == nil {
		t.Fatal("missing password was accepted")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %v", resp)
	}

	conn, _, err := websocket.DefaultDialer.Dial(base+"?pass="+pass, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
}
