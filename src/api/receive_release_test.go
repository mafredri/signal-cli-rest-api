package api

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// handleSignalReceive returned on a WebSocket write error without calling
// RemoveReceiveChannel. The subscription's refcount then never reached zero,
// so signal-cli kept receiving (and acknowledging) with no consumer attached.
func TestManualReleasesSubscriptionWhenTheWebSocketWriteFails(t *testing.T) {
	f, url := startManualAPI(t)
	ws, _, err := websocket.DefaultDialer.Dial(strings.Replace(url, "/v1/receive/", "/test/handle-receive/", 1), nil)
	if err != nil {
		t.Fatal(err)
	}
	req, ok := f.next("subscribeReceive", 5*time.Second)
	if !ok {
		t.Fatal("no subscribeReceive for a new WebSocket")
	}
	f.respond(t, req, 0)
	// Abort the connection, so writing to it fails.
	ws.UnderlyingConn().(*net.TCPConn).SetLinger(0)
	ws.UnderlyingConn().Close()
	deadline := time.After(5 * time.Second)
	for timestamp := int64(1); ; timestamp++ {
		f.notify(t, timestamp)
		select {
		case req := <-f.requests:
			if req.Method == "unsubscribeReceive" {
				f.respond(t, req, nil)
				return
			}
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("writing to the WebSocket failed but the wrapper never unsubscribed; signal-cli keeps receiving and the wrapper drops every message")
		}
	}
}

// When the wrapper cannot attach a receive channel, it closes the WebSocket
// instead of leaving it open with nothing to deliver.
func TestManualClosesTheWebSocketWhenSubscribingFails(t *testing.T) {
	f, url := startManualAPI(t)
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	req, ok := f.next("subscribeReceive", 2*time.Second)
	if !ok {
		t.Fatal("no subscribeReceive for a new WebSocket")
	}
	f.respondError(t, req, -32602, "Specified account does not exist")
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = ws.ReadMessage()
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the WebSocket stayed open after subscribing failed")
	}
	if err == nil {
		t.Fatal("received a message after subscribing failed")
	}
}
