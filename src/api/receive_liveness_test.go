package api

import (
	"testing"
	"time"
)

// A consumer that stops answering pings (host asleep, network path gone) must
// be detached; in manual mode signal-cli keeps receiving while it is attached.
func TestManualDetachesAWebSocketThatStopsAnsweringPings(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("waits for the wrapper's pong timeout")
	}
	f, url := startManualAPI(t)
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	// Never reading means gorilla never answers the wrapper's pings.
	if _, ok := f.next("unsubscribeReceive", pingPeriod+pongWait+5*time.Second); !ok {
		t.Fatalf("a WebSocket that answered no ping for %s is still attached", pingPeriod+pongWait)
	}
}

func TestManualKeepsAWebSocketThatAnswersPings(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("waits past the wrapper's pong timeout")
	}
	f, url := startManualAPI(t)
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	go func() {
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()
	if _, ok := f.next("unsubscribeReceive", pingPeriod+pongWait+5*time.Second); ok {
		t.Fatal("a WebSocket that answered every ping was detached")
	}
}
