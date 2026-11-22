package api

import (
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gorilla/websocket"
)

const (
	testPongWait   = time.Second
	testPingPeriod = 50 * time.Millisecond
)

func withPongTimeout(timeout time.Duration) func(*Api) {
	return func(a *Api) {
		a.SetWebSocketPongTimeout(timeout)
		a.pingPeriod = testPingPeriod
	}
}

// readAll keeps reading, which makes gorilla answer pings unless the ping
// handler is replaced, and reports when the connection ends.
func readAll(ws *websocket.Conn) <-chan struct{} {
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()
	return ended
}

// pinged replaces the ping handler with one that reports each ping and
// answers it only if answer is true.
func pinged(ws *websocket.Conn, answer bool) <-chan struct{} {
	pings := make(chan struct{}, 1)
	ws.SetPingHandler(func(data string) error {
		select {
		case pings <- struct{}{}:
		default:
		}
		if !answer {
			return nil
		}
		return ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	return pings
}

func waitForPing(t *testing.T, pings <-chan struct{}) {
	t.Helper()
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("the server sent no ping")
	}
}

// A consumer that stops answering pings (host asleep, network path gone) is
// detached; in manual mode signal-cli keeps receiving while it is attached.
func TestManualDetachesAWebSocketThatStopsAnsweringPings(t *testing.T) {
	f, url := startManualAPIWith(t, withPongTimeout(testPongWait))
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	pings := pinged(ws, false)
	readAll(ws)
	waitForPing(t, pings)

	req, ok := f.next("unsubscribeReceive", 5*testPongWait)
	if !ok {
		t.Fatal("a WebSocket that answered no ping is still attached")
	}
	f.respond(t, req, nil)
}

// Messages from the client do not stand in for pongs.
func TestManualDetachesAWebSocketThatSendsMessagesButNoPongs(t *testing.T) {
	f, url := startManualAPIWith(t, withPongTimeout(testPongWait))
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	pings := pinged(ws, false)
	ended := readAll(ws)
	waitForPing(t, pings)
	go func() {
		for {
			select {
			case <-ended:
				return
			case <-time.After(testPingPeriod):
				if ws.WriteMessage(websocket.TextMessage, []byte("{}")) != nil {
					return
				}
			}
		}
	}()

	req, ok := f.next("unsubscribeReceive", 5*testPongWait)
	if !ok {
		t.Fatal("a WebSocket that sent messages but no pongs is still attached")
	}
	f.respond(t, req, nil)
}

func TestManualKeepsAWebSocketThatAnswersPings(t *testing.T) {
	f, url := startManualAPIWith(t, withPongTimeout(testPongWait))
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	pings := pinged(ws, true)
	readAll(ws)
	waitForPing(t, pings)

	if _, ok := f.next("unsubscribeReceive", 3*testPongWait); ok {
		t.Fatal("a WebSocket that answered every ping was detached")
	}
}

func TestManualKeepsAnUnansweringWebSocketWhenThePongTimeoutIsOff(t *testing.T) {
	f, url := startManualAPIWith(t, withPongTimeout(0))
	ws := dialAttached(t, f, url, true)
	defer ws.Close()
	pings := pinged(ws, false)
	readAll(ws)
	waitForPing(t, pings)

	if _, ok := f.next("unsubscribeReceive", 3*testPongWait); ok {
		t.Fatal("a WebSocket was detached although the pong timeout is off")
	}
}

// The timeout applies in on-start mode too, where there is no subscription
// to end; the server closes the WebSocket.
func TestOnStartClosesAWebSocketThatStopsAnsweringPings(t *testing.T) {
	_, url := startAPI(t, client.OnStart, withPongTimeout(testPongWait))
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	pings := pinged(ws, false)
	ended := readAll(ws)
	waitForPing(t, pings)
	select {
	case <-ended:
	case <-time.After(5 * testPongWait):
		t.Fatal("a WebSocket that answered no ping is still open")
	}
}
