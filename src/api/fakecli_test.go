package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// gin's mode is process-wide, so set it once rather than in parallel tests.
func init() { gin.SetMode(gin.TestMode) }

const apiLossAccount = "+15550000001"

type apiCliRequest struct {
	Method string `json:"method"`
	Id     string `json:"id"`
	conn   net.Conn
}

// apiFakeCli imitates signal-cli's JSON-RPC TCP socket. It accepts every
// connection: the wrapper's shared one and, in manual receive mode, one per
// subscribed account. Notifications go to the connection of the latest
// subscribeReceive.
type apiFakeCli struct {
	ln       net.Listener
	mu       sync.Mutex
	receive  net.Conn
	requests chan apiCliRequest
}

func newAPIFakeCli(t *testing.T) *apiFakeCli {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// The wrapper's connection stays open: closing it would make ReceiveData
	// reconnect and exit the test binary when that fails.
	t.Cleanup(func() { ln.Close() })
	f := &apiFakeCli{ln: ln, requests: make(chan apiCliRequest, 1024)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					var req apiCliRequest
					if json.Unmarshal(line, &req) == nil {
						req.conn = c
						if req.Method == "subscribeReceive" {
							f.mu.Lock()
							f.receive = c
							f.mu.Unlock()
						}
						f.requests <- req
					}
				}
			}()
		}
	}()
	return f
}

func (f *apiFakeCli) writeTo(t *testing.T, conn net.Conn, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := conn.Write(append(b, '\n')); err != nil {
		t.Fatal(err)
	}
}

func (f *apiFakeCli) next(method string, d time.Duration) (apiCliRequest, bool) {
	deadline := time.After(d)
	for {
		select {
		case req := <-f.requests:
			if req.Method == method {
				return req, true
			}
		case <-deadline:
			return apiCliRequest{}, false
		}
	}
}

func (f *apiFakeCli) respond(t *testing.T, req apiCliRequest, result any) {
	t.Helper()
	f.writeTo(t, req.conn, map[string]any{"jsonrpc": "2.0", "id": req.Id, "result": result})
}

func (f *apiFakeCli) respondError(t *testing.T, req apiCliRequest, code int, message string) {
	t.Helper()
	f.writeTo(t, req.conn, map[string]any{"jsonrpc": "2.0", "id": req.Id, "error": map[string]any{"code": code, "message": message}})
}

func (f *apiFakeCli) notify(t *testing.T, timestamp int64) {
	t.Helper()
	f.mu.Lock()
	conn := f.receive
	f.mu.Unlock()
	f.writeTo(t, conn, map[string]any{"jsonrpc": "2.0", "method": "receive", "params": map[string]any{"subscription": 0, "result": map[string]any{
		"account": apiLossAccount, "envelope": map[string]any{"timestamp": timestamp, "dataMessage": map[string]any{"message": strings.Repeat("x", 512)}},
	}}})
}

func startManualAPI(t *testing.T) (*apiFakeCli, string) {
	t.Helper()
	return startManualAPIWith(t, nil)
}

// startManualAPIWith calls configure, if not nil, on the Api before serving.
func startManualAPIWith(t *testing.T, configure func(*Api)) (*apiFakeCli, string) {
	t.Helper()
	return startAPI(t, client.Manual, configure)
}

func startAPI(t *testing.T, mode client.JsonRpcReceiveMode, configure func(*Api)) (*apiFakeCli, string) {
	t.Helper()
	f := newAPIFakeCli(t)
	dir := t.TempDir()
	cfg := filepath.Join(dir, "jsonrpc2.yml")
	port := f.ln.Addr().(*net.TCPAddr).Port
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("config:\n  \"<multi-account>\":\n    tcp_port: %d\n", port)), 0o644); err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalClient(dir, dir, dir, client.JsonRpc, cfg, filepath.Join(dir, "api.yml"), "", mode)
	if err := sc.Init(1); err != nil {
		t.Fatal(err)
	}
	api := NewApi(sc)
	if configure != nil {
		configure(api)
	}
	router := gin.New()
	router.GET("/v1/receive/:number", api.Receive)
	// Runs only handleSignalReceive, without the goroutines that read and
	// ping, so that a failed write is the only way for it to return.
	router.GET("/test/handle-receive/:number", func(c *gin.Context) {
		ws, err := connectionUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		api.handleSignalReceive(ws, c.Param("number"), make(chan struct{}))
	})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)
	return f, "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/receive/" + apiLossAccount
}

func dialAttached(t *testing.T, f *apiFakeCli, url string, expectSubscribe bool) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if expectSubscribe {
		req, ok := f.next("subscribeReceive", 2*time.Second)
		if !ok {
			t.Fatal("no subscribeReceive for a new WebSocket")
		}
		f.respond(t, req, 0)
	}
	// GetReceiveChannel registers the channel after subscribeReceive returns;
	// notify until one reaches the WebSocket.
	got := make(chan error, 1)
	go func() { _, _, err := ws.ReadMessage(); got <- err }()
	deadline := time.After(5 * time.Second)
	for timestamp := int64(-1); ; timestamp-- {
		f.notify(t, timestamp)
		select {
		case err := <-got:
			if err != nil {
				t.Fatal(err)
			}
			return ws
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("no notification reached the WebSocket")
		}
	}
}
