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
}

// apiFakeCli imitates signal-cli's JSON-RPC TCP socket for one connection.
type apiFakeCli struct {
	ln       net.Listener
	mu       sync.Mutex
	conn     net.Conn
	requests chan apiCliRequest
}

func newAPIFakeCli(t *testing.T) *apiFakeCli {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &apiFakeCli{ln: ln, requests: make(chan apiCliRequest, 1024)}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conn = c
		f.mu.Unlock()
		r := bufio.NewReader(c)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				return
			}
			var req apiCliRequest
			if json.Unmarshal(line, &req) == nil {
				f.requests <- req
			}
		}
	}()
	return f
}

func (f *apiFakeCli) write(t *testing.T, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.conn.Write(append(b, '\n')); err != nil {
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

// respond waits before answering: upstream getRaw registers its response
// channel after writing the request, so an immediate reply is dropped.
func (f *apiFakeCli) respond(t *testing.T, req apiCliRequest, result any) {
	t.Helper()
	time.Sleep(30 * time.Millisecond)
	f.write(t, map[string]any{"jsonrpc": "2.0", "id": req.Id, "result": result})
}

func (f *apiFakeCli) notify(t *testing.T, timestamp int64) {
	t.Helper()
	f.write(t, map[string]any{"jsonrpc": "2.0", "method": "receive", "params": map[string]any{"subscription": 0, "result": map[string]any{
		"account": apiLossAccount, "envelope": map[string]any{"timestamp": timestamp, "dataMessage": map[string]any{"message": strings.Repeat("x", 512)}},
	}}})
}

func startManualAPI(t *testing.T) (*apiFakeCli, string) {
	t.Helper()
	f := newAPIFakeCli(t)
	dir := t.TempDir()
	cfg := filepath.Join(dir, "jsonrpc2.yml")
	port := f.ln.Addr().(*net.TCPAddr).Port
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf("config:\n  \"<multi-account>\":\n    tcp_port: %d\n", port)), 0o644); err != nil {
		t.Fatal(err)
	}
	sc := client.NewSignalClient(dir, dir, dir, client.JsonRpc, cfg, filepath.Join(dir, "api.yml"), "", client.Manual)
	if err := sc.Init(1); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.GET("/v1/receive/:number", NewApi(sc).Receive)
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
	time.Sleep(50 * time.Millisecond) // GetReceiveChannel registers the channel
	return ws
}
