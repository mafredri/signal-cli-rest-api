package client

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

// tcpSignalCli imitates signal-cli's JSON-RPC TCP socket for tests that need
// real connections: the manual receiver dials its own.
type tcpSignalCli struct {
	t        *testing.T
	ln       net.Listener
	requests chan tcpRequest
	mu       sync.Mutex
	conns    []net.Conn
}

type tcpRequest struct {
	fakeRequest
	conn net.Conn
}

func newTCPSignalCli(t *testing.T) *tcpSignalCli {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &tcpSignalCli{t: t, ln: ln, requests: make(chan tcpRequest, 1024)}
	t.Cleanup(func() {
		ln.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, c := range f.conns {
			c.Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.conns = append(f.conns, conn)
			f.mu.Unlock()
			go func() {
				r := bufio.NewReader(conn)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					var req fakeRequest
					if json.Unmarshal(line, &req) == nil {
						f.requests <- tcpRequest{req, conn}
					}
				}
			}()
		}
	}()
	return f
}

func (f *tcpSignalCli) addr() string { return f.ln.Addr().String() }

// next returns the next request for method, failing the test after a timeout.
func (f *tcpSignalCli) next(method string) tcpRequest {
	f.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case req := <-f.requests:
			if req.Method == method {
				return req
			}
		case <-deadline:
			f.t.Fatalf("no %s request", method)
		}
	}
}

// none fails the test if a request for method arrives within d.
func (f *tcpSignalCli) none(method string, d time.Duration) {
	f.t.Helper()
	deadline := time.After(d)
	for {
		select {
		case req := <-f.requests:
			if req.Method == method {
				f.t.Fatalf("unexpected %s request", method)
			}
		case <-deadline:
			return
		}
	}
}

func writeLine(conn net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = conn.Write(append(b, '\n'))
	return err
}

func (r tcpRequest) reply(t *testing.T, result any) {
	t.Helper()
	if err := writeLine(r.conn, map[string]any{"jsonrpc": "2.0", "id": r.Id, "result": result}); err != nil {
		t.Fatal(err)
	}
}

func (r tcpRequest) replyError(t *testing.T, code int, message string) {
	t.Helper()
	if err := writeLine(r.conn, map[string]any{"jsonrpc": "2.0", "id": r.Id, "error": map[string]any{"code": code, "message": message}}); err != nil {
		t.Fatal(err)
	}
}

// notification is a manual-mode receive notification for account, as
// signal-cli writes it.
func notification(account string, timestamp int64, text string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "method": "receive", "params": map[string]any{
		"subscription": 0,
		"result": map[string]any{
			"account":  account,
			"envelope": map[string]any{"timestamp": timestamp, "dataMessage": map[string]any{"message": text}},
		},
	}}
}

func (r tcpRequest) notify(t *testing.T, account string, timestamp int64) {
	t.Helper()
	if err := writeLine(r.conn, notification(account, timestamp, "")); err != nil {
		t.Fatal(err)
	}
}

// timestampOf returns the envelope timestamp of a delivered message.
func timestampOf(t *testing.T, msg JsonRpc2ReceivedMessage) int64 {
	t.Helper()
	var p struct {
		Envelope struct {
			Timestamp int64 `json:"timestamp"`
		} `json:"envelope"`
	}
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		t.Fatal(err)
	}
	return p.Envelope.Timestamp
}

// receive returns the next message on c, failing the test after a timeout
// or if c is closed.
func receive(t *testing.T, c chan JsonRpc2ReceivedMessage) JsonRpc2ReceivedMessage {
	t.Helper()
	select {
	case msg, ok := <-c:
		if !ok {
			t.Fatal("the receive channel was closed")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
	}
	return JsonRpc2ReceivedMessage{}
}
