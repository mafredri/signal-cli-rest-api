package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeCliRequest is one JSON-RPC request the wrapper wrote to the fake
// signal-cli, with the connection it arrived on.
type fakeCliRequest struct {
	Method string          `json:"method"`
	Id     string          `json:"id"`
	Params json.RawMessage `json:"params"`
	conn   *fakeCliConn
}

type fakeCliConn struct {
	net.Conn
	mu sync.Mutex
}

func (c *fakeCliConn) writeLine(t *testing.T, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.Write(append(b, '\n')); err != nil {
		t.Logf("fake signal-cli write: %v", err)
	}
}

// fakeCli imitates signal-cli's JSON-RPC TCP socket: it records requests and
// writes only what the test tells it to.
type fakeCli struct {
	ln       net.Listener
	requests chan fakeCliRequest
	conns    chan *fakeCliConn
}

func newFakeCli(t *testing.T) *fakeCli {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCli{ln: ln, requests: make(chan fakeCliRequest, 1024), conns: make(chan *fakeCliConn, 16)}
	// The listener stays open: ReceiveData redials after a lost connection and
	// exits the process when it cannot reconnect.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			fc := &fakeCliConn{Conn: c}
			f.conns <- fc
			go func() {
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					var req fakeCliRequest
					if err := json.Unmarshal(line, &req); err != nil {
						continue
					}
					req.conn = fc
					f.requests <- req
				}
			}()
		}
	}()
	return f
}

func (f *fakeCli) addr() string { return f.ln.Addr().String() }

func (f *fakeCli) nextConn(t *testing.T) *fakeCliConn {
	t.Helper()
	select {
	case c := <-f.conns:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("wrapper did not connect to fake signal-cli")
		return nil
	}
}

func (f *fakeCli) next(t *testing.T, method string) fakeCliRequest {
	t.Helper()
	req, ok := f.nextWithin(method, 5*time.Second)
	if !ok {
		t.Fatalf("no %s request within 5s", method)
	}
	return req
}

func (f *fakeCli) nextWithin(method string, d time.Duration) (fakeCliRequest, bool) {
	deadline := time.After(d)
	for {
		select {
		case req := <-f.requests:
			if req.Method == method {
				return req, true
			}
		case <-deadline:
			return fakeCliRequest{}, false
		}
	}
}

// respond answers req once the wrapper has registered its response channel.
// Upstream getRaw registers the channel after writing the request, so an
// immediate reply is dropped (a separate bug); waiting keeps these tests about
// receive delivery only.
func (f *fakeCli) respond(t *testing.T, r *JsonRpc2Client, req fakeCliRequest, result any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.receivedResponsesMutex.Lock()
		_, ok := r.receivedResponsesById[req.Id]
		r.receivedResponsesMutex.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wrapper never waited for the response to %s", req.Method)
		}
		time.Sleep(time.Millisecond)
	}
	req.conn.writeLine(t, map[string]any{"jsonrpc": "2.0", "id": req.Id, "result": result})
}

// notify writes a receive notification in manual mode's shape when
// subscription >= 0, otherwise in on-start mode's shape.
func notify(t *testing.T, c *fakeCliConn, subscription int, account string, timestamp int64) {
	t.Helper()
	envelope := map[string]any{"account": account, "envelope": map[string]any{"timestamp": timestamp, "dataMessage": map[string]any{"message": fmt.Sprint(timestamp)}}}
	var params any = envelope
	if subscription >= 0 {
		params = map[string]any{"subscription": subscription, "result": envelope}
	}
	c.writeLine(t, map[string]any{"jsonrpc": "2.0", "method": "receive", "params": params})
}

func timestampOf(t *testing.T, msg JsonRpc2ReceivedMessage) int64 {
	t.Helper()
	var p struct {
		Envelope struct {
			Timestamp int64 `json:"timestamp"`
		} `json:"envelope"`
	}
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		t.Fatalf("notification params %s: %v", msg.Params, err)
	}
	return p.Envelope.Timestamp
}

// receiveWithin returns the timestamps that arrive on c within d.
func receiveWithin(t *testing.T, c chan JsonRpc2ReceivedMessage, d time.Duration) []int64 {
	t.Helper()
	var got []int64
	deadline := time.After(d)
	for {
		select {
		case msg := <-c:
			got = append(got, timestampOf(t, msg))
		case <-deadline:
			return got
		}
	}
}
