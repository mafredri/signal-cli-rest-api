package client

import (
	"encoding/json"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

// fakeSignalCli is a net.Conn that plays signal-cli's side of the JSON-RPC
// connection in memory. Every request written to it is recorded and answered
// from inside Write, and Write returns only after ReceiveData has handled the
// answer: the order in which a fast reply arrives on a real connection.
type fakeSignalCli struct {
	net.Conn
	toReader *io.PipeWriter
	fromConn *io.PipeReader
	// writeErr, if set, fails every Write.
	writeErr error
	// answer returns the result or error for a request; nil answers {}.
	answer func(req fakeRequest) (result any, err *Error)

	writeMu  sync.Mutex
	mu       sync.Mutex
	requests []fakeRequest
}

type fakeRequest struct {
	Id     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func newFakeSignalCli() *fakeSignalCli {
	r, w := io.Pipe()
	return &fakeSignalCli{toReader: w, fromConn: r}
}

// newFakeJsonRpc2Client returns a client connected to f, with its read loop
// running. The read loop blocks on the pipe for the rest of the test binary;
// closing the pipe would make it try to reconnect.
func newFakeJsonRpc2Client(f *fakeSignalCli, mode JsonRpcReceiveMode) *JsonRpc2Client {
	r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, mode)
	r.conn = f
	go r.ReceiveData(utils.MULTI_ACCOUNT_NUMBER, "")
	return r
}

// newFakeJsonRpcSignalClient returns a SignalClient in json-rpc mode whose
// requests go to f.
func newFakeJsonRpcSignalClient(t *testing.T, f *fakeSignalCli) *SignalClient {
	dir := t.TempDir() + "/"
	s := NewSignalClient(dir, dir, dir, JsonRpc, "", dir+"api.yml", "", OnStart)
	s.signalCliApiConfig = utils.NewSignalCliApiConfig()
	s.jsonRpc2Clients[utils.MULTI_ACCOUNT_NUMBER] = newFakeJsonRpc2Client(f, OnStart)
	return s
}

func (f *fakeSignalCli) Read(b []byte) (int, error) { return f.fromConn.Read(b) }

func (f *fakeSignalCli) Write(b []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	var req fakeRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return 0, err
	}
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()

	var result any = map[string]any{}
	var rpcErr *Error
	if f.answer != nil {
		result, rpcErr = f.answer(req)
	}
	reply := map[string]any{"jsonrpc": "2.0", "id": req.Id}
	if rpcErr != nil {
		reply["error"] = rpcErr
	} else {
		reply["result"] = result
	}
	if err := f.send(reply); err != nil {
		return 0, err
	}
	return len(b), nil
}

// send writes one message to the client and returns after ReceiveData has
// handled it: ReceiveData reads the empty object that follows only once it
// has finished with the message.
func (f *fakeSignalCli) send(msg any) error {
	line, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if _, err := f.toReader.Write(append(line, '\n')); err != nil {
		return err
	}
	_, err = f.toReader.Write([]byte("{}\n"))
	return err
}

// Requests returns the requests written so far.
func (f *fakeSignalCli) Requests() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRequest(nil), f.requests...)
}
