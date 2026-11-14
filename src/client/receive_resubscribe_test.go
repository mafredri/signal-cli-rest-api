package client

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

// signal-cli removes a connection's receive handlers when the connection ends
// (SignalJsonRpcDispatcherHandler.handleConnection), so after ReceiveData
// redials, an attached WebSocket receives nothing unless the wrapper
// subscribes again.
func TestManualResubscribesAfterSignalCliReconnect(t *testing.T) {
	const account = "+15550000001"
	f := newFakeCli(t)
	r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, Manual)
	if err := r.Dial(f.addr(), 1); err != nil {
		t.Fatal(err)
	}
	go r.ReceiveData(utils.MULTI_ACCOUNT_NUMBER, "")
	conn := f.nextConn(t)

	type attached struct {
		c   chan JsonRpc2ReceivedMessage
		id  string
		err error
	}
	done := make(chan attached, 1)
	go func() {
		c, id, err := r.GetReceiveChannel(account)
		done <- attached{c, id, err}
	}()
	f.respond(t, r, f.next(t, "subscribeReceive"), 0)
	ws := <-done
	if ws.err != nil {
		t.Fatal(ws.err)
	}

	conn.Close()
	conn = f.nextConn(t)
	req, ok := f.nextWithin("subscribeReceive", 2*time.Second)
	if !ok {
		t.Fatal("the wrapper did not subscribe again after reconnecting to signal-cli")
	}
	if req.conn != conn {
		t.Fatal("subscribeReceive was not sent on the new connection")
	}
	f.respond(t, r, req, 5)

	notify(t, conn, 5, account, 42)
	if got := receiveWithin(t, ws.c, 300*time.Millisecond); !slices.Equal(got, []int64{42}) {
		t.Fatalf("received %v after reconnecting, want [42]", got)
	}

	go r.RemoveReceiveChannel(ws.id)
	unsub := f.next(t, "unsubscribeReceive")
	var params struct {
		Subscription int64 `json:"subscription"`
	}
	if err := json.Unmarshal(unsub.Params, &params); err != nil {
		t.Fatal(err)
	}
	if params.Subscription != 5 {
		t.Fatalf("unsubscribed subscription %d, want the renewed 5", params.Subscription)
	}
	f.respond(t, r, unsub, nil)
}
