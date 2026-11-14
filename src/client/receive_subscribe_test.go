package client

import (
	"slices"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

const lossAccount = "+15550000001"

func startClient(t *testing.T, f *fakeCli, mode JsonRpcReceiveMode) (*JsonRpc2Client, *fakeCliConn) {
	t.Helper()
	r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, mode)
	if err := r.Dial(f.addr(), 1); err != nil {
		t.Fatal(err)
	}
	go r.ReceiveData(utils.MULTI_ACCOUNT_NUMBER, "")
	return r, f.nextConn(t)
}

type channelResult struct {
	c   chan JsonRpc2ReceivedMessage
	id  string
	err error
}

func attach(t *testing.T, f *fakeCli, r *JsonRpc2Client, beforeResponse, afterResponse func(conn *fakeCliConn)) channelResult {
	t.Helper()
	done := make(chan channelResult, 1)
	go func() {
		c, id, err := r.GetReceiveChannel(lossAccount)
		done <- channelResult{c, id, err}
	}()
	req := f.next(t, "subscribeReceive")
	if beforeResponse != nil {
		beforeResponse(req.conn)
	}
	f.respond(t, r, req, 0)
	if afterResponse != nil {
		afterResponse(req.conn)
	}
	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	return res
}

// signal-cli adds the receive handler, which starts its receive thread, before
// it writes the subscribeReceive response (SignalJsonRpcDispatcherHandler,
// SubscribeReceiveCommand). The backlog can therefore precede the response.
func TestManualSubscribeKeepsNotificationsSentBeforeTheResponse(t *testing.T) {
	f := newFakeCli(t)
	r, _ := startClient(t, f, Manual)
	res := attach(t, f, r, func(conn *fakeCliConn) { notify(t, conn, 0, lossAccount, 1) }, nil)
	if got := receiveWithin(t, res.c, 300*time.Millisecond); !slices.Equal(got, []int64{1}) {
		t.Fatalf("received %v, want [1]: the notification signal-cli sent while subscribing reached no channel", got)
	}
}

// A notification right after the response races GetReceiveChannel, which
// registers its channel only after acquireReceiveSubscription returns.
func TestManualSubscribeKeepsNotificationsSentRightAfterTheResponse(t *testing.T) {
	f := newFakeCli(t)
	r, _ := startClient(t, f, Manual)
	lost := 0
	const rounds = 200
	for i := range rounds {
		ts := int64(i + 1)
		res := attach(t, f, r, nil, func(conn *fakeCliConn) { notify(t, conn, 0, lossAccount, ts) })
		if got := receiveWithin(t, res.c, 20*time.Millisecond); !slices.Equal(got, []int64{ts}) {
			lost++
		}
		go r.RemoveReceiveChannel(res.id)
		f.respond(t, r, f.next(t, "unsubscribeReceive"), nil)
	}
	if lost > 0 {
		t.Fatalf("%d of %d notifications sent right after the subscribeReceive response were lost", lost, rounds)
	}
}
