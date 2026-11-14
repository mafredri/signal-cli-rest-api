package client

import (
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

const immediateReplyAccount = "+15550000002"

// signal-cli can answer before getRaw has finished writing the request. The
// reply must still reach the caller; before the channel was registered ahead
// of the write, it was discarded and GetReceiveChannel never returned, while
// signal-cli was already subscribed.
func TestManualSubscribeSurvivesAnImmediateReply(t *testing.T) {
	hung := 0
	const rounds = 300
	for range rounds {
		f := newFakeCli(t)
		r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, Manual)
		if err := r.Dial(f.addr(), 1); err != nil {
			t.Fatal(err)
		}
		go r.ReceiveData(utils.MULTI_ACCOUNT_NUMBER, "")
		f.nextConn(t)
		done := make(chan struct{})
		go func() { r.GetReceiveChannel(immediateReplyAccount); close(done) }()
		req := f.next(t, "subscribeReceive")
		req.conn.writeLine(t, map[string]any{"jsonrpc": "2.0", "id": req.Id, "result": 0})
		select {
		case <-done:
		case <-time.After(time.Second):
			hung++
		}
	}
	if hung > 0 {
		t.Fatalf("GetReceiveChannel never returned in %d of %d rounds", hung, rounds)
	}
}
