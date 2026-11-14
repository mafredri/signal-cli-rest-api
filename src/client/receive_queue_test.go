package client

import (
	"slices"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

func startOnStartClient(t *testing.T) (*JsonRpc2Client, *fakeCliConn) {
	t.Helper()
	f := newFakeCli(t)
	r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, OnStart)
	if err := r.Dial(f.addr(), 1); err != nil {
		t.Fatal(err)
	}
	go r.ReceiveData(utils.MULTI_ACCOUNT_NUMBER, "")
	return r, f.nextConn(t)
}

func sequence(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = int64(i + 1)
	}
	return s
}

// signal-cli acknowledges a message to the Signal server before it sends the
// notification, so a notification the wrapper drops is lost.
func TestReceiveChannelKeepsNotificationsWhileTheConsumerIsBusy(t *testing.T) {
	const account = "+15550000001"
	r, conn := startOnStartClient(t)
	c, _, err := r.GetReceiveChannel(account)
	if err != nil {
		t.Fatal(err)
	}
	const sent = 500
	for i := range sent {
		notify(t, conn, -1, account, int64(i+1))
	}
	time.Sleep(200 * time.Millisecond) // the consumer is busy
	if got := receiveWithin(t, c, 500*time.Millisecond); !slices.Equal(got, sequence(sent)) {
		t.Fatalf("received %d of %d notifications, or out of order", len(got), sent)
	}
}

// A consumer that never reads must not hold up delivery to the others, or the
// JSON-RPC responses that arrive on the same connection.
func TestReceiveChannelThatIsNeverReadDoesNotBlockOthers(t *testing.T) {
	const account = "+15550000001"
	r, conn := startOnStartClient(t)
	_, stuckID, err := r.GetReceiveChannel(account)
	if err != nil {
		t.Fatal(err)
	}
	live, _, err := r.GetReceiveChannel(account)
	if err != nil {
		t.Fatal(err)
	}
	const sent = 200
	var got []int64
	for i := range sent {
		notify(t, conn, -1, account, int64(i+1))
		got = append(got, receiveWithin(t, live, time.Millisecond)...)
	}
	got = append(got, receiveWithin(t, live, 300*time.Millisecond)...)
	if !slices.Equal(got, sequence(sent)) {
		t.Fatalf("live channel received %d of %d notifications", len(got), sent)
	}
	r.RemoveReceiveChannel(stuckID)
	notify(t, conn, -1, account, sent+1)
	if got := receiveWithin(t, live, 300*time.Millisecond); !slices.Equal(got, []int64{sent + 1}) {
		t.Fatalf("after removing the unread channel received %v", got)
	}
}
