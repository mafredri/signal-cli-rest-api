package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
)

const (
	manualAccount      = "+15550000001"
	otherManualAccount = "+15550000002"
)

func newManualClient(f *tcpSignalCli) *JsonRpc2Client {
	r := NewJsonRpc2Client(utils.NewSignalCliApiConfig(), utils.MULTI_ACCOUNT_NUMBER, Manual)
	r.address = f.addr()
	return r
}

// attach gets a receive channel and answers its subscription.
func attach(t *testing.T, f *tcpSignalCli, r *JsonRpc2Client, account string) (chan JsonRpc2ReceivedMessage, string, tcpRequest) {
	t.Helper()
	c, id, err := r.GetReceiveChannel(account)
	if err != nil {
		t.Fatal(err)
	}
	sub := f.next("subscribeReceive")
	sub.reply(t, 1)
	return c, id, sub
}

// detach removes a receive channel, answering the unsubscription when it is
// the account's last.
func detach(t *testing.T, f *tcpSignalCli, r *JsonRpc2Client, id string) tcpRequest {
	t.Helper()
	done := make(chan struct{})
	go func() {
		r.RemoveReceiveChannel(id)
		close(done)
	}()
	unsub := f.next("unsubscribeReceive")
	unsub.reply(t, nil)
	<-done
	return unsub
}

func TestManualReceiveSubscribesOnItsOwnConnection(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	_, id, sub := attach(t, f, r, manualAccount)
	var params struct {
		Account string `json:"account"`
	}
	json.Unmarshal(sub.Params, &params)
	if params.Account != manualAccount {
		t.Fatalf("subscribeReceive account %q, want %q", params.Account, manualAccount)
	}
	_, otherId, otherSub := attach(t, f, r, otherManualAccount)
	if otherSub.conn == sub.conn {
		t.Fatal("two accounts share a receive connection")
	}

	unsub := detach(t, f, r, id)
	if unsub.conn != sub.conn {
		t.Fatal("unsubscribeReceive was not sent on the subscription's connection")
	}
	var unsubParams struct {
		Account      string `json:"account"`
		Subscription int64  `json:"subscription"`
	}
	json.Unmarshal(unsub.Params, &unsubParams)
	if unsubParams.Account != manualAccount || unsubParams.Subscription != 1 {
		t.Fatalf("unsubscribeReceive params %s", unsub.Params)
	}
	detach(t, f, r, otherId)
}

// signal-cli starts receiving before it writes the subscribeReceive reply,
// so messages can precede it.
func TestManualReceiveDeliversMessagesBeforeTheSubscriptionReply(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	c, id, err := r.GetReceiveChannel(manualAccount)
	if err != nil {
		t.Fatal(err)
	}
	sub := f.next("subscribeReceive")
	for i := int64(1); i <= 200; i++ {
		sub.notify(t, manualAccount, i)
	}
	sub.reply(t, 1)
	for i := int64(1); i <= 200; i++ {
		if got := timestampOf(t, receive(t, c)); got != i {
			t.Fatalf("message %d has timestamp %d", i, got)
		}
	}
	detach(t, f, r, id)
}

// The receiver reads only as fast as the consumer takes messages, so a
// consumer that is behind makes signal-cli wait instead of losing messages
// or buffering them without limit.
func TestManualReceiveHoldsBackSignalCliWhileTheConsumerIsBehind(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	c, id, sub := attach(t, f, r, manualAccount)

	const total = 512
	text := strings.Repeat("x", 64*1024)
	var written atomic.Int64
	writeErr := make(chan error, 1)
	go func() {
		for i := int64(1); i <= total; i++ {
			if err := writeLine(sub.conn, notification(manualAccount, i, text)); err != nil {
				writeErr <- err
				return
			}
			written.Add(1)
		}
		writeErr <- nil
	}()

	// Wait until signal-cli's writes stop making progress.
	var stalledAt int64
	for previous := int64(-1); ; {
		time.Sleep(200 * time.Millisecond)
		n := written.Load()
		if n == total {
			t.Fatalf("the receiver read all %d messages (%d MiB) while the consumer took none", total, total/16)
		}
		if n == previous {
			stalledAt = n
			break
		}
		previous = n
	}

	for i := int64(1); i <= total; i++ {
		if got := timestampOf(t, receive(t, c)); got != i {
			t.Fatalf("message %d has timestamp %d", i, got)
		}
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
	t.Logf("signal-cli waited after %d of %d messages", stalledAt, total)
	detach(t, f, r, id)
}

// Messages signal-cli writes until it confirms the unsubscription reach the
// next channel for the account.
func TestManualReceiveKeepsMessagesThatArriveAfterTheLastChannelDetached(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	c, id, sub := attach(t, f, r, manualAccount)
	sub.notify(t, manualAccount, 1)
	if got := timestampOf(t, receive(t, c)); got != 1 {
		t.Fatalf("timestamp %d, want 1", got)
	}

	done := make(chan struct{})
	go func() {
		r.RemoveReceiveChannel(id)
		close(done)
	}()
	unsub := f.next("unsubscribeReceive")
	unsub.notify(t, manualAccount, 2)
	unsub.notify(t, manualAccount, 3)
	unsub.reply(t, nil)
	<-done

	c, id, sub = attach(t, f, r, manualAccount)
	sub.notify(t, manualAccount, 4)
	for _, want := range []int64{2, 3, 4} {
		if got := timestampOf(t, receive(t, c)); got != want {
			t.Fatalf("timestamp %d, want %d", got, want)
		}
	}
	detach(t, f, r, id)
}

func TestManualReceiveSharesOneSubscriptionPerAccount(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	first, firstId, sub := attach(t, f, r, manualAccount)
	second, secondId, err := r.GetReceiveChannel(manualAccount)
	if err != nil {
		t.Fatal(err)
	}
	f.none("subscribeReceive", 200*time.Millisecond)

	// Each channel is read by its own consumer, and a message reaches them in
	// turn.
	got := make(chan JsonRpc2ReceivedMessage, 2)
	for _, c := range []chan JsonRpc2ReceivedMessage{first, second} {
		go func() {
			select {
			case msg := <-c:
				got <- msg
			case <-time.After(5 * time.Second):
				got <- JsonRpc2ReceivedMessage{}
			}
		}()
	}
	sub.notify(t, manualAccount, 1)
	for range 2 {
		if ts := timestampOf(t, <-got); ts != 1 {
			t.Fatalf("timestamp %d, want 1", ts)
		}
	}

	r.RemoveReceiveChannel(firstId)
	f.none("unsubscribeReceive", 200*time.Millisecond)
	sub.notify(t, manualAccount, 2)
	if got := timestampOf(t, receive(t, second)); got != 2 {
		t.Fatalf("timestamp %d, want 2", got)
	}
	detach(t, f, r, secondId)
}

func TestManualReceiveClosesTheChannelWhenSubscribingFails(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	c, id, err := r.GetReceiveChannel(manualAccount)
	if err != nil {
		t.Fatal(err)
	}
	f.next("subscribeReceive").replyError(t, -32602, "Specified account does not exist")
	assertClosed(t, c)
	r.RemoveReceiveChannel(id)
	f.none("unsubscribeReceive", 200*time.Millisecond)
}

// After signal-cli restarts, the channel is closed so its consumer
// reconnects, and the next channel subscribes on a new connection.
func TestManualReceiveSubscribesAgainAfterTheConnectionEnds(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	c, id, sub := attach(t, f, r, manualAccount)
	sub.conn.Close()
	assertClosed(t, c)
	r.RemoveReceiveChannel(id)

	c, id, sub2 := attach(t, f, r, manualAccount)
	if sub2.conn == sub.conn {
		t.Fatal("subscribed again on the closed connection")
	}
	sub2.notify(t, manualAccount, 1)
	if got := timestampOf(t, receive(t, c)); got != 1 {
		t.Fatalf("timestamp %d, want 1", got)
	}
	detach(t, f, r, id)
}

func assertClosed(t *testing.T, c chan JsonRpc2ReceivedMessage) {
	t.Helper()
	select {
	case _, ok := <-c:
		if ok {
			t.Fatal("received a message instead of the channel closing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the receive channel stayed open")
	}
}

// A channel that replaces another while a delivery waits for the old one
// gets the message, in order.
func TestManualReceiveDeliversToAChannelThatReplacesOneThatWasNotReading(t *testing.T) {
	f := newTCPSignalCli(t)
	r := newManualClient(f)
	_, oldId, sub := attach(t, f, r, manualAccount)
	sub.notify(t, manualAccount, 1)
	// Let the receiver start waiting for the old channel to take message 1.
	time.Sleep(100 * time.Millisecond)
	replacement, id, err := r.GetReceiveChannel(manualAccount)
	if err != nil {
		t.Fatal(err)
	}
	r.RemoveReceiveChannel(oldId)
	sub.notify(t, manualAccount, 2)
	for _, want := range []int64{1, 2} {
		if got := timestampOf(t, receive(t, replacement)); got != want {
			t.Fatalf("timestamp %d, want %d", got, want)
		}
	}
	detach(t, f, r, id)
}

// A webhook that never answers delays the receiver, but cannot keep its
// last channel from detaching or the next one from attaching, and the
// message being posted reaches that next channel.
func TestManualReceiveStopsDespiteAWebhookThatDoesNotAnswer(t *testing.T) {
	previous := unsubscribeTimeout
	unsubscribeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { unsubscribeTimeout = previous })
	release := make(chan struct{})
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		select {
		case <-release:
		case <-req.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		webhook.Close()
	})

	f := newTCPSignalCli(t)
	r := newManualClient(f)
	r.receiveWebhookUrl = webhook.URL
	_, id, sub := attach(t, f, r, manualAccount)
	sub.notify(t, manualAccount, 1)

	done := make(chan struct{})
	go func() {
		r.RemoveReceiveChannel(id)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("detaching waited for the webhook")
	}

	c, id, _ := attach(t, f, r, manualAccount)
	if got := timestampOf(t, receive(t, c)); got != 1 {
		t.Fatalf("timestamp %d, want 1", got)
	}
	detach(t, f, r, id)
}
