package client

import (
	"errors"
	"testing"
	"time"
)

func TestGetRawReceivesAReplyThatArrivesBeforeWriteReturns(t *testing.T) {
	r := newFakeJsonRpc2Client(newFakeSignalCli(), OnStart)

	errc := make(chan error, 1)
	go func() {
		_, err := r.getRaw("version", nil, nil)
		errc <- err
	}()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("getRaw did not return the reply signal-cli sent before Write returned")
	}
	assertNoPendingResponses(t, r)
}

func TestGetRawForgetsTheRequestWhenWriteFails(t *testing.T) {
	f := newFakeSignalCli()
	f.writeErr = errors.New("broken pipe")
	r := newFakeJsonRpc2Client(f, OnStart)

	if _, err := r.getRaw("version", nil, nil); !errors.Is(err, f.writeErr) {
		t.Fatalf("getRaw error = %v, want %v", err, f.writeErr)
	}
	assertNoPendingResponses(t, r)
}

func assertNoPendingResponses(t *testing.T, r *JsonRpc2Client) {
	t.Helper()
	r.receivedResponsesMutex.Lock()
	defer r.receivedResponsesMutex.Unlock()
	if n := len(r.receivedResponsesById); n != 0 {
		t.Fatalf("%d responses still registered", n)
	}
}
