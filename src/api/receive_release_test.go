package api

import (
	"net"
	"testing"
	"time"
)

// handleSignalReceive returns on a WebSocket write error without calling
// RemoveReceiveChannel. The subscription's refcount then never reaches zero,
// so signal-cli keeps receiving (and acknowledging) with no consumer attached.
func TestManualReleasesSubscriptionWhenTheWebSocketWriteFails(t *testing.T) {
	f, url := startManualAPI(t)
	for attempt := 1; attempt <= 30; attempt++ {
		ws := dialAttached(t, f, url, true)
		// Abort the connection while notifications are being written to it.
		ws.UnderlyingConn().(*net.TCPConn).SetLinger(0)
		ws.UnderlyingConn().Close()
		for i := range 64 {
			f.notify(t, int64(attempt*1000+i))
		}
		req, ok := f.next("unsubscribeReceive", 2*time.Second)
		if !ok {
			t.Fatalf("attempt %d: the WebSocket is gone but the wrapper never unsubscribed; signal-cli keeps receiving and the wrapper drops every message", attempt)
		}
		f.respond(t, req, nil)
	}
}
