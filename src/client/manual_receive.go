package client

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
)

// unsubscribeTimeout bounds how long a stopping receiver waits for
// signal-cli to confirm the subscription ended. A variable for tests.
var unsubscribeTimeout = 10 * time.Second

// manualReceiver delivers one account's messages in manual receive mode.
//
// signal-cli acknowledges a message to Signal before it writes the message
// to the JSON-RPC connection, and keeps no copy once the write returned,
// even when the write failed. So that no message signal-cli wrote is lost,
// an account with attached receive channels gets its own connection, and
// the receiver reads from it only as fast as the channels take the
// messages. While a channel is slow, signal-cli's write blocks, it stops
// receiving for the account, and further messages stay queued at Signal. On
// the shared connection this would also hold up every reply and every
// other account.
type manualReceiver struct {
	address    string
	account    string
	webhookUrl string

	// lifecycle serializes attaching and detaching, which start and stop
	// the connection.
	lifecycle sync.Mutex

	mu       sync.Mutex
	channels map[string]*manualChannel
	// pending holds messages that arrived while no channel was attached,
	// for the next one.
	pending []JsonRpc2ReceivedMessage
	current *manualConnection
}

type manualChannel struct {
	c chan JsonRpc2ReceivedMessage
	// detached is closed when the channel is removed, so a delivery to it
	// does not block.
	detached chan struct{}
}

type manualConnection struct {
	conn net.Conn
	// ctx ends webhook posts when the receiver stops.
	ctx    context.Context
	cancel context.CancelFunc
	// subscription receives the subscription id from signal-cli's reply.
	subscription chan int64
	// unsubscribed is closed when signal-cli confirms the unsubscription.
	unsubscribed  chan struct{}
	unsubscribeId string
	stopping      bool
	// done is closed when the read loop has ended.
	done chan struct{}
}

func newManualReceiver(address string, account string, webhookUrl string) *manualReceiver {
	return &manualReceiver{address: address, account: account, webhookUrl: webhookUrl, channels: map[string]*manualChannel{}}
}

// attach returns a new channel for the account's messages, and its id for
// detach. The channel is closed when the connection to signal-cli ends or
// signal-cli refuses the subscription.
func (m *manualReceiver) attach() (chan JsonRpc2ReceivedMessage, string, error) {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()

	u, err := uuid.NewV4()
	if err != nil {
		return nil, "", err
	}
	id := u.String()
	ch := &manualChannel{c: make(chan JsonRpc2ReceivedMessage), detached: make(chan struct{})}
	m.mu.Lock()
	m.channels[id] = ch
	running := m.current != nil
	m.mu.Unlock()

	if !running {
		if err := m.start(); err != nil {
			m.mu.Lock()
			delete(m.channels, id)
			m.mu.Unlock()
			return nil, "", err
		}
	}
	return ch.c, id, nil
}

// detach removes a channel. Removing the last one ends the subscription.
func (m *manualReceiver) detach(id string) {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()

	m.mu.Lock()
	ch, ok := m.channels[id]
	if ok {
		delete(m.channels, id)
		close(ch.detached)
	}
	last := len(m.channels) == 0
	c := m.current
	m.mu.Unlock()

	if ok && last && c != nil {
		m.stop(c)
	}
}

// start connects and subscribes. It does not wait for the reply: signal-cli
// can write messages before it, and the read loop only reaches the reply
// once the attached channel has taken them.
func (m *manualReceiver) start() error {
	conn, err := net.Dial("tcp", m.address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &manualConnection{
		conn:         conn,
		ctx:          ctx,
		cancel:       cancel,
		subscription: make(chan int64, 1),
		unsubscribed: make(chan struct{}),
		done:         make(chan struct{}),
	}
	u, err := uuid.NewV4()
	if err != nil {
		conn.Close()
		return err
	}
	subscribeId := u.String()
	m.mu.Lock()
	m.current = c
	m.mu.Unlock()
	go m.read(c, subscribeId)

	if err := writeJsonRpcRequest(conn, subscribeId, "subscribeReceive", m.account, nil); err != nil {
		m.mu.Lock()
		c.stopping = true
		m.mu.Unlock()
		conn.Close()
		<-c.done
		return err
	}
	log.Debugf("Subscribing to receive notifications for account %s", m.account)
	return nil
}

// stop unsubscribes and keeps reading until signal-cli confirms, so the
// messages it writes meanwhile are kept for the next channel, then closes
// the connection.
func (m *manualReceiver) stop(c *manualConnection) {
	m.mu.Lock()
	c.stopping = true
	m.mu.Unlock()

	timeout := time.After(unsubscribeTimeout)
	select {
	case subscription := <-c.subscription:
		u, err := uuid.NewV4()
		if err != nil {
			break
		}
		m.mu.Lock()
		c.unsubscribeId = u.String()
		m.mu.Unlock()
		err = writeJsonRpcRequest(c.conn, c.unsubscribeId, "unsubscribeReceive", m.account, map[string]any{"subscription": subscription})
		if err != nil {
			break
		}
		select {
		case <-c.unsubscribed:
			log.Debugf("Unsubscribed from receive notifications for account %s (subscription=%d)", m.account, subscription)
		case <-c.done:
		case <-timeout:
			log.Warnf("signal-cli did not confirm the end of subscription %d for account %s", subscription, m.account)
		}
	case <-c.done:
	case <-timeout:
		log.Warnf("signal-cli did not confirm the receive subscription for account %s", m.account)
	}
	// A webhook that does not answer would keep the read loop from ending.
	// The message it was posting is still kept for the next channel.
	c.cancel()
	c.conn.Close()
	<-c.done
}

func (m *manualReceiver) read(c *manualConnection, subscribeId string) {
	defer close(c.done)
	defer m.connectionEnded(c)

	// Messages kept while no channel was attached come first.
	m.mu.Lock()
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()
	for _, msg := range pending {
		m.deliver(msg)
	}

	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			m.mu.Lock()
			stopping := c.stopping
			m.mu.Unlock()
			if !stopping {
				log.Errorf("Lost the receive connection to signal-cli for account %s: %s", m.account, err.Error())
			}
			return
		}
		var msg struct {
			Id     string          `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  *Error          `json:"error"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			log.Warn("Received unparsable message: ", string(line))
			continue
		}
		switch {
		case msg.Method == "receive":
			// In manual mode signal-cli wraps the envelope in
			// {"subscription":N,"result":{...}}; deliver the envelope as in
			// on-start mode.
			var wrapper struct {
				Result json.RawMessage `json:"result"`
			}
			params := msg.Params
			if err := json.Unmarshal(msg.Params, &wrapper); err == nil && len(wrapper.Result) > 0 {
				params = wrapper.Result
			}
			if m.webhookUrl != "" {
				if err := postMessageToWebhookContext(c.ctx, m.webhookUrl, line); err != nil {
					log.Error("Couldn't post data to webhook: ", err)
				}
			}
			m.deliver(JsonRpc2ReceivedMessage{Method: msg.Method, Params: params})
		case msg.Id == subscribeId:
			if msg.Error != nil {
				log.Errorf("Couldn't create subscription for account %s: %s", m.account, msg.Error.Message)
				return
			}
			var subscription int64
			if err := json.Unmarshal(msg.Result, &subscription); err != nil {
				log.Errorf("Couldn't read subscription for account %s: %s", m.account, err.Error())
				return
			}
			c.subscription <- subscription
		case msg.Id != "":
			m.mu.Lock()
			isUnsubscribe := msg.Id == c.unsubscribeId
			m.mu.Unlock()
			if isUnsubscribe {
				close(c.unsubscribed)
			}
		}
	}
}

// deliver hands msg to every attached channel, waiting for each to take
// it. When every channel it tried detached first, it tries the channels
// attached since; a message is kept for the next channel only when none is
// attached.
func (m *manualReceiver) deliver(msg JsonRpc2ReceivedMessage) {
	for {
		m.mu.Lock()
		if len(m.channels) == 0 {
			m.pending = append(m.pending, msg)
			m.mu.Unlock()
			return
		}
		channels := make([]*manualChannel, 0, len(m.channels))
		for _, ch := range m.channels {
			channels = append(channels, ch)
		}
		m.mu.Unlock()

		delivered := false
		for _, ch := range channels {
			select {
			case ch.c <- msg:
				delivered = true
			case <-ch.detached:
			}
		}
		if delivered {
			return
		}
	}
}

// connectionEnded closes the attached channels, so their consumers detach
// and a new attach connects again.
func (m *manualReceiver) connectionEnded(c *manualConnection) {
	c.cancel()
	c.conn.Close()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current != c {
		return
	}
	m.current = nil
	for id, ch := range m.channels {
		close(ch.c)
		delete(m.channels, id)
	}
}

func writeJsonRpcRequest(conn net.Conn, id string, method string, account string, params map[string]any) error {
	if params == nil {
		params = map[string]any{}
	}
	params["account"] = account
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "id": id, "params": params})
	if err != nil {
		return err
	}
	_, err = conn.Write(append(b, '\n'))
	return err
}
