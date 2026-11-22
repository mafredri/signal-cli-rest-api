package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/utils"
	uuid "github.com/gofrs/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
)

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type JsonRpc2MessageResponse struct {
	Id     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Err    Error           `json:"error"`
}

type JsonRpc2ReceivedMessage struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Err    Error           `json:"error"`
}

type RateLimitMessage struct {
	Response RateLimitResponse `json:"response"`
}

type RateLimitResponse struct {
	Results []RateLimitResult `json:"results"`
}

type RateLimitResult struct {
	Token string `json:"token"`
}

type RateLimitErrorType struct {
	ChallengeTokens []string
	Err             error
}

func (r *RateLimitErrorType) Error() string {
	return r.Err.Error()
}

type JsonRpc2Client struct {
	conn                     net.Conn
	receivedResponsesById    map[string]chan JsonRpc2MessageResponse
	receivedMessagesChannels map[string]chan JsonRpc2ReceivedMessage
	signalCliApiConfig       *utils.SignalCliApiConfig
	number                   string
	jsonRpcReceiveMode       JsonRpcReceiveMode
	receivedMessagesMutex    sync.Mutex
	receivedResponsesMutex   sync.Mutex
	// In manual receive mode each account with attached receive channels
	// has a manualReceiver; manualChannels maps a channel id to it.
	manualReceivers   map[string]*manualReceiver
	manualChannels    map[string]*manualReceiver
	manualMutex       sync.Mutex
	receiveWebhookUrl string
	address           string
}

func NewJsonRpc2Client(signalCliApiConfig *utils.SignalCliApiConfig, number string, jsonRpcReceiveMode JsonRpcReceiveMode) *JsonRpc2Client {
	return &JsonRpc2Client{
		signalCliApiConfig:       signalCliApiConfig,
		number:                   number,
		jsonRpcReceiveMode:       jsonRpcReceiveMode,
		receivedResponsesById:    make(map[string]chan JsonRpc2MessageResponse),
		receivedMessagesChannels: make(map[string]chan JsonRpc2ReceivedMessage),
		manualReceivers:          make(map[string]*manualReceiver),
		manualChannels:           make(map[string]*manualReceiver),
	}
}

// Dial connects to signal-cli at address. The address is set only here,
// before the client is used: manual receivers read it to open their own
// connections while ReceiveData reconnects.
func (r *JsonRpc2Client) Dial(address string, maxRetries int) error {
	r.address = address
	return r.dial(maxRetries)
}

func (r *JsonRpc2Client) dial(maxRetries int) error {
	var err error
	address := r.address
	connected := false
	for i := 0; i < maxRetries; i++ {
		r.conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Info("Waiting for signal-cli to start up in daemon mode...")
			time.Sleep(2 * time.Second)
			continue
		}

		connected = true
		log.Info("Successfully connected to signal-cli in daemon mode")
		break
	}

	if !connected {
		return err
	}

	return nil
}

func (r *JsonRpc2Client) getRaw(command string, account *string, args interface{}) (string, error) {
	type Request struct {
		JsonRpc string      `json:"jsonrpc"`
		Method  string      `json:"method"`
		Id      string      `json:"id"`
		Params  interface{} `json:"params,omitempty"`
	}

	trustModeStr := ""
	trustMode, err := r.signalCliApiConfig.GetTrustModeForNumber(r.number)
	if err == nil {
		trustModeStr, err = utils.TrustModeToString(trustMode)
		if err != nil {
			trustModeStr = ""
			log.Error("Invalid trust mode: ", trustModeStr)
		}
	}

	u, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	fullCommand := Request{JsonRpc: "2.0", Method: command, Id: u.String()}
	if args != nil {
		fullCommand.Params = args
	}

	fullCommandBytes, err := json.Marshal(fullCommand)
	if err != nil {
		return "", err
	}

	if trustModeStr != "" {
		fullCommandBytes, err = sjson.SetBytes(fullCommandBytes, "params.trustNewIdentities", trustModeStr)
		if err != nil {
			return "", err
		}
	}

	if account != nil {
		fullCommandBytes, err = sjson.SetBytes(fullCommandBytes, "params.account", account)
		if err != nil {
			return "", err
		}
	}

	log.Debug("json-rpc command: ", string(fullCommandBytes))

	// Register before writing: the reader can observe a fast reply before
	// Write returns. Buffer one response so write failure cleanup cannot leave
	// the receiver blocked on a channel whose caller has already returned.
	responseChan := make(chan JsonRpc2MessageResponse, 1)
	r.receivedResponsesMutex.Lock()
	r.receivedResponsesById[u.String()] = responseChan
	r.receivedResponsesMutex.Unlock()
	defer func() {
		r.receivedResponsesMutex.Lock()
		delete(r.receivedResponsesById, u.String())
		r.receivedResponsesMutex.Unlock()
	}()

	_, err = r.conn.Write([]byte(string(fullCommandBytes) + "\n"))
	if err != nil {
		return "", err
	}

	resp := <-responseChan

	log.Debug("json-rpc command response message: ", string(resp.Result))
	log.Debug("json-rpc response error: ", string(resp.Err.Message))

	if resp.Err.Code != 0 {
		log.Debug("json-rpc command error code: ", resp.Err.Code)
		if resp.Err.Code == -5 {
			var rateLimitMessage RateLimitMessage
			err = json.Unmarshal(resp.Err.Data, &rateLimitMessage)
			if err != nil {
				return "", errors.New(resp.Err.Message + " (Couldn't parse JSON for more details")
			}
			challengeTokens := []string{}
			for _, rateLimitResult := range rateLimitMessage.Response.Results {
				challengeTokens = append(challengeTokens, rateLimitResult.Token)
			}

			return "", &RateLimitErrorType{
				ChallengeTokens: challengeTokens,
				Err:             errors.New(resp.Err.Message),
			}
		} else if resp.Err.Code == -4 {
			return "", errors.New("Unable to complete request due to untrusted recipient keys")
		}
		return "", errors.New(resp.Err.Message)
	}

	return string(resp.Result), nil
}

func postMessageToWebhook(webhookUrl string, data []byte) error {
	return postMessageToWebhookContext(context.Background(), webhookUrl, data)
}

func postMessageToWebhookContext(ctx context.Context, webhookUrl string, data []byte) error {
	r, err := http.NewRequestWithContext(ctx, "POST", webhookUrl, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	r.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	log.Info(res.StatusCode)
	if res.StatusCode != 200 && res.StatusCode != 201 {
		return errors.New("Unexpected status code returned (" + strconv.Itoa(res.StatusCode) + ")")
	}
	return nil
}

func (r *JsonRpc2Client) ReceiveData(number string, receiveWebhookUrl string) {
	connbuf := bufio.NewReader(r.conn)
	for {
		str, err := connbuf.ReadString('\n')
		if err != nil {
			log.Error("Lost connection to signal-cli...attempting to reconnect (", err.Error(), ")")
			r.conn.Close()
			err = r.dial(15)
			if err != nil {
				log.Fatal("Unable to reconnect to signal-cli: ", err.Error(), "...aborting")
			}
			connbuf = bufio.NewReader(r.conn)
			log.Info("Successfully reconnected to signal-cli")
			continue
		}
		log.Debug("json-rpc received data: ", str)

		var resp1 JsonRpc2ReceivedMessage
		json.Unmarshal([]byte(str), &resp1)
		if resp1.Method == "receive" {
			r.receivedMessagesMutex.Lock()
			for _, c := range r.receivedMessagesChannels {
				select {
				case c <- resp1:
					log.Debug("Message sent to golang channel")
				default:
					log.Debug("Couldn't send message to golang channel, as there's no receiver")
				}
			}
			r.receivedMessagesMutex.Unlock()

			if receiveWebhookUrl != "" {
				err = postMessageToWebhook(receiveWebhookUrl, []byte(str))
				if err != nil {
					log.Error("Couldn't post data to webhook: ", err)
				}
			}
		}

		var resp2 JsonRpc2MessageResponse
		err = json.Unmarshal([]byte(str), &resp2)
		if err == nil {
			if resp2.Id != "" {
				// Remove the entry while delivering, so a repeated id cannot
				// send a second response into the one-slot channel.
				r.receivedResponsesMutex.Lock()
				responseChan, ok := r.receivedResponsesById[resp2.Id]
				delete(r.receivedResponsesById, resp2.Id)
				r.receivedResponsesMutex.Unlock()
				if ok {
					responseChan <- resp2
				}
			}
		} else {
			log.Warn("Received unparsable message: ", str)
		}
	}
}

// GetReceiveChannel returns a channel for the account's messages and its id
// for RemoveReceiveChannel. In manual receive mode the channel is closed
// when the wrapper can no longer receive for the account.
func (r *JsonRpc2Client) GetReceiveChannel(account string) (chan JsonRpc2ReceivedMessage, string, error) {
	if r.jsonRpcReceiveMode == Manual {
		r.manualMutex.Lock()
		receiver, ok := r.manualReceivers[account]
		if !ok {
			receiver = newManualReceiver(r.address, account, r.receiveWebhookUrl)
			r.manualReceivers[account] = receiver
		}
		r.manualMutex.Unlock()
		c, id, err := receiver.attach()
		if err != nil {
			return nil, "", fmt.Errorf("Couldn't create subscription for account %s: %w", account, err)
		}
		r.manualMutex.Lock()
		r.manualChannels[id] = receiver
		r.manualMutex.Unlock()
		return c, id, nil
	}

	c := make(chan JsonRpc2ReceivedMessage, 64)

	channelUuid, err := uuid.NewV4()
	if err != nil {
		return c, "", err
	}

	r.receivedMessagesMutex.Lock()
	r.receivedMessagesChannels[channelUuid.String()] = c
	r.receivedMessagesMutex.Unlock()

	return c, channelUuid.String(), nil
}

func (r *JsonRpc2Client) RemoveReceiveChannel(channelUuid string) {
	if r.jsonRpcReceiveMode == Manual {
		r.manualMutex.Lock()
		receiver, ok := r.manualChannels[channelUuid]
		delete(r.manualChannels, channelUuid)
		r.manualMutex.Unlock()
		if ok {
			receiver.detach(channelUuid)
		}
		return
	}

	r.receivedMessagesMutex.Lock()
	delete(r.receivedMessagesChannels, channelUuid)
	r.receivedMessagesMutex.Unlock()
}
