package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ds "github.com/bbernhard/signal-cli-rest-api/datastructs"
)

// Explicit ranges reach signal-cli as text-style arguments and leave the
// message text, and so the mention offsets, unchanged.
func TestSendPassesExplicitTextStyles(t *testing.T) {
	if config := os.Getenv("WRAPPER_TEXT_STYLES_CONFIG"); config != "" {
		dir := filepath.Dir(config)
		c := NewSignalClient(dir, dir+"/", dir+"/", JsonRpc, config, filepath.Join(dir, "api.yaml"), "", OnStart)
		if err := c.Init(1); err != nil {
			t.Fatal(err)
		}
		styles := []ds.MessageTextStyle{{Start: 0, Length: 4, Style: "BOLD"}, {Start: 2, Length: 2, Style: "ITALIC"}}
		mentions := []ds.MessageMention{{Start: 5, Length: 1, Author: "a11e4b7a-0000-4000-8000-000000000001"}}
		if _, err := c.SendV2("+15550001111", "**hi ￼", []string{"+15550002222"}, nil, "", mentions, nil, nil, nil, nil, nil, styles, nil, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		styled := "styled"
		if _, err := c.SendV2("+15550001111", "x", []string{"+15550002222"}, nil, "", nil, nil, nil, nil, nil, &styled, styles, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Fatalf("styled text_mode with text_styles: %v", err)
		}
		if _, err := c.SendV2("+15550001111", "x", []string{"+15550002222"}, nil, "", nil, nil, nil, nil, nil, nil, []ds.MessageTextStyle{{Start: 0, Length: 1, Style: "UNDERLINE"}}, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "invalid text style") {
			t.Fatalf("unknown style: %v", err)
		}
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dir := t.TempDir()
	config := filepath.Join(dir, "rpc.yaml")
	if err = os.WriteFile(config, []byte(fmt.Sprintf("config:\n  '<multi-account>':\n    tcp_port: %d\n", listener.Addr().(*net.TCPAddr).Port)), 0600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	result := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(15 * time.Second))
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			result <- fmt.Errorf("missing send RPC: %v", scanner.Err())
			return
		}
		var request struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		if err = json.Unmarshal(scanner.Bytes(), &request); err != nil {
			result <- err
			return
		}
		if request.Method != "send" || request.Params["message"] != "**hi ￼" {
			result <- fmt.Errorf("wrong send RPC %v", request)
			return
		}
		if got, want := request.Params["text-style"], []any{"0:4:BOLD", "2:2:ITALIC"}; !reflect.DeepEqual(got, want) {
			result <- fmt.Errorf("text-style %v, want %v", got, want)
			return
		}
		if got, want := request.Params["mentions"], []any{"5:1:a11e4b7a-0000-4000-8000-000000000001"}; !reflect.DeepEqual(got, want) {
			result <- fmt.Errorf("mentions %v, want %v", got, want)
			return
		}
		response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"timestamp": 1}})
		if _, err = conn.Write(append(response, '\n')); err != nil {
			result <- err
			return
		}
		// The rejected requests must not reach signal-cli.
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if scanner.Scan() {
			result <- fmt.Errorf("a rejected send reached signal-cli: %s", scanner.Bytes())
			return
		}
		result <- nil
		<-stop
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSendPassesExplicitTextStyles$")
	cmd.Env = append(os.Environ(), "WRAPPER_TEXT_STYLES_CONFIG="+config)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("send child: %s %v", output, err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
}
