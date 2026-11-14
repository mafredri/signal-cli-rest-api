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
	"testing"
	"time"
)

// Run the real JSON-RPC client in a child test process because its receive loop
// reconnects forever and has no shutdown API. Keeping the peer alive until the
// child exits avoids turning fixture cleanup into an upstream reconnect/fatal.
func TestGroupPermissionsOverRPC(t *testing.T) {
	if config := os.Getenv("WRAPPER_GROUP_PERMISSION_CONFIG"); config != "" {
		dir := filepath.Dir(config)
		client := NewSignalClient(dir, dir, dir, JsonRpc, config, filepath.Join(dir, "api.yaml"), "", OnStart)
		if err := client.Init(1); err != nil {
			t.Fatal(err)
		}
		groups, err := client.GetGroups("+15550001111")
		if err != nil {
			t.Fatal(err)
		}
		if len(groups) != 2 {
			t.Fatalf("got %d groups, want 2", len(groups))
		}
		for i, want := range []struct{ send, edit, add string }{
			{"every-member", "only-admins", "only-admins"},
			{"only-admins", "every-member", "every-member"},
		} {
			got := groups[i].Permissions
			if got.SendMessages != want.send || got.EditGroup != want.edit || got.AddMembers != want.add {
				t.Errorf("group %d permissions = %+v, want send=%s edit=%s add=%s", i, got, want.send, want.edit, want.add)
			}
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
	peerErrors := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			peerErrors <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			peerErrors <- fmt.Errorf("missing RPC request: %v", scanner.Err())
			return
		}
		var request struct {
			ID     string `json:"id"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			peerErrors <- err
			return
		}
		if request.Method != "listGroups" {
			peerErrors <- fmt.Errorf("unexpected RPC method %q", request.Method)
			return
		}
		// Native listGroups shape: permissionEditDetails is independent of
		// permissionSendMessage. Deliberately make them differ in both directions.
		groups := json.RawMessage(`[
			{"id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","name":"Open discussion","isMember":true,"permissionSendMessage":"EVERY_MEMBER","permissionEditDetails":"ONLY_ADMINS","permissionAddMember":"ONLY_ADMINS"},
			{"id":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=","name":"Announcement","isMember":true,"permissionSendMessage":"ONLY_ADMINS","permissionEditDetails":"EVERY_MEMBER","permissionAddMember":"EVERY_MEMBER"}
		]`)
		data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": groups})
		if err == nil {
			_, err = conn.Write(append(data, '\n'))
		}
		if err != nil {
			peerErrors <- err
			return
		}
		<-stop
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGroupPermissionsOverRPC$")
	child.Env = append(os.Environ(), "WRAPPER_GROUP_PERMISSION_CONFIG="+config)
	output, err := child.CombinedOutput()
	if err != nil {
		t.Fatalf("real client subprocess failed: %v\n%s", err, output)
	}
	select {
	case err := <-peerErrors:
		t.Fatal(err)
	default:
	}
}
