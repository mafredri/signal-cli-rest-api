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

func TestPartialProfileRPCPreservesOmittedFields(t *testing.T) {
	if config := os.Getenv("WRAPPER_PROFILE_PATCH_CONFIG"); config != "" {
		dir := filepath.Dir(config)
		c := NewSignalClient(dir, dir+"/", dir+"/", JsonRpc, config, filepath.Join(dir, "api.yaml"), "", OnStart)
		if err := c.Init(1); err != nil {
			t.Fatal(err)
		}
		name := "Example Bot"
		avatar := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6x2kAAAAASUVORK5CYII="
		for _, p := range []struct {
			name, avatar *string
			remove       bool
		}{{&name, nil, false}, {nil, &avatar, false}, {nil, nil, true}} {
			if err := c.PatchProfile("+15550001111", p.name, p.avatar, p.remove, nil); err != nil {
				t.Fatal(err)
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
		for i := 0; i < 3; i++ {
			if !scanner.Scan() {
				result <- fmt.Errorf("missing profile RPC %d", i)
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
			if request.Method != "updateProfile" || request.Params["account"] != "+15550001111" {
				result <- fmt.Errorf("wrong profile RPC %v", request)
				return
			}
			_, name := request.Params["given-name"]
			_, avatar := request.Params["avatar"]
			_, remove := request.Params["remove-avatar"]
			if name != (i == 0) || avatar != (i == 1) || remove != (i == 2) {
				result <- fmt.Errorf("omitted fields changed %d: %v", i, request.Params)
				return
			}
			if avatar {
				if _, err = os.Stat(request.Params["avatar"].(string)); err != nil {
					result <- err
					return
				}
			}
			response, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{}})
			if _, err = conn.Write(append(response, '\n')); err != nil {
				result <- err
				return
			}
		}
		result <- nil
		<-stop
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPartialProfileRPCPreservesOmittedFields$")
	cmd.Env = append(os.Environ(), "WRAPPER_PROFILE_PATCH_CONFIG="+config)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("profile producer child: %s %v", output, err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".png" {
			t.Fatal("avatar bytes retained")
		}
	}
}
