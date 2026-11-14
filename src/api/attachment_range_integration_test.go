package api

import (
	"bytes"
	"fmt"
	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestAttachmentHTTPRangesAndStreaming(t *testing.T) {
	for _, mode := range []client.SignalCliMode{client.Normal, client.JsonRpc} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "attachments"), 0700); err != nil {
				t.Fatal(err)
			}
			body := bytes.Repeat([]byte("attachment content\n"), 65536)
			if err := os.WriteFile(filepath.Join(root, "attachments", "sample"), body, 0600); err != nil {
				t.Fatal(err)
			}
			sc := client.NewSignalClient(root, root+"/", root+"/", mode, "", root+"/settings.yml", "", client.OnStart)
			api := NewApi(sc)
			gin.SetMode(gin.ReleaseMode)
			router := gin.New()
			router.GET("/files/:attachment", api.ServeAttachment)
			router.HEAD("/files/:attachment", api.ServeAttachment)
			server := httptest.NewServer(router)
			defer server.Close()
			for _, tt := range []struct {
				name, method, rangeValue string
				status                   int
				start, end               int
			}{
				{"whole", "GET", "", 200, 0, len(body)}, {"first", "GET", "bytes=0-4095", 206, 0, 4096}, {"middle", "GET", "bytes=4096-8191", 206, 4096, 8192}, {"suffix", "GET", "bytes=-17", 206, len(body) - 17, len(body)}, {"last", "GET", "bytes=1200000-9999999", 206, 1200000, len(body)}, {"unsatisfiable", "GET", "bytes=9999999-10000000", 416, 0, 0}, {"head", "HEAD", "", 200, 0, 0},
			} {
				t.Run(tt.name, func(t *testing.T) {
					req, _ := http.NewRequest(tt.method, server.URL+"/files/sample", nil)
					req.Header.Set("Range", tt.rangeValue)
					res, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer res.Body.Close()
					data, err := io.ReadAll(res.Body)
					if err != nil || res.StatusCode != tt.status {
						t.Fatalf("response %d %v", res.StatusCode, err)
					}
					if tt.status < 400 && !bytes.Equal(data, body[tt.start:tt.end]) {
						t.Fatalf("wrong bytes: %d", len(data))
					}
					if tt.status < 400 && res.Header.Get("Accept-Ranges") != "bytes" {
						t.Fatal("missing ranges")
					}
					if tt.status == 206 && res.Header.Get("Content-Range") == "" {
						t.Fatal("missing range identity")
					}
				})
			}
			if err := os.Mkdir(filepath.Join(root, "attachments", "directory"), 0700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"missing", "..", "directory"} {
				res, err := http.Get(server.URL + "/files/" + name)
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				if res.StatusCode < 400 {
					t.Fatalf("unsafe/missing attachment %s accepted", name)
				}
			}
			// An early receiver disconnect is permitted; a later reader still gets
			// its independently requested byte range. This is not an RSS benchmark.
			req, _ := http.NewRequest("GET", server.URL+"/files/sample", nil)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			small := make([]byte, 128)
			if _, err = res.Body.Read(small); err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			request, _ := http.NewRequest("GET", server.URL+"/files/sample", nil)
			request.Header.Set("Range", "bytes=11-22")
			next, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(next.Body)
			next.Body.Close()
			if err != nil || next.StatusCode != 206 || !bytes.Equal(data, body[11:23]) {
				t.Fatal("reader after disconnect failed", err)
			}
		})
	}
}
