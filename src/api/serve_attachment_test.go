package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
)

// pngHeader is enough of a PNG for content detection.
var pngHeader = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

func newAttachmentServer(t *testing.T, files map[string][]byte) (string, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "attachments")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	sc := client.NewSignalClient(root, root+"/", root+"/", client.Normal, "", root+"/settings.yml", "", client.OnStart)
	api := NewApi(sc)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/v1/attachments/:attachment", api.ServeAttachment)
	router.HEAD("/v1/attachments/:attachment", api.ServeAttachment)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server.URL + "/v1/attachments/", dir
}

func doAttachmentRequest(t *testing.T, method, url string, header map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, body
}

func TestServeAttachmentRanges(t *testing.T) {
	body := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte("0123456789"), 100000)...)
	size := len(body)
	base, _ := newAttachmentServer(t, map[string][]byte{"sample": body})

	for _, tt := range []struct {
		name         string
		method       string
		rangeHeader  string
		status       int
		contentRange string
		start, end   int
	}{
		{"whole", "GET", "", 200, "", 0, size},
		{"first bytes", "GET", "bytes=0-4095", 206, fmt.Sprintf("bytes 0-4095/%d", size), 0, 4096},
		{"middle", "GET", "bytes=4096-8191", 206, fmt.Sprintf("bytes 4096-8191/%d", size), 4096, 8192},
		{"suffix", "GET", "bytes=-17", 206, fmt.Sprintf("bytes %d-%d/%d", size-17, size-1, size), size - 17, size},
		{"end past the file", "GET", fmt.Sprintf("bytes=%d-%d", size-10, size+1000), 206, fmt.Sprintf("bytes %d-%d/%d", size-10, size-1, size), size - 10, size},
		{"unsatisfiable", "GET", fmt.Sprintf("bytes=%d-", size), 416, fmt.Sprintf("bytes */%d", size), 0, 0},
		{"head", "HEAD", "", 200, "", 0, 0},
		{"head ignores a range", "HEAD", "bytes=0-2", 200, "", 0, 0},
		{"head ignores an unsatisfiable range", "HEAD", fmt.Sprintf("bytes=%d-", size), 200, "", 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, got := doAttachmentRequest(t, tt.method, base+"sample", map[string]string{"Range": tt.rangeHeader})
			if res.StatusCode != tt.status {
				t.Fatalf("status %d, want %d", res.StatusCode, tt.status)
			}
			if cr := res.Header.Get("Content-Range"); cr != tt.contentRange {
				t.Errorf("Content-Range %q, want %q", cr, tt.contentRange)
			}
			if tt.status == 416 {
				return
			}
			if ar := res.Header.Get("Accept-Ranges"); ar != "bytes" {
				t.Errorf("Accept-Ranges %q, want bytes", ar)
			}
			if ct := res.Header.Get("Content-Type"); ct != "image/png" {
				t.Errorf("Content-Type %q, want image/png", ct)
			}
			wantLength := tt.end - tt.start
			if tt.method == "HEAD" {
				wantLength = size
			} else if !bytes.Equal(got, body[tt.start:tt.end]) {
				t.Errorf("got %d bytes, want bytes %d-%d", len(got), tt.start, tt.end)
			}
			if cl := res.Header.Get("Content-Length"); cl != fmt.Sprint(wantLength) {
				t.Errorf("Content-Length %s, want %d", cl, wantLength)
			}
		})
	}
}

func TestServeAttachmentConditionalRequests(t *testing.T) {
	base, dir := newAttachmentServer(t, map[string][]byte{"sample": []byte("0123456789")})
	modified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(dir, "sample"), modified, modified); err != nil {
		t.Fatal(err)
	}
	lastModified := modified.Format(http.TimeFormat)
	earlier := modified.Add(-time.Hour).Format(http.TimeFormat)

	for _, tt := range []struct {
		name   string
		header map[string]string
		status int
		body   string
	}{
		{"not modified since", map[string]string{"If-Modified-Since": lastModified}, 304, ""},
		{"modified since", map[string]string{"If-Modified-Since": earlier}, 200, "0123456789"},
		{"unmodified since", map[string]string{"If-Unmodified-Since": earlier}, 412, ""},
		{"if-range matches", map[string]string{"Range": "bytes=2-4", "If-Range": lastModified}, 206, "234"},
		{"if-range stale", map[string]string{"Range": "bytes=2-4", "If-Range": earlier}, 200, "0123456789"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, got := doAttachmentRequest(t, "GET", base+"sample", tt.header)
			if res.StatusCode != tt.status || string(got) != tt.body {
				t.Fatalf("got %d %q, want %d %q", res.StatusCode, got, tt.status, tt.body)
			}
			if lm := res.Header.Get("Last-Modified"); lm != lastModified {
				t.Errorf("Last-Modified %q, want %q", lm, lastModified)
			}
		})
	}
}

// signal-cli names a stored attachment after the sender's file name, so the
// extension says nothing reliable about the content.
func TestServeAttachmentTypeComesFromContent(t *testing.T) {
	base, _ := newAttachmentServer(t, map[string][]byte{"picture.html": pngHeader})
	res, _ := doAttachmentRequest(t, "GET", base+"picture.html", nil)
	if ct := res.Header.Get("Content-Type"); ct != "image/png" {
		t.Fatalf("Content-Type %q, want image/png", ct)
	}
}

func TestServeAttachmentErrors(t *testing.T) {
	base, dir := newAttachmentServer(t, nil)
	if err := os.Mkdir(filepath.Join(dir, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name   string
		status int
	}{
		{"missing", 404},
		{"directory", 400},
		{"..", 400},
		// SecureJoin resolves the link inside the attachment directory, where
		// its target does not exist.
		{"escape", 404},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{"GET", "HEAD"} {
				res, _ := doAttachmentRequest(t, method, base+tt.name, nil)
				if res.StatusCode != tt.status {
					t.Errorf("%s status %d, want %d", method, res.StatusCode, tt.status)
				}
			}
		})
	}
}
