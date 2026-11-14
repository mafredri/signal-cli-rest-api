package api

import (
	"bytes"
	"encoding/base64"
	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPartialProfileHTTPPreservesOmittedFields(t *testing.T) {
	root := t.TempDir()
	log := filepath.Join(root, "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PROFILE_TEST_ARGS\"\n"
	if err := os.WriteFile(filepath.Join(root, "signal-cli"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("PROFILE_TEST_ARGS", log)
	sc := client.NewSignalClient(root, root+"/", root+"/", client.Normal, "", root+"/settings.yml", "", client.OnStart)
	if err := sc.Init(1); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.PATCH("/profiles/:number", NewApi(sc).PatchProfile)
	avatar := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6x2kAAAAASUVORK5CYII="
	if _, err := base64.StdEncoding.DecodeString(avatar); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, body    string
		status        int
		want, exclude string
	}{
		{"name only", `{"name":"Example Bot"}`, 204, "--given-name\nExample Bot", "--remove-avatar"},
		{"avatar only", `{"base64_avatar":"` + avatar + `"}`, 204, "--avatar", "--given-name"},
		{"remove avatar", `{"remove_avatar":true}`, 204, "--remove-avatar", "--given-name"},
		{"empty", `{}`, 400, "", ""},
		{"conflicting avatar", `{"remove_avatar":true,"base64_avatar":"` + avatar + `"}`, 400, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPatch, "/profiles/+15550001111", bytes.NewBufferString(tt.body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			if recorder.Code != tt.status {
				t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
			}
			if tt.status != 204 {
				return
			}
			got, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(got), tt.want) || strings.Contains(string(got), tt.exclude) {
				t.Fatalf("wrong partial CLI args: %s", got)
			}
		})
	}
	entries, _ := os.ReadDir(root)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".png") {
			t.Fatal("avatar temporary bytes retained")
		}
	}
}

func TestContactAvatarAbsentReturnsNotFound(t *testing.T) {
	root := t.TempDir()
	script := "#!/bin/sh\nprintf 'Could not find avatar' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(root, "signal-cli"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	sc := client.NewSignalClient(root, root+"/", root+"/", client.Normal, "", root+"/settings.yml", "", client.OnStart)
	if err := sc.Init(1); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.GET("/contacts/:number/:uuid/avatar", NewApi(sc).GetProfileAvatar)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/contacts/+15550001111/11111111-1111-4111-8111-111111111111/avatar", nil)
	r.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing avatar status %d: %s", recorder.Code, recorder.Body.String())
	}
}
