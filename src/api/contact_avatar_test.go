package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
)

func TestContactAvatarStatus(t *testing.T) {
	for _, tt := range []struct {
		name, stderr string
		status       int
	}{
		// signal-cli's getAvatar prints this when the contact has no avatar.
		{"no avatar", "Could not find avatar", 404},
		{"other failure", "Unregistered user", 400},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			script := "#!/bin/sh\nprintf '" + tt.stderr + "' >&2\nexit 1\n"
			if err := os.WriteFile(filepath.Join(root, "signal-cli"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
			sc := client.NewSignalClient(root, root+"/", root+"/", client.Normal, "", root+"/settings.yml", "", client.OnStart)
			if err := sc.Init(1); err != nil {
				t.Fatal(err)
			}
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.GET("/v1/contacts/:number/:uuid/avatar", NewApi(sc).GetProfileAvatar)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/contacts/+15550001111/11111111-1111-4111-8111-111111111111/avatar", nil))
			if recorder.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", recorder.Code, tt.status, recorder.Body.String())
			}
		})
	}
}
