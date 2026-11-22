package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bbernhard/signal-cli-rest-api/client"
	"github.com/gin-gonic/gin"
)

func TestPatchProfile(t *testing.T) {
	root := t.TempDir()
	argsFile := filepath.Join(root, "args")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"" + argsFile + "\"\n"
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
	router.PATCH("/v1/profiles/:number", NewApi(sc).PatchProfile)
	const avatar = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6x2kAAAAASUVORK5CYII="

	for _, tt := range []struct {
		name, account, body string
		status              int
		// args are signal-cli's arguments after "updateProfile"; an avatar
		// path is written as "<file>".
		args []string
	}{
		{"name", "+15550001111", `{"name":"Example Bot"}`, 204, []string{"--given-name", "Example Bot"}},
		{"about", "+15550001111", `{"about":"Hello"}`, 204, []string{"--about", "Hello"}},
		{"clear about", "+15550001111", `{"about":""}`, 204, []string{"--about", ""}},
		{"name and about", "+15550001111", `{"name":"Example Bot","about":"Hello"}`, 204, []string{"--given-name", "Example Bot", "--about", "Hello"}},
		{"avatar", "+15550001111", `{"base64_avatar":"` + avatar + `"}`, 204, []string{"--avatar", "<file>"}},
		{"remove avatar", "+15550001111", `{"remove_avatar":true}`, 204, []string{"--remove-avatar"}},
		{"aci account", "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE", `{"name":"Example Bot"}`, 204, []string{"--given-name", "Example Bot"}},
		{"invalid account", "--evil-flag", `{"name":"Example Bot"}`, 400, nil},
		{"no field", "+15550001111", `{}`, 400, nil},
		{"blank name", "+15550001111", `{"name":"  "}`, 400, nil},
		{"empty avatar", "+15550001111", `{"base64_avatar":""}`, 400, nil},
		{"avatar and removal", "+15550001111", `{"base64_avatar":"` + avatar + `","remove_avatar":true}`, 400, nil},
		{"not json", "+15550001111", `name`, 400, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			os.Remove(argsFile)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPatch, "/v1/profiles/"+tt.account, strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			if recorder.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", recorder.Code, tt.status, recorder.Body.String())
			}
			out, err := os.ReadFile(argsFile)
			if tt.status != 204 {
				if err == nil {
					t.Fatalf("signal-cli ran for a rejected request: %q", out)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
			account := strings.ToLower(tt.account)
			want := append([]string{"--config", root, "-a", account, "updateProfile"}, tt.args...)
			for i, a := range args {
				if i > 0 && args[i-1] == "--avatar" {
					if !strings.HasPrefix(a, root+"/") {
						t.Errorf("avatar path %q outside the avatar directory", a)
					}
					args[i] = "<file>"
				}
			}
			if strings.Join(args, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("signal-cli args %q, want %q", args, want)
			}
		})
	}
}
