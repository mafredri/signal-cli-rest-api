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

func TestSendV2TextStyles(t *testing.T) {
	root := t.TempDir()
	argsFile, stdinFile := filepath.Join(root, "args"), filepath.Join(root, "stdin")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"" + argsFile + "\"\ncat > \"" + stdinFile + "\"\n" +
		"printf '{\"timestamp\":1,\"results\":[]}'\n"
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
	router.POST("/v2/send", NewApi(sc).SendV2)

	for _, tt := range []struct {
		name, defaultMode, body string
		status                  int
	}{
		// The ranges describe the text as given, so the server's default
		// styled mode must not parse it.
		{"default styled mode", "styled", `{"number":"+15550000001","recipients":["+15550000002"],"message":"**hi**","text_styles":[{"start":0,"length":6,"style":"BOLD"}]}`, 201},
		// An empty list still means the text is given without markup.
		{"default styled mode, no ranges", "styled", `{"number":"+15550000001","recipients":["+15550000002"],"message":"**hi**","text_styles":[]}`, 201},
		{"explicit styled mode, no ranges", "", `{"number":"+15550000001","recipients":["+15550000002"],"message":"**hi**","text_mode":"styled","text_styles":[]}`, 400},
		{"explicit styled mode", "", `{"number":"+15550000001","recipients":["+15550000002"],"message":"**hi**","text_mode":"styled","text_styles":[{"start":0,"length":6,"style":"BOLD"}]}`, 400},
		{"range past the message", "", `{"number":"+15550000001","recipients":["+15550000002"],"message":"**hi**","text_styles":[{"start":0,"length":7,"style":"BOLD"}]}`, 400},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DEFAULT_SIGNAL_TEXT_MODE", tt.defaultMode)
			os.Remove(argsFile)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v2/send", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			if recorder.Code != tt.status {
				t.Fatalf("status %d, want %d: %s", recorder.Code, tt.status, recorder.Body.String())
			}
			if tt.status != 201 {
				return
			}
			args, err := os.ReadFile(argsFile)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(tt.body, `"text_styles":[]`) {
				if strings.Contains(string(args), "--text-style") {
					t.Errorf("signal-cli args %q style a request without ranges", args)
				}
			} else if !strings.Contains(string(args), "\n--text-style\n0:6:BOLD\n") {
				t.Errorf("signal-cli args %q lack --text-style 0:6:BOLD", args)
			}
			if stdin, _ := os.ReadFile(stdinFile); string(stdin) != "**hi**" {
				t.Errorf("message %q, want the text as given", stdin)
			}
		})
	}
}
