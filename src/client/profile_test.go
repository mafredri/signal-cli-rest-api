package client

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// A 1x1 PNG.
const testAvatar = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6x2kAAAAASUVORK5CYII="

func TestPatchProfileSendsOnlyGivenFields(t *testing.T) {
	name, about, empty, avatar := "Example Bot", "Hello", "", testAvatar
	for _, tt := range []struct {
		name         string
		profileName  *string
		avatar       *string
		removeAvatar bool
		about        *string
		want         map[string]any
	}{
		{"name", &name, nil, false, nil, map[string]any{"given-name": name}},
		{"about", nil, nil, false, &about, map[string]any{"about": about}},
		{"clear about", nil, nil, false, &empty, map[string]any{"about": ""}},
		{"remove avatar", nil, nil, true, nil, map[string]any{"remove-avatar": true}},
		{"avatar", nil, &avatar, false, nil, map[string]any{"avatar": "<file>"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := patchProfileParams(t, func(s *SignalClient) error {
				return s.PatchProfile("+15550001111", tt.profileName, tt.avatar, tt.removeAvatar, tt.about)
			})
			if !reflect.DeepEqual(params, tt.want) {
				t.Fatalf("updateProfile params %v, want %v", params, tt.want)
			}
		})
	}
}

// PUT keeps its behavior: a request without an avatar removes it.
func TestUpdateProfileSetsNameAndReplacesAvatar(t *testing.T) {
	name := "Example Bot"
	params := patchProfileParams(t, func(s *SignalClient) error {
		return s.UpdateProfile("+15550001111", name, "", nil)
	})
	if want := map[string]any{"given-name": name, "remove-avatar": true}; !reflect.DeepEqual(params, want) {
		t.Fatalf("without avatar: params %v, want %v", params, want)
	}
	params = patchProfileParams(t, func(s *SignalClient) error {
		return s.UpdateProfile("+15550001111", name, testAvatar, nil)
	})
	if want := map[string]any{"given-name": name, "avatar": "<file>"}; !reflect.DeepEqual(params, want) {
		t.Fatalf("with avatar: params %v, want %v", params, want)
	}
}

func TestPatchProfileRejectsSettingAndRemovingTheAvatar(t *testing.T) {
	f := newFakeSignalCli()
	avatar := testAvatar
	if err := newFakeJsonRpcSignalClient(t, f).PatchProfile("+15550001111", nil, &avatar, true, nil); err == nil {
		t.Fatal("PatchProfile accepted an avatar together with remove_avatar")
	}
	if n := len(f.Requests()); n != 0 {
		t.Fatalf("sent %d requests", n)
	}
}

// patchProfileParams runs call against a fake signal-cli and returns the
// params of the single updateProfile request without the account. An avatar
// path is replaced by "<file>" after checking that the file existed while
// signal-cli could read it and was removed afterwards.
func patchProfileParams(t *testing.T, call func(*SignalClient) error) map[string]any {
	t.Helper()
	f := newFakeSignalCli()
	var avatarPath string
	f.answer = func(req fakeRequest) (any, *Error) {
		var p struct {
			Avatar string `json:"avatar"`
		}
		json.Unmarshal(req.Params, &p)
		if p.Avatar != "" {
			if _, err := os.Stat(p.Avatar); err != nil {
				t.Errorf("avatar file missing during updateProfile: %v", err)
			}
			avatarPath = p.Avatar
		}
		return map[string]any{}, nil
	}
	if err := call(newFakeJsonRpcSignalClient(t, f)); err != nil {
		t.Fatal(err)
	}
	reqs := f.Requests()
	if len(reqs) != 1 || reqs[0].Method != "updateProfile" {
		t.Fatalf("requests %+v, want one updateProfile", reqs)
	}
	var params map[string]any
	if err := json.Unmarshal(reqs[0].Params, &params); err != nil {
		t.Fatal(err)
	}
	if params["account"] != "+15550001111" {
		t.Errorf("account %v", params["account"])
	}
	delete(params, "account")
	if avatarPath != "" {
		if _, err := os.Stat(avatarPath); !os.IsNotExist(err) {
			t.Errorf("avatar file %s left behind (%v)", avatarPath, err)
		}
		params["avatar"] = "<file>"
	}
	return params
}
