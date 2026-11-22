package client

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	ds "github.com/bbernhard/signal-cli-rest-api/datastructs"
)

func TestTextStyleArgs(t *testing.T) {
	// "😀" is two UTF-16 code units, so "a😀b" is four.
	const message = "a😀b"
	for _, tt := range []struct {
		name   string
		styles []ds.MessageTextStyle
		want   []string
		err    string
	}{
		{"none", nil, []string{}, ""},
		{"whole message", []ds.MessageTextStyle{{Start: 0, Length: 4, Style: "BOLD"}}, []string{"0:4:BOLD"}, ""},
		{"after a surrogate pair", []ds.MessageTextStyle{{Start: 3, Length: 1, Style: "ITALIC"}}, []string{"3:1:ITALIC"}, ""},
		{"nested and adjacent", []ds.MessageTextStyle{{Start: 0, Length: 4, Style: "BOLD"}, {Start: 1, Length: 2, Style: "ITALIC"}, {Start: 3, Length: 1, Style: "SPOILER"}}, []string{"0:4:BOLD", "1:2:ITALIC", "3:1:SPOILER"}, ""},
		{"every style", []ds.MessageTextStyle{{Start: 0, Length: 1, Style: "STRIKETHROUGH"}, {Start: 0, Length: 1, Style: "MONOSPACE"}}, []string{"0:1:STRIKETHROUGH", "0:1:MONOSPACE"}, ""},
		{"past the end", []ds.MessageTextStyle{{Start: 2, Length: 3, Style: "BOLD"}}, nil, "outside the message"},
		{"starts at the end", []ds.MessageTextStyle{{Start: 4, Length: 1, Style: "BOLD"}}, nil, "outside the message"},
		{"overflowing length", []ds.MessageTextStyle{{Start: 1, Length: 1<<63 - 1, Style: "BOLD"}}, nil, "outside the message"},
		{"negative start", []ds.MessageTextStyle{{Start: -1, Length: 1, Style: "BOLD"}}, nil, "outside the message"},
		{"empty range", []ds.MessageTextStyle{{Start: 0, Length: 0, Style: "BOLD"}}, nil, "outside the message"},
		{"lowercase style", []ds.MessageTextStyle{{Start: 0, Length: 1, Style: "bold"}}, nil, "invalid text style"},
		{"no style", []ds.MessageTextStyle{{Start: 0, Length: 1}}, nil, "invalid text style"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := textStyleArgs(message, tt.styles)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("error %v, want %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// The message keeps its markers and the ranges reach signal-cli as given.
func TestSendV2PassesTextStyles(t *testing.T) {
	f := newFakeSignalCli()
	f.answer = func(fakeRequest) (any, *Error) {
		return map[string]any{"timestamp": 1, "results": []any{}}, nil
	}
	const message = "**not markup** 😀"
	styles := []ds.MessageTextStyle{{Start: 0, Length: 14, Style: "BOLD"}, {Start: 2, Length: 3, Style: "ITALIC"}}
	_, err := newFakeJsonRpcSignalClient(t, f).SendV2("+15550000001", message, []string{"+15550000002"}, nil, "", nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, styles)
	if err != nil {
		t.Fatal(err)
	}
	reqs := f.Requests()
	if len(reqs) != 1 || reqs[0].Method != "send" {
		t.Fatalf("requests %+v, want one send", reqs)
	}
	var params struct {
		Message   string   `json:"message"`
		TextStyle []string `json:"text-style"`
	}
	if err := json.Unmarshal(reqs[0].Params, &params); err != nil {
		t.Fatal(err)
	}
	if params.Message != message {
		t.Errorf("message %q, want %q", params.Message, message)
	}
	if want := []string{"0:14:BOLD", "2:3:ITALIC"}; !reflect.DeepEqual(params.TextStyle, want) {
		t.Errorf("text-style %q, want %q", params.TextStyle, want)
	}
}

func TestSendV2RejectsTextStylesWithTheStyledMode(t *testing.T) {
	f := newFakeSignalCli()
	styled := "styled"
	_, err := newFakeJsonRpcSignalClient(t, f).SendV2("+15550000001", "**hi**", []string{"+15550000002"}, nil, "", nil,
		nil, nil, nil, nil, &styled, nil, nil, nil, nil, []ds.MessageTextStyle{{Start: 0, Length: 2, Style: "BOLD"}})
	if err == nil {
		t.Fatal("SendV2 accepted text_styles with the styled text_mode")
	}
	if n := len(f.Requests()); n != 0 {
		t.Fatalf("sent %d requests", n)
	}
}

func TestAboutListsTextStyles(t *testing.T) {
	s := NewSignalClient("", "", "", JsonRpc, "", "", "", OnStart)
	caps := s.About().Capabilities["v2/send"]
	for _, c := range caps {
		if c == "text_styles" {
			return
		}
	}
	t.Fatalf("v2/send capabilities %q lack text_styles", caps)
}
