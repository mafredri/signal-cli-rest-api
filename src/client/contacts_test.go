package client

import (
	"testing"
)

// signal-cli's listContacts JSON (JsonContact) carries the profile's names
// only inside "profile".
func TestListContactsProfileName(t *testing.T) {
	f := newFakeSignalCli()
	f.answer = func(req fakeRequest) (any, *Error) {
		return []map[string]any{
			{"number": "+15550000001", "profile": map[string]any{"givenName": "Ada", "familyName": "Lovelace"}},
			{"number": "+15550000002", "profile": map[string]any{"givenName": "Ada"}},
			{"number": "+15550000003", "profile": map[string]any{"familyName": "Lovelace"}},
			{"number": "+15550000004", "profile": nil},
			{"number": "+15550000005", "profile": map[string]any{"givenName": " ", "familyName": "Lovelace"}},
			{"number": "+15550000006", "profile": map[string]any{"givenName": "Ada", "familyName": "\t"}},
			{"number": "+15550000007", "profile": map[string]any{"givenName": " ", "familyName": "\t"}},
			{"number": "+15550000008", "profile": map[string]any{"givenName": " Ada ", "familyName": "Lovelace"}},
		}, nil
	}
	contacts, err := newFakeJsonRpcSignalClient(t, f).ListContacts("+15550009999", false, "")
	if err != nil {
		t.Fatal(err)
	}
	// Blank names are left out, other names are kept as given.
	want := []string{"Ada Lovelace", "Ada", "Lovelace", "", "Lovelace", "Ada", "", " Ada  Lovelace"}
	if len(contacts) != len(want) {
		t.Fatalf("got %d contacts, want %d", len(contacts), len(want))
	}
	for i, w := range want {
		if contacts[i].ProfileName != w {
			t.Errorf("contact %s: profile_name %q, want %q", contacts[i].Number, contacts[i].ProfileName, w)
		}
	}
}
