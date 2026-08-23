package main

import (
	"testing"
)

func TestStripScopePrefix(t *testing.T) {
	cases := []struct {
		name, hitPath, scopeFolder, want string
	}{
		{"nested", "Projekte/Rechnungen/fakt.pdf", "Projekte/Rechnungen", "fakt.pdf"},
		{"scope-self", "Projekte/Rechnungen", "Projekte/Rechnungen", ""},
		{"slashed-scope", "Projekte/Rechnungen/sub/x.txt", "/Projekte/Rechnungen/", "sub/x.txt"},
		{"empty-scope", "anything/x.txt", "", "anything/x.txt"},
		{"dot-scope", "anything/x.txt", ".", "anything/x.txt"},
		{"root-scope", "anything/x.txt", "/", "anything/x.txt"},
		{"outside-scope", "other/file.txt", "Projekte", "other/file.txt"},
		{"no-prefix-bleed", "ProjekteX/f.txt", "Projekte", "ProjekteX/f.txt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripScopePrefix(tc.hitPath, tc.scopeFolder); got != tc.want {
				t.Errorf("stripScopePrefix(%q, %q) = %q, want %q", tc.hitPath, tc.scopeFolder, got, tc.want)
			}
		})
	}
}

func TestEscapeBlevePattern(t *testing.T) {
	cases := []struct{ in, want string }{
		{"einfach", "einfach"},
		{`Anführungs " innen`, `Anführungs \" innen`},
		{`Backslash \ Ende`, `Backslash \\ Ende`},
		{`beide " \ zusammen`, `beide \" \\ zusammen`},
	}
	for _, tc := range cases {
		if got := escapeBlevePattern(tc.in); got != tc.want {
			t.Errorf("escapeBlevePattern(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParseSearchResponse prüft die Zerlegung einer 207-Antwort im
// exakten Format des webdav-Such-Handlers (Href via FormatReference,
// collection als Kind-Element, RFC1123-Datum, getcontentlength/size).
func TestParseSearchResponse(t *testing.T) {
	const xmlBody = `<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:response>
    <d:href>/dav/spaces/f7e671d7-36e5-493f-b0c7-ffe5ee4319a5$6bd275c4-1111-2222-3333-444444444444/Projekte/Rechnungen</d:href>
    <d:propstat>
      <d:prop>
        <oc:name>Rechnungen</oc:name>
        <d:resourcetype><d:collection/></d:resourcetype>
        <d:getlastmodified>Sat, 22 Aug 2026 10:00:00 GMT</d:getlastmodified>
        <oc:size>0</oc:size>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/dav/spaces/f7e671d7-36e5-493f-b0c7-ffe5ee4319a5$6bd275c4-1111-2222-3333-444444444444/Projekte%20Neu/Rechnung%202026.pdf</d:href>
    <d:propstat>
      <d:prop>
        <oc:name>Rechnung 2026.pdf</oc:name>
        <d:resourcetype/>
        <d:getlastmodified>Sat, 22 Aug 2026 11:30:00 GMT</d:getlastmodified>
        <d:getcontentlength>12345</d:getcontentlength>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/dav/etwas-anderes/foo</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype/>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>
</d:multistatus>`

	hits := parseSearchResponse([]byte(xmlBody))
	if len(hits) != 2 {
		t.Fatalf("want 2 Hits (fremdes Href wird ignoriert), got %d", len(hits))
	}

	dir, file := hits[0], hits[1]
	wantSpace := "f7e671d7-36e5-493f-b0c7-ffe5ee4319a5$6bd275c4-1111-2222-3333-444444444444"
	if !dir.IsDir {
		t.Error("erster Treffer soll Verzeichnis sein (d:collection)")
	}
	if dir.SpaceID != wantSpace {
		t.Errorf("SpaceID = %q, want %q", dir.SpaceID, wantSpace)
	}
	if dir.Path != "Projekte/Rechnungen" {
		t.Errorf("dir.Path = %q, want Projekte/Rechnungen", dir.Path)
	}
	if dir.MTime != "2026-08-22" {
		t.Errorf("dir.MTime = %q, want 2026-08-22", dir.MTime)
	}

	if file.IsDir {
		t.Error("zweiter Treffer soll Datei sein (leeres resourcetype)")
	}
	if file.Path != "Projekte Neu/Rechnung 2026.pdf" {
		t.Errorf("file.Path = %q, want %q (URL-Deescaped)", file.Path, "Projekte Neu/Rechnung 2026.pdf")
	}
	if file.Size != 12345 {
		t.Errorf("file.Size = %d, want 12345", file.Size)
	}
	if file.MTime != "2026-08-22" {
		t.Errorf("file.MTime = %q, want 2026-08-22", file.MTime)
	}
}

func TestChatConfigApplyDefaultsSearchAndHeartbeat(t *testing.T) {
	var cfg Config
	cfg.LLM.Model = "test-model"
	var chat ChatConfig
	chat.Search.MaxResults = 500 // über Cap → 100
	chat.Heartbeat.IntervalSeconds = 1 // unter Minimum → 20
	chat.applyDefaults(&cfg)

	if chat.Search.WebDavURL != "http://127.0.0.1:9115" {
		t.Errorf("Search.WebDavURL = %q, want Default", chat.Search.WebDavURL)
	}
	if chat.Search.MaxResults != 100 {
		t.Errorf("Search.MaxResults = %d, want Cap 100", chat.Search.MaxResults)
	}
	if chat.Heartbeat.IntervalSeconds != 20 {
		t.Errorf("Heartbeat.IntervalSeconds = %d, want Default 20", chat.Heartbeat.IntervalSeconds)
	}
	if chat.MaxIterations != 8 || chat.DefaultModel != "test-model" {
		t.Error("bestehende Defaults durchnandergekommen")
	}

	// explizite Werte bleiben erhalten
	var chat2 ChatConfig
	chat2.Search.WebDavURL = "http://127.0.0.1:9999"
	chat2.Search.MaxResults = 50
	chat2.Heartbeat.IntervalSeconds = 30
	chat2.applyDefaults(&cfg)
	if chat2.Search.WebDavURL != "http://127.0.0.1:9999" || chat2.Search.MaxResults != 50 || chat2.Heartbeat.IntervalSeconds != 30 {
		t.Error("explizite Config-Werte wurden überschrieben")
	}
}
