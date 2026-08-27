package main

// Invariantien der Write-Tools (Blank-Chat):
// 1. safeWritePath normalisiert + begrenzt die Tiefe.
// 2. '..' wird blockiert.
// 3. Leerer Input = die Root selbst.

import (
	"testing"
)

func TestSafeWritePath(t *testing.T) {
	const root = "workspace"
	const maxDepth = 3

	cases := []struct {
		rel    string
		want   string
		wantOK bool
	}{
		{rel: "", want: "workspace", wantOK: true},
		{rel: "projekt", want: "workspace/projekt", wantOK: true},
		{rel: "projekt/datei.html", want: "workspace/projekt/datei.html", wantOK: true},
		{rel: "/projekt/datei.html", want: "workspace/projekt/datei.html", wantOK: true},
		{rel: "a/b/c/d", want: "", wantOK: false}, // tiefer als maxDepth
		{rel: "a/b/c", want: "workspace/a/b/c", wantOK: true},
		{rel: "../etc/passwd", want: "", wantOK: false},
		{rel: "projekt/../../x", want: "", wantOK: false},
	}
	for _, tc := range cases {
		got, err := safeWritePath(root, tc.rel, maxDepth)
		if (err == nil) != tc.wantOK {
			t.Errorf("safeWritePath(%q) error = %v, wantOK = %v", tc.rel, err, tc.wantOK)
			continue
		}
		if tc.wantOK && got != tc.want {
			t.Errorf("safeWritePath(%q) = %q, want %q", tc.rel, got, tc.want)
		}
	}
}

func TestChatWriteTools(t *testing.T) {
	tools := chatWriteTools()
	if len(tools) != 3 {
		t.Fatalf("chatWriteTools() returned %d tools, want 3", len(tools))
	}
	names := map[string]bool{}
	for _, t := range tools {
		names[t.Function.Name] = true
	}
	for _, name := range []string{"write_file", "mkdir", "rmdir"} {
		if !names[name] {
			t.Errorf("chatWriteTools() missing %s", name)
		}
	}
}
