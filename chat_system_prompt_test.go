package main

// Invariantien des externalisierten Chat-System-Prompts:
// 1. chat_system.txt (taka-prompts-Paket) ≡ chatSystemPromptBuiltin
//    → sonst würden File-Chat-Instanzen mit/ohne Paket unterschiedliche Prompts fahren.
// 2. renderChatSystemPrompt löst {{folder}}/{{tools}} auf (Folder- und File-Chat).
// 3. Leerer systemPrompt → Built-in-Fallback.

import (
	"os"
	"strings"
	"testing"
)

func TestChatSystemPromptFileMatchesBuiltin(t *testing.T) {
	data, err := os.ReadFile("chat_system.txt")
	if err != nil {
		t.Fatalf("chat_system.txt not readable: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != chatSystemPromptBuiltin {
		t.Errorf("chat_system.txt differs from chatSystemPromptBuiltin")
	}
}

func TestRenderChatSystemPrompt(t *testing.T) {
	folder := "Finanzrechnung"
	searchTools := "Bei großen Ordnern: erst mit search_item suchen. "

	// Folder-Chat: beide Platzhalter gefüllt
	srv := &Server{cfg: Config{}}
	srv.cfg.Chat.systemPrompt = chatSystemPromptBuiltin
	got := renderChatSystemPrompt(srv, folder, searchTools)
	if strings.Contains(got, "{{") {
		t.Errorf("unresolved placeholder in rendered prompt")
	}
	if !strings.Contains(got, "„Finanzrechnung“ arbeitet") {
		t.Errorf("folder placeholder not substituted")
	}
	if !strings.Contains(got, "Bei großen Ordnern") {
		t.Errorf("tools placeholder not substituted")
	}

	// File-Chat: tools leer → kein leerer Satzbruch
	srv.cfg.Chat.systemPrompt = chatSystemPromptBuiltin
	got = renderChatSystemPrompt(srv, folder, "")
	if strings.Contains(got, "{{") {
		t.Errorf("unresolved placeholder in rendered prompt (file chat)")
	}
	if !strings.Contains(strings.Join(strings.Fields(got), " "), "lesen. Pfade sind immer relativ") {
		t.Errorf("file-chat render does not flow from tools-slot to next sentence")
	}

	// Fallback: systemPrompt nicht geladen → Built-in
	srv = &Server{cfg: Config{}}
	got = renderChatSystemPrompt(srv, folder, searchTools)
	if strings.Contains(got, "{{") {
		t.Errorf("fallback render has unresolved placeholder")
	}
}
