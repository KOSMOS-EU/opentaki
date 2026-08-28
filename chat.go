package main

// chat.go — Ordner-Chat via Tool-Loop (Phase 1, ohne Streaming)
//
// Das Modell beschafft sich den Dokumenteninhalt selbst über Tools:
//
//	List(path)           → WebDAV PROPFIND depth=1, rekursiv (BFS, begrenzt:
//	                       list_depth Ebenen, max_listings Einträge gesamt)
//	Read(path)           → WebDAV GET + s.extract (pdf/office/svg/text/…)
//	                       oder VLM-Beschreibung bei Bildern
//
// Zugriffsmodell: Taki bekommt KEIN User-Token. Der Client (Web-Extension)
// legt einen Public-Link-Share (view, password, 1h) auf den Ordner an —
// derselbe Mechanismus wie die Jobs/Pipelines (origin_url). Der Share-Token
// begrenzt den Zugriff strukturell auf den Ordner-Teilbaum (OpenCloud erzwung,
// nicht Taki-App-Logik). Taki hält Token/Password nur in-memory pro Request
// und LOGGT sie nie.

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── Config ───────────────────────────────────────────────────

// ChatConfig steuert den Ordner-Chat (yaml: chat:)
type ChatConfig struct {
	MaxIterations int    `yaml:"max_iterations"` // default 8
	DefaultModel  string `yaml:"default_model"`  // default: llm.model
	MaxFileChars  int    `yaml:"max_file_chars"` // default 60000
	MaxListings   int    `yaml:"max_listings"`   // max. Einträge im rekursiven Listing, default 100
	ListDepth     int    `yaml:"list_depth"`     // max. Verzeichnisebenen im rekursiven Listing, default 3
	// SystemPromptFile: externes System-Prompt-Template mit den
	// Platzhaltern {{folder}} und {{tools}}. Default:
	// <configDir>/prompts/chat_system.txt (taka-prompts-Paket).
	// Datei fehlt → Built-in-Template (chatSystemPromptBuiltin).
	SystemPromptFile string `yaml:"system_prompt_file"`
	systemPrompt     string // runtime: geladenes Template (leer = noch nicht geladen)
	// BlankSystemPromptFile: externes Template für den Blank-Chat mit den
	// Platzhaltern {{root}}, {{tools_write}} und {{options_rule}}.
	// Default: <configDir>/prompts/chat_system_blank.txt (taka-prompts-Paket).
	// Datei fehlt → Built-in-Template (chatSystemPromptBlankBuiltin).
	BlankSystemPromptFile string `yaml:"blank_system_prompt_file"`
	blankSystemPrompt     string // runtime (leer = noch nicht geladen)
	// EditableExtensions: Dateierweiterungen (ohne Punkt) die im Create-Mode
	// (Blank-Chat) editierbar sind. Für diese Typen liefern Read und Edit
	// den rohen Content.
	// Default: html xhtml css js json md txt yaml yml xml svg ts vue py go sh
	EditableExtensions []string `yaml:"editable_extensions"`
	Search            ChatSearchConfig    `yaml:"search"`
	Heartbeat         ChatHeartbeatConfig `yaml:"heartbeat"`
	ChatToken         ChatTokenConfig     `yaml:"chat_token"`
	Write             ChatWriteConfig     `yaml:"write"`
	// runtime: lowercase-Set aus EditableExtensions
	editableSet map[string]bool
}

// ChatWriteConfig steuert die Write-Tools des Blank-Chats (yaml: chat.write:).
// Nur Requests mit context.write erhalten die Tools — der Personal-Space-Gate
// liegt auf der Extension (createFileHandler + Panel-visibility).
type ChatWriteConfig struct {
	MaxFileBytes int `yaml:"max_file_bytes"` // default 1 MB
	MaxDepth     int `yaml:"max_depth"`      // default 3 (Verzeichnisebenen unter der Root)
}

// chatSystemPromptBuiltin ist das Fallback-Template für den Chat-System-Prompt,
// falls kein externes chat_system.txt geladen wurde. Es muss inhaltlich mit
// der chat_system.txt im taka-prompts-Paket übereinstimmen.
const chatSystemPromptBuiltin = `Du bist ein Assistent, der mit dem Inhalt des Cloud-Ordners „{{folder}}“ arbeitet.
Du kannst dessen Inhalte mit den Tools List (Verzeichnisinhalt), Meta (KI-Metadaten ohne Dateilesen), Read (Dateitext, auch PDF/Office/SVG/Bilder) lesen.
{{tools}}Pfade sind immer relativ zum Ordner (leerer Pfad = der Ordner selbst).
Beantworte auf Basis dessen, was du tatsächlich aus den Dateien erlesen hast.
Lies nur Dateien, die für die Frage relevant sind.
Ist die Frage auf mehrere Entitäten gerichtet (z. B. mehrere Personen oder Unterordner im Listing), beziehe alle davon in der Antwort mit ein – nicht nur die ersten. Wenn das Tool-Budget nicht ausreicht, um alles zu bearbeiten, nenne in der Antwort ausdrücklich, welche Entitäten du nicht ausgewertet hast.
Wenn du mehrere unabhängige Einträge (Listings, Dateien) brauchst, rufe die Tools in einem Schritt parallel auf (mehrere tool_calls pro Antwort), nicht nacheinander in separaten Schritten.
Zählung in den Ergebnissen: Jedes Suchergebnis und jedes Listing endet mit „found: X, limit: Y“. Bei Suchen ist found die Gesamtzahl aller Treffer, bei Listings die Zahl der gezeigten Einträge (limit = max. Einträge). Ist das Ergebnis NICHT als unvollständig markiert, hast du die vollständige Menge — lies dann die relevanten Dateien (Read oder Meta) oder antworte; such nicht endlos weiter. Ist found > limit oder das Ergebnis als unvollständig markiert, iteriere nicht blind weiter — beende deinen Turn mit dem Tool present_options und biete an: (1) konkretisieren (präzisere Begriffe, Dokumenttyp, Zeitraum), (2) limit erhöhen (limit-Parameter der Suche, max. 100) oder (3) alles durchlaufen lassen (kann mehrere Minuten dauern), jeweils als vollständige User-Anweisung. Nenne in der Antwort kurz den gefundenen Umfang und zeige eine Beispiel-Auswahl aus dem, was du schon gesehen hast (max. 10 Einträge), damit der User echte Begriffe und Namen sehen kann.
Wenn der User eine Entscheidung treffen muss (z. B. unklare Vorgabe, mehrere mögliche Wege, uneindeutiger Umfang) — auch dann, wenn du am Antwortende nur eine Ja/Nein- oder andere Rückfrage stellen wolltest — beende den Turn mit dem Tool present_options, NIEMALS mit freier oder nummerierter Frage im Antworttext: 1-5 kurze Optionen, jede eine vollständige User-Anweisung (z. B. „Ja, prüfe alle übrigen Rechnungsdokumente“ / „Nein, der bisherige Umfang reicht“ / „Nur Rechnungen aus 2025 auswerten“). Der User klickt die gewählte Option, und sie erreicht dich als nächste User-Nachricht. Kannst du die Frage direkt beantworten, antworte normal OHNE present_options.
„Alle“ oder „komplett“ in der User-Frage — ebenso Aufträge wie „Übersicht erstellen“ oder „alles auswerten“ — beschreiben das Suchziel (finde alle passenden Treffer), nicht die Genehmigung, eine unvollständige Treffermenge komplett zu durchlaufen. Eine solche Genehmigung zählt nur, wenn der User sie nach Kenntnis der gefundenen Anzahl ausdrücklich erteilt hat (z. B. „ja, alle 1590 durchlaufen“).
Nach 3 Suchversuchen ohne Treffer frage den User ebenfalls, statt weiter zu raten. Wenn der User im Verlauf bereits eine solche Vorgabe gemacht hat, frage nicht erneut.
Filter in der Frage (z. B. Jahr, Lieferant, Betrag) deckst du nur vollständig ab, wenn die passenden Metadaten (doc.date, doc.type, …) bei den Treffern gesetzt sind. Sind solche Metadaten-Suchen leer oder dünn (doc.date ist bei älteren Dateien oft nicht belegt), verenge stillschweigend nicht den Antwortumfang auf das, was du schon hast (z. B. einen Ordner, dessen Name den Filterbegriff trägt): such zusätzlich einfach nach dem Filterbegriff selbst (z. B. nach dem Jahr im Namen). Kannst du Vollständigkeit damit nicht sicherstellen, beziehe die Antwort ausdrücklich auf den Umfang, den du tatsächlich ausgewertet hast, und biete per present_options an, die übrigen Kandidaten durchlaufen zu lassen.
Wenn eine Datei gekürzt wurde (Kürzungs-Hinweis im Tool-Ergebnis), erwähne in der Antwort explizit, dass dieses Dokument nur teilweise ausgewertet wurde.
Wenn du alle nötigen Informationen hast, antworte direkt und strukturiert.`

// renderChatSystemPrompt füllt die Platzhalter des System-Prompt-Templates
// ({{folder}} = Ordnername, {{tools}} = Such-Tool-Beschreibung, leer im File-Chat).
func renderChatSystemPrompt(s *Server, folder, searchTools string) string {
	prompt := s.cfg.Chat.systemPrompt
	if prompt == "" {
		prompt = chatSystemPromptBuiltin
	}
	prompt = strings.ReplaceAll(prompt, "{{folder}}", folder)
	return strings.ReplaceAll(prompt, "{{tools}}", searchTools)
}

// chatSystemPromptBlankBuiltin ist das Fallback-Template für den Blank-Chat
// (Create with Chat, kein geteilter Ordner, Schreiben in das Workspace-
// Verzeichnis des persönlichen Spaces). Es muss inhaltlich mit
// chat_system_blank.txt im taka-prompts-Paket übereinstimmen.
const chatSystemPromptBlankBuiltin = `Du bist ein kreativer Assistent, der Dateien für den User in seinem persönlichen Cloud-Arbeitsbereich „{{root}}“ erstellt.
{{tools_write}}Regeln:
• Bei großen Ordnern: erst mit Search (type="file" für Dateien, type="dir" für Verzeichnisse) suchen, statt alles aufzulisten.
• Lege ZUERST ein Projektverzeichnis mit Mkdir an — der User soll seine Erzeugnisse in klar getrennten Ordner-Projekten finden (Name: kurz beschreibend, kleingeschrieben, Bindestriche, z. B. "url-kurzner" oder "notizen-2026-08").
• Neue Dateien: mit Write erstellen (vollständiger Inhalt). Pro Antwort max. EINE neue Datei — mehr auf Anweisung.
• Bestehende Dateien ändern: VORZUGSWEISE mit Edit (Unified-Diff-Patch, nur die geänderten Zeilen). Write überschreibt die gesamte Datei — nutze es nur für neue Dateien oder wenn der Großteil des Inhalts sich ändert.
• Vor dem Edit einer bestehenden Datei immer zuerst Read aufrufen, um den genauen Zeilenkontext zu sehen.
• Wenn die Datei fragmentiert oder unvollständig wirkt (z. B. abgeschnittene Tags, fehlendes JavaScript), überarbeite sie zu einer vollständigen, funktionierenden Seite.
• Ggf. weitere Dateien im selben Projekt: erst mit present_options anbieten.
• Nenne in der Antwort den vollen Pfad relativ zu {{root}}, z. B. „url-kurzner/app.html“.
• Vor der ersten Änderung an einem existierenden Projekt (Datei/Ordner) den User um Bestätigung bitten (present_options).
{{options_rule}}Wenn der User eine Entscheidung treffen muss (z. B. unklare Vorgabe, mehrere mögliche Varianten) — beende den Turn mit dem Tool present_options, NIEMALS mit freier oder nummerierter Frage im Antworttext: 1-5 kurze Optionen, jede eine vollständige User-Anweisung. Kannst du den Auftrag direkt ausführen, antworte normal OHNE present_options.
Dateiformate für interaktive Inhalte:
• XHTML (Endung .xhtml): direkt in der Cloud als Seite/App/Spiel/Rechner öffnet — ideal für selbstständige, interaktive Anwendungen (Spiele, Taschenrechner, Visualisierungen, Tools).
• HTML (Endung .html): für Internet-/Intranet-Spaces gedacht (externe Links, CDN-Resources, Frameworks).
Beide Formate werden in der Chat-UI als Live-Vorschau angezeigt, wenn der Codeblock mit dem html-Tag (drei Backticks + "html") umflossen ist.
Wenn du fertig bist, antworte kurz und strukturiert (was du wo erstellt hast).`

// renderChatBlankSystemPrompt füllt die Platzhalter des Blank-Chat-
// System-Prompt-Templates ({{root}} = Workspace-Verzeichnis,
// {{tools_write}} = Write-Tool-Beschreibung, {{options_rule}} = leer).
func renderChatBlankSystemPrompt(s *Server, root, writeTools string) string {
	prompt := s.cfg.Chat.blankSystemPrompt
	if prompt == "" {
		prompt = chatSystemPromptBlankBuiltin
	}
	prompt = strings.ReplaceAll(prompt, "{{root}}", root)
	prompt = strings.ReplaceAll(prompt, "{{tools_write}}", writeTools)
	return strings.ReplaceAll(prompt, "{{options_rule}}", "")
}

// ChatSearchConfig steuert die WebDAV-REPORT-Suche (yaml: chat.search:)
type ChatSearchConfig struct {
	WebDavURL  string `yaml:"webdav_url"`  // Default http://127.0.0.1:9115
	MaxResults int    `yaml:"max_results"` // Default 20, Cap 100
}

// ChatHeartbeatConfig steuert den SSE-Heartbeat (yaml: chat.heartbeat:)
type ChatHeartbeatConfig struct {
	IntervalSeconds int `yaml:"interval_seconds"` // Default 20, <5 = aus
}

// ChatTokenConfig steuert das langlebige Chat-Token (yaml: chat.chat_token:).
// Der Proxy-OIDC-Gate akzeptiert nur IDP-Tokens — den vom Proxy geprägten
// 1-Day-User-JWT kann die Extension also nicht als Bearer präsentieren.
// Deshalb wrappt /chat/token (OIDC-gated) den User-JWT in ein HMAC-Token,
// das Taki auf der unprotected /chat-direct-Route selbst validiert.
type ChatTokenConfig struct {
	// Secret: HMAC-SHA256-Secret. Leer = Chat-Token aus (die Extension
	// nutzt dann weiterhin die OIDC-gated /chat/-Route).
	Secret string `yaml:"secret"`
	// TTLHours: maximale Gültigkeit eines Chat-Tokens. Die tatsächliche
	// Expiry ist min(TTL, Expiry des eingebetteten User-JWTs). Default 24.
	TTLHours int `yaml:"ttl_hours"`
}

func (c *ChatConfig) applyDefaults(cfg *Config) {
	if c.MaxIterations < 1 {
		c.MaxIterations = 8
	}
	if c.MaxFileChars < 1 {
		c.MaxFileChars = 60000
	}
	if c.MaxListings < 1 {
		c.MaxListings = 100
	}
	if c.ListDepth < 1 {
		c.ListDepth = 3
	}
	if c.DefaultModel == "" {
		c.DefaultModel = cfg.LLM.Model
	}
	if c.Search.WebDavURL == "" {
		c.Search.WebDavURL = "http://127.0.0.1:9115"
	}
	if c.Search.MaxResults < 1 {
		c.Search.MaxResults = 20
	}
	if c.Search.MaxResults > 100 {
		c.Search.MaxResults = 100
	}
	if c.Heartbeat.IntervalSeconds < 5 {
		c.Heartbeat.IntervalSeconds = 20
	}
	if c.ChatToken.TTLHours < 1 {
		c.ChatToken.TTLHours = 24
	}
	if c.Write.MaxFileBytes < 1 {
		c.Write.MaxFileBytes = 1024 * 1024
	}
	if c.Write.MaxDepth < 1 {
		c.Write.MaxDepth = 3
	}
	// EditableExtensions: Default-List für den Create-Mode
	if len(c.EditableExtensions) == 0 {
		c.EditableExtensions = []string{
			"html", "xhtml", "css", "js", "json", "md", "txt",
			"yaml", "yml", "xml", "svg", "ts", "vue", "py", "go", "sh",
		}
	}
	c.editableSet = make(map[string]bool, len(c.EditableExtensions))
	for _, ext := range c.EditableExtensions {
		c.editableSet[strings.ToLower(ext)] = true
	}
}

// isEditableFile prüft, ob die Dateierweiterung in der editable_extensions
// Config-Liste steht. Datei ohne Extension → false.
func (c *ChatConfig) isEditableFile(path string) bool {
	dot := strings.LastIndex(path, ".")
	if dot < 0 || dot == len(path)-1 {
		return false
	}
	ext := strings.ToLower(path[dot+1:])
	return c.editableSet[ext]
}

// applyUnifiedDiff wendet einen Unified-Diff-Patch auf fileContent an.
// Format: "--- x" / "+++ y" Header, dann Hunks "@@ -oldStart,oldCount +newStart,newCount @@"
// mit Kontextzeilen (Prefix " "), Entfernungen ("-"-Prefix) und Einfügungen ("+"-Prefix).
// Liefert den neuen Content und die Anzahl geänderter Zeilen.
func applyUnifiedDiff(fileContent, patchText string) (string, int, error) {
	// Hunks werden sequentiell angewendet: vor jedem Hunk ist currentLines
	// der aktuelle (bereits teilweise gepatchte) Stand der Datei. Nach dem
	// Hunk wird der unveränderte Rest angehängt → das ergibt den Ausgangs-
	// stand des nächsten Hunks.
	currentLines := strings.Split(fileContent, "\n")
	patchLines := strings.Split(patchText, "\n")

	changedLines := 0
	i := 0 // Index in patchLines

	for i < len(patchLines) {
		line := patchLines[i]

		if !strings.HasPrefix(line, "@@") {
			// Kopfzeilen des Diffs (--- /+++ / diff / index ...) und leere
			// Zeilen außerhalb der Hunks — überspringen.
			if isDiffHeaderLine(line) || line == "" {
				i++
				continue
			}
			return "", 0, fmt.Errorf("unerwartete Patch-Zeile %q", truncateForErr(line))
		}

		// Hunk-Header parsen: @@ -a,b +c,d @@
		oldStart, err := parseHunkHeader(line)
		if err != nil {
			return "", 0, fmt.Errorf("ungültiger Hunk-Header %q: %s", line, err)
		}
		i++

		// Hunk-Körper einlesen, bis eine neue @@-Zeile kommt
		var hunkBody []string // Zeilen mit Prefix: " " Kontext, "-" entfernen, "+" einfügen
		for i < len(patchLines) && !strings.HasPrefix(patchLines[i], "@@") {
			hunkLine := patchLines[i]
			switch {
			case strings.HasPrefix(hunkLine, " ") ||
				strings.HasPrefix(hunkLine, "-") ||
				strings.HasPrefix(hunkLine, "+") ||
				hunkLine == "\\": // No-newline-Marker
				hunkBody = append(hunkBody, hunkLine)
				i++
			case hunkLine == "":
				// Leere Zeile im Hunk = Kontext auf leerer Datei-Zeile
				hunkBody = append(hunkBody, " ")
				i++
			default:
				// Datei-Header o.ä. innerhalb eines Hunks — als Kontext behandeln
				hunkBody = append(hunkBody, hunkLine)
				i++
			}
		}

		// Hunk anwenden: Startposition aus dem Header, bei Abweichung
		// vor/zurück suchen (wie patch(1)), dann Kontext abgleichen.
		oldPos, changed, applyErr := applyHunkAt(currentLines, oldStart-1, hunkBody)
		if applyErr != nil {
			return "", 0, fmt.Errorf("Hunk %q: %s. Tatsächlicher Dateiinhalt:\n%s",
				line, applyErr, actualWindow(currentLines, 0, minInt(len(currentLines), 10)))
		}
		changedLines += changed
		// Ergebnis: vor dem Hunk + Hunk-Ergebnis + Rest der Datei.
		// Der Rest beginnt hinter den verbrauchten alten Zeilen (Kontext+„-“).
		consumedOld := 0
		for _, body := range hunkBody {
			if len(body) > 0 && (body[0] == ' ' || body[0] == '-') {
				consumedOld++
			}
		}
		// prefix und tail kopieren: sie teilen das Backing-Array mit
		// currentLines, und die Appends unten würden sie sonst überschreiben.
		oldPrefix := append([]string(nil), currentLines[:oldPos]...)
		oldTail := append([]string(nil), currentLines[oldPos+consumedOld:]...)
		patched := hunkResult(hunkBody, oldPos, currentLines)
		currentLines = make([]string, 0, len(oldPrefix)+len(patched)+len(oldTail))
		currentLines = append(currentLines, oldPrefix...)
		currentLines = append(currentLines, patched...)
		currentLines = append(currentLines, oldTail...)
	}

	return strings.Join(currentLines, "\n"), changedLines, nil
}

// hunkResult wandelt den Hunk-Körper in die neuen Zeilen ab (Kontext bleibt,
// "-" wird verworfen, "+" übernommen). Der aktuelle Stand (currentLines)
// dient als Quelle für Kontextzeilen.
func hunkResult(hunkBody []string, oldPos int, currentLines []string) []string {
	var out []string
	pos := oldPos
	for _, body := range hunkBody {
		if body == "" {
			body = " "
		}
		switch body[0] {
		case ' ':
			if pos < len(currentLines) {
				out = append(out, currentLines[pos])
			}
			pos++
		case '-':
			pos++
		case '+':
			out = append(out, body[1:])
		case '\\':
			// "\ No newline at end of file" — ignorieren
		}
	}
	return out
}

// applyHunkAt prüft, ob der Hunk bei Position startPos (0-basiert) in
// currentLines passt. Bei Nichtpassgen wird vor/zurück gesucht
// (max. 50 Zeilen, wie patch(1)). Liefert die verwendete Position und die
// Anzahl geänderter Zeilen.
func applyHunkAt(currentLines []string, startPos int, hunkBody []string) (int, int, error) {
	if startPos < 0 {
		startPos = 0
	}

	matchesAt := func(pos int) bool {
		p := pos
		for _, body := range hunkBody {
			if body == "" {
				continue
			}
			switch body[0] {
			case ' ':
				if p >= len(currentLines) || strings.TrimRight(currentLines[p], "\r") != strings.TrimRight(body[1:], "\r") {
					return false
				}
				p++
			case '-':
				if p >= len(currentLines) || strings.TrimRight(currentLines[p], "\r") != strings.TrimRight(body[1:], "\r") {
					return false
				}
				p++
			case '+':
				// Nur Eintrag, kein Abgleich
			case '\\':
			}
		}
		return true
	}

	changed := 0
	for _, body := range hunkBody {
		if len(body) > 0 && (body[0] == '-' || body[0] == '+') {
			changed++
		}
	}

	// Exakte Position zuerst
	if matchesAt(startPos) {
		return startPos, changed, nil
	}
	// Vor/zurück suchen (Fuzz)
	for offset := 1; offset <= 50; offset++ {
		if startPos-offset >= 0 && matchesAt(startPos - offset) {
			return startPos - offset, changed, nil
		}
		if matchesAt(startPos + offset) {
			return startPos + offset, changed, nil
		}
	}
	return 0, 0, fmt.Errorf(
		"Hunk-Kontext wurde in der Datei nicht gefunden (Startzeile %d). Nutze Read für den aktuellen Stand.",
		startPos+1)
}

// ── egrep (Inhaltssuche im Share) ─────────────────────────────

type egrepHit struct {
	path   string
	lineNo int
	line   string
}

// egrepWalk durchsucht Dateien unter rootRel rekursiv mit der Regex.
// Liefert die Treffer, die Anzahl gescannter Dateien und einen Cut-Flag,
// falls die Treffer-Limit erreicht wurde.
func (s *Server) egrepWalk(d *shareWebDav, rootRel string, re *regexp.Regexp, limit int) ([]egrepHit, int, bool) {
	maxDepth := s.cfg.Chat.ListDepth
	entries, _, entryCut, err := d.propfindTree(rootRel, maxDepth, s.cfg.Chat.MaxListings)
	if err != nil {
		return nil, 0, false
	}
	var hits []egrepHit
	filesScanned := 0
	cut := false
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		if len(hits) >= limit {
			cut = true
			break
		}
		data, ct, getErr := d.getfile(e.Rel)
		if getErr != nil {
			continue
		}
		if !isTextyContent(ct, data) {
			continue
		}
		filesScanned++
		lines := strings.Split(string(data), "\n")
		for lineNo, line := range lines {
			if !re.MatchString(line) {
				continue
			}
			hits = append(hits, egrepHit{path: e.Rel, lineNo: lineNo + 1, line: truncateChars(line, 200)})
			if len(hits) >= limit {
				cut = true
				break
			}
		}
	}
	if entryCut {
		cut = true
	}
	return hits, filesScanned, cut
}

// isTextyContent erkennt, ob der Content als Text durchsuchbar ist
// (MIME-Typ + Byte-Prüfe). Binäre Dateien (PDF, Office, Bilder, …)
// werden übersprungen.
func isTextyContent(ct string, data []byte) bool {
	lct := strings.ToLower(ct)
	texty := []string{"text/", "json", "xml", "yaml", "csv", "x-sh", "x-python", "javascript", "typescript"}
	for _, t := range texty {
		if strings.Contains(lct, t) {
			return true
		}
	}
	// Kein erkennbarer MIME-Typ → per Magic-Byte prüfen
	if len(data) < 2 || data[0] != 0x00 {
		// Heuristik: kein Nullbyte in den ersten 512 Bytes = wahrscheinlich Text
		if len(data) > 512 {
			data = data[:512]
		}
		for _, b := range data {
			if b == 0x00 {
				return false
			}
		}
		return true
	}
	return false
}

func relPathOrRoot(relPath string) string {
	if relPath == "" {
		return "dem geteilten Ordner"
	}
	return relPath
}

// isDiffHeaderLine erkennt Kopfzeilen eines Unified/Extended Diffs.
func isDiffHeaderLine(line string) bool {
	return strings.HasPrefix(line, "---") || strings.HasPrefix(line, "+++") ||
		strings.HasPrefix(line, "diff ") || strings.HasPrefix(line, "index ") ||
		strings.HasPrefix(line, "old mode") || strings.HasPrefix(line, "new mode") ||
		strings.HasPrefix(line, "deleted file") || strings.HasPrefix(line, "new file") ||
		strings.HasPrefix(line, "rename from") || strings.HasPrefix(line, "rename to") ||
		strings.HasPrefix(line, "similarity index") || strings.HasPrefix(line, "Binary files")
}

// actualAt liefert die Datei-Zeile an Position (1-basiert) für Fehlermeldungen.
func actualAt(lines []string, pos0 int) string {
	if pos0 < 0 || pos0 >= len(lines) {
		return "<außerhalb der Datei>"
	}
	return truncateForErr(lines[pos0])
}

// actualWindow rendert einen Zeilenbereich der Datei mit Zeilennummern,
// damit das Modell bei einem Fehlschlag den tatsächlichen Inhalt sehen kann.
func actualWindow(lines []string, from0, count int) string {
	if from0 < 0 {
		from0 = 0
	}
	to := from0 + count
	if to > len(lines) {
		to = len(lines)
	}
	if from0 >= len(lines) {
		return "<Datei ist leer oder Zeile existiert nicht>"
	}
	var sb strings.Builder
	for n := from0; n < to; n++ {
		fmt.Fprintf(&sb, "%4d: %s\n", n+1, truncateForErr(lines[n]))
	}
	return strings.TrimRight(sb.String(), "\n")
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// parseHunkHeader liest "@@ -a[,b] +c[,d] @@ ..." und liefert die alte Startzeile (1-basiert).
func parseHunkHeader(header string) (int, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(header, "@@"))
	parts := strings.SplitN(rest, " @@", 2)
	if len(parts) == 0 {
		return 0, fmt.Errorf("kein @@-Abschluss")
	}
	segments := strings.Fields(parts[0])
	if len(segments) < 2 || !strings.HasPrefix(segments[0], "-") || !strings.HasPrefix(segments[1], "+") {
		return 0, fmt.Errorf("erwarte Format '-Start,Count +Start,Count'")
	}
	oldSpec := strings.TrimPrefix(segments[0], "-")
	oldSpec = strings.SplitN(oldSpec, ",", 2)[0] // Start,Count → nur Start
	oldStart, err := strconv.Atoi(oldSpec)
	if err != nil {
		return 0, fmt.Errorf("alte Startzeile %q ist keine Zahl", oldSpec)
	}
	return oldStart, nil
}

func truncateForErr(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 60 {
		return s[:57] + "..."
	}
	if s == "" {
		return "<leer>"
	}
	return s
}

func trailingNewlineTail(content string) string {
	if strings.HasSuffix(content, "\n") {
		return "\n"
	}
	return ""
}

// ── Tool-Definitionen (OpenAI-Format) ────────────────────────

type toolDefinition struct {
	Type     string       `json:"type"` // "function"
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

func (s *Server) chatTools() []toolDefinition {
	return []toolDefinition{
		{Type: "function", Function: toolFunction{
			Name:        "List",
			Description: "Listet den Inhalt eines Verzeichnisses im geteilten Ordner REKURSIV (begrenzt durch Tiefe und Eintragszahl, Kürzungen werden angegeben): Unterverzeichnisse (mit /) und Dateien, jeweils mit relativem Pfad (direkt als Read-Pfad nutzbar), Datum (JJJJ-MM-TT) und bei Dateien der Größe. Liefert KEINE Dateiinhalte. Leerer Pfad = der geteilte Ordner selbst.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad relativ zum geteilten Ordner, leer = Basisordner"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Read",
			Description: "Liest eine Datei im geteilten Ordner. Editierbare Typen (code, html, md, txt, …) liefern raw Content; andere Typen (pdf, office, …) den extrahierten Text. Bilder liefern eine VLM-Beschreibung. Große Dateien werden auf 500 Zeilen gekürzt — der Abschluss-Hinweis nennt die exakte Fortsetzungs-Zeile, mit der du die Datei vollständig in Abschnitten liest. Pfade relativ zum geteilten Ordner.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Datei relativ zum geteilten Ordner"},"offset":{"type":"integer","description":"Optional: erste Zeile (1-basiert). Nur bei editierbaren Texttypen. Default 1."},"limit":{"type":"integer","description":"Optional: Anzahl der Zeilen. Nur bei editierbaren Texttypen. Default 500."}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Meta",
			Description: "Liefert die KI-Metadaten eines Dokuments (Dokumenttyp, Betreff, Datum, Referenz, Absender/Empfänger, Beträge, Tags) OHNE den Dateiinhalt zu laden — zum schnellen Drüberschauen und um zu entscheiden, welche Dateien man mit Read im Detail lesen muss. Liefert Werte nur, wenn die Datei bereits von der KI angereichert wurde; sonst Hinweis. Pfade relativ zum geteilten Ordner.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Datei relativ zum geteilten Ordner"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Search",
			Description: "Sucht nach DATEIEN oder VERZEICHNISSEN im geteilten Ordner (in beliebiger Tiefe), deren Name den Teilstring enthält: liefert relative Pfade (direkt als Read-Pfad nutzbar), Datum und Größe. Liefert KEINE Dateiinhalte. Bei großen Ordnern VORZIEHEN vor List, um gezielt Einträge zu finden. Mit dem optionalen extra-Parameter lassen sich Treffer zusätzlich nach Dateityp, Tag und KI-Metadaten eingrenzen. type: \"file\" (Default) oder \"dir\".",
			Parameters: json.RawMessage(`{"type":"object","properties":{"pattern":{"type":"string","description":"Teilstring des Namens (mind. 2-3 Zeichen, Groß-/Kleinschreibung egal)"},"type":{"type":"string","enum":["file","dir"],"description":"Optional: \"file\" (Default) oder \"dir\"."},"extra":{"type":"string","description":"Optional: zusätzliche Bedingung, wird mit AND verknüpft. Beispiele: \"metadata.doc.type:rechnung\" (KI-Dokumenttyp), \"mediatype:pdf\" (Dateityp), \"metadata.doc.date:*2025*\" (Dokumentenjahr), \"tag:privat\" (Tag). Wildcards OHNE Anführungszeichen. Metadaten-Felder nur bei KI-angereicherten Dateien."},"limit":{"type":"integer","minimum":1,"maximum":100,"description":"Optional: maximale Trefferzahl (Default 20, max 100). Nur bei ausdrücklicher User-Zustimmung auf einen höheren Wert setzen."}},"required":["pattern"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Grep",
			Description: "Durchsucht den INHALT von Dateien im geteilten Ordner (rekursiv) mit einer regulären Expression (POSIX ERE). Liefert die Trefferzeilen mit Pfad und Zeilennummer. Nutze das, um gezielt in vielen Dateien zu finden, statt jede Datei einzeln mit Read zu lesen. Binäre Dateien werden übersprungen.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"pattern":{"type":"string","description":"Reguläre Expression (ERE), z. B. \"Rechnung|Invoice\" oder \"total:[0-9,.]+\""},"path":{"type":"string","description":"Optional: Verzeichnis oder Datei relativ zum geteilten Ordner. Leer = der gesamte geteilte Ordner."},"limit":{"type":"integer","minimum":1,"maximum":200,"description":"Optional: maximale Trefferzeilen (Default 50, max 200)."}},"required":["pattern"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "present_options",
			Description: "Beendet den Turn mit konkreten Antwort-Optionen, die der User in der Chat-UI anklicken kann. NUR verwenden, wenn der User eine Entscheidung treffen soll (z. B. unklare Vorgabe, mehrere mögliche Wege, Umfang-Frage) und du selbst nicht weiterkommst. Max. 5 kurze Optionen, jede für sich eine vollständige User-Anweisung. Wenn du die Frage direkt beantworten kannst, antworte normal OHNE dieses Tool.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"options":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":5,"description":"1-5 kurze Optionen, jede eine vollständige User-Anweisung, z. B. \"Nur Rechnungen aus 2025 auswerten\""}},"required":["options"]}`),
		}},
	}
}

// chatWriteTools: die Write-Tools des Blank-Chats (Write, Mkdir, Rmdir, Edit).
// Nur bei context.write im Request injiziert — der Personal-Space-Gate liegt
// auf der Extension. Pfade sind relativ zur Write-Root.
func chatWriteTools() []toolDefinition {
	return []toolDefinition{
		{Type: "function", Function: toolFunction{
			Name:        "Write",
			Description: "Erstellt eine Datei (oder überschreibt eine bestehende) mit dem vorgegebenen Textinhalt im Arbeitsbereich. Pfad relativ zur Arbeitsbereich-Root (z. B. \"projekt/datei.html\"). Überschreibt vorhandene Dateien — rufe vorher Mkdir auf, wenn das Verzeichnis neu ist. Leerer Content wird abgelehnt.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Datei relativ zur Arbeitsbereich-Root, z. B. \"projekt/datei.html\""},"content":{"type":"string","description":"Vollständiger Dateiinhalt (Unicode-Text), nicht leer"}},"required":["path","content"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Mkdir",
			Description: "Legt ein Verzeichnis im Arbeitsbereich an (Pfad relativ zur Root, z. B. \"projekt\"). Existiert es bereits, ist der Call ein no-op.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad des Verzeichnisses relativ zur Arbeitsbereich-Root"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Rmdir",
			Description: "Entfernt ein LEERES Verzeichnis im Arbeitsbereich (Pfad relativ zur Root, z. B. \"projekt\"). Nicht-leere Verzeichnisse werden NICHT entfernt (Fehlermeldung).",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad des Verzeichnisses relativ zur Arbeitsbereich-Root"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "Edit",
			Description: "Wendet einen Unified-Diff-Patch auf eine Datei im Arbeitsbereich an. Effizienter als Write für kleine Änderungen. Format: Standard Unified Diff mit --- /+++ Header und @@-Hunk-Header. Nutze Read vorher, um den genauen Zeilenkontext zu sehen.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Datei relativ zur Arbeitsbereich-Root"},"patch":{"type":"string","description":"Unified-Diff-Patch (mit --- /+++ und @@ Hunk-Headern)"}},"required":["path","patch"]}`),
		}},
	}
}

// ── LLM-Nachrichten mit Tool-Support ─────────────────────────

type chatToolMessage struct {
	Role       string         `json:"role"`
	Content    *string        `json:"content"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatToolCallFunc `json:"function"`
}

type chatToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON-String
}

type chatToolsRequest struct {
	Model     string            `json:"model"`
	MaxTokens int               `json:"max_tokens"`
	Temp      float64           `json:"temperature"`
	Messages  []chatToolMessage `json:"messages"`
	Tools     []toolDefinition  `json:"tools"`
}

type chatToolsResponse struct {
	Choices []struct {
		FinishReason string          `json:"finish_reason"`
		Message      chatToolMessage `json:"message"`
	} `json:"choices"`
}

func strPtr(s string) *string { return &s }

// llmChatTools führt einen Chat-Completion-Call mit Tools aus.
// Liefert die Assistenten-Antwort (ggf. mit tool_calls) + FinishReason.
func (s *Server) llmChatTools(sessionID, model string, messages []chatToolMessage, tools []toolDefinition) (*chatToolMessage, string, error) {
	s.llmSem <- struct{}{}        // acquire slot
	defer func() { <-s.llmSem }() // release slot
	t := s.llmTrackStart()
	defer s.llmTrackDone(t)

	reqBody := chatToolsRequest{
		Model:     model,
		MaxTokens: s.cfg.LLM.MaxTokens,
		Temp:      0.0,
		Messages:  messages,
		Tools:     tools,
	}
	jsonData, _ := json.Marshal(reqBody)
	u := strings.TrimRight(s.cfg.LLM.APIBase, "/") + "/chat/completions"

	const maxRetries = 3
	backoff := [3]time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := s.client.Post(u, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			log.Printf("chat LLM error [%s] (attempt %d/%d): %v", sessionID, attempt+1, maxRetries, err)
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return nil, "", fmt.Errorf("LLM nicht erreichbar: %v", err)
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var chatResp chatToolsResponse
		if err := json.Unmarshal(respBody, &chatResp); err != nil {
			log.Printf("chat LLM response parse error [%s] (HTTP %d, attempt %d/%d): %v (raw: %.300s)",
				sessionID, resp.StatusCode, attempt+1, maxRetries, err, string(respBody))
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return nil, "", fmt.Errorf("LLM-Antwort nicht lesbar (HTTP %d)", resp.StatusCode)
		}

		if len(chatResp.Choices) > 0 {
			msg := chatResp.Choices[0].Message
			if msg.Content != nil {
				*msg.Content = stripThinkTags(*msg.Content)
			}
			return &msg, chatResp.Choices[0].FinishReason, nil
		}

		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			log.Printf("chat LLM HTTP %d [%s] (attempt %d/%d), retrying in %v",
				resp.StatusCode, sessionID, attempt+1, maxRetries, backoff[attempt])
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return nil, "", fmt.Errorf("LLM-Backend fehlerhaft (HTTP %d)", resp.StatusCode)
		}

		log.Printf("chat LLM empty response [%s] (attempt %d/%d, model=%s, backend=%s, HTTP %d, raw: %.300s)",
			sessionID, attempt+1, maxRetries, model, resp.Header.Get("X-Backend"), resp.StatusCode, string(respBody))
		if attempt < maxRetries-1 {
			time.Sleep(backoff[attempt])
			continue
		}
		return nil, "", fmt.Errorf("LLM-Antwort leer")
	}
	return nil, "", fmt.Errorf("LLM-Antwort leer")
}

// ── WebDAV-Client (ephemeraler Share) ────────────────────────

const maxShareBytes = 200 * 1024 * 1024 // 200MB, deckt sich mit /tika/text

type shareWebDav struct {
	client   *http.Client
	base     string // http://<opencloud>/dav/public-files/<token>
	basePath string // /dav/public-files/<token> (path-only, für Href-Vergleiche)
	token    string // Share-Token (nie loggen)
	passwd   string // Share-Password (nie loggen)
}

func newShareWebDav(s *Server, token, password string) *shareWebDav {
	basePath := "/dav/public-files/" + url.PathEscape(token)
	base := strings.TrimRight(s.cfg.OpenCloud.URL, "/") + basePath
	return &shareWebDav{
		client:   &http.Client{Timeout: 10 * time.Minute},
		base:     base,
		basePath: basePath,
		token:    token,
		passwd:   password,
	}
}

func (d *shareWebDav) urlFor(relPath string) string {
	if relPath == "" {
		return d.base + "/"
	}
	// Segmente explizit escapen (Leerzeichen, ',', '#', '?' … in Dateinamen)
	seg := strings.Split(strings.TrimLeft(relPath, "/"), "/")
	for i, s := range seg {
		seg[i] = url.PathEscape(s)
	}
	return d.base + "/" + strings.Join(seg, "/")
}

func (d *shareWebDav) do(method, u string, depth int, body []byte) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, u, rd)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("public", d.passwd)
	if depth >= 0 {
		req.Header.Set("Depth", fmt.Sprint(depth))
		if body != nil {
			req.Header.Set("Content-Type", "application/xml; charset=utf-8")
		}
	}
	return d.client.Do(req)
}

// safeRelPath normalisiert einen Tool-Pfad (relativ zur Share-Root).
// Defense-in-Depth: das eigentliche Scope-Grenzen liegt beim Share (OpenCloud).
func safeRelPath(p string) (string, error) {
	p = strings.TrimLeft(p, "/")
	if p == "" {
		return "", nil
	}
	if strings.Contains(p, "..") {
		return "", fmt.Errorf("Pfade mit '..' sind nicht erlaubt")
	}
	return path.Clean(p), nil
}

// humanSize rendert eine Byte-Zahl lesbar (0 = "0 B", z.B. auch für leere Dateien).
func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// shareAuthError erkennt Share-Auth-/Scope-Fehler (401 = password/abgelaufen,
// 404 = außerhalb des Share-Scope oder nicht vorhanden).
func shareAuthError(resp *http.Response) (string, bool) {
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "Share-Zugriff verweigert (Password falsch oder Share abgelaufen — bitte Chat neu starten)", true
	}
	if resp.StatusCode == 404 {
		return "Datei/Ordner nicht gefunden. Prüfe den Pfad mit List (Tippfehler?)", true
	}
	return "", false
}

// listingEntry ist ein Eintrag in einem Verzeichnis (Name relativ zum
// gelisteten Ordner, isDir laut resourcetype bzw. Href-Endschiff).
type listingEntry struct {
	Name  string
	IsDir bool
	MTime string // "2006-01-02" (UTC), "" wenn unbekannt
	Size  int64  // Bytes (0 für Verzeichnisse/unbekannt)
}

// propfind listet einen Ordner (Depth: 1). Liefert die Kind-Einträge, ohne
// die Collection selbst.
//
// Wichtig (Reva/OC10-Konvention, RFC 4918 §9.1): Collection-Hrefs ENDEN AUF
// "/". D.h. der letzte Pfad-Segment ist bei Verzeichnissen leer — der Name
// kommt aus dem Href OHNE den abschließenden Slash, das isDir-Signal ist der
// Slash selbst (zusätzlich resourcetype/collection aus der Antwort).
func (d *shareWebDav) propfind(relPath string) ([]listingEntry, error) {
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
    <d:getlastmodified/>
    <d:getcontentlength/>
  </d:prop>
</d:propfind>`)

	resp, err := d.do("PROPFIND", d.urlFor(relPath), 1, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if msg, ok := shareAuthError(resp); ok {
		return nil, fmt.Errorf("%s", msg)
	}
	if resp.StatusCode != 207 {
		return nil, fmt.Errorf("PROPFIND: unerwarteter HTTP %d", resp.StatusCode)
	}

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	var ms struct {
		Responses []struct {
			Href     string `xml:"href"`
			Propstat []struct {
				Prop struct {
					DisplayName  string `xml:"displayname"`
					ResourceType struct {
						Collection *struct{} `xml:"collection"`
					} `xml:"resourcetype"`
					GetLastModified  string `xml:"getlastmodified"`
					GetContentLength string `xml:"getcontentlength"`
				} `xml:"prop"`
			} `xml:"propstat"`
		} `xml:"response"`
	}
	if err := xml.Unmarshal(data, &ms); err != nil {
		return nil, fmt.Errorf("PROPFIND-Antwort nicht lesbar: %v", err)
	}

	// Href der Collection selbst, in der Form wie Reva antwortet:
	// path-only (kein Scheme/Host), escaped, Collection mit trailing "/".
	selfRef := path.Join(d.basePath, strings.TrimLeft(relPath, "/")) + "/"

	var entries []listingEntry
	for _, r := range ms.Responses {
		isDir := strings.HasSuffix(r.Href, "/")

		// Href unescape (Reva: net.EncodePath = url.EscapedPath)
		href := r.Href
		if u, err := url.PathUnescape(href); err == nil {
			href = u
		}
		hrefClean := path.Clean(href)
		if hrefClean == path.Clean(selfRef) {
			continue // die Collection selbst
		}
		name := path.Base(strings.TrimSuffix(href, "/"))
		if name == "" || name == "." {
			continue
		}
		if !isDir {
			// Fallback: resourcetype aus der Antwort (falls kein Href-Slash)
			for _, ps := range r.Propstat {
				if ps.Prop.ResourceType.Collection != nil {
					isDir = true
					break
				}
			}
		}
		// Datum/Größe (Reva: RFC1123 / Dezimal-Bytes; fehlt bei Verzeichnissen)
		var mtimeRaw, sizeRaw string
		for _, ps := range r.Propstat {
			if mtimeRaw == "" {
				mtimeRaw = ps.Prop.GetLastModified
			}
			if sizeRaw == "" {
				sizeRaw = ps.Prop.GetContentLength
			}
		}
		var mdate string
		if t, err := time.Parse(time.RFC1123, mtimeRaw); err == nil {
			mdate = t.UTC().Format("2006-01-02")
		}
		var size int64
		if n, err := strconv.ParseInt(sizeRaw, 10, 64); err == nil {
			size = n
		}
		entries = append(entries, listingEntry{Name: name, IsDir: isDir, MTime: mdate, Size: size})
	}
	return entries, nil
}

// walkEntry ist ein Eintrag im rekursiven Listing (Pfad relativ zur
// Share-Root — direkt als read_file-Pfad verwendbar).
type walkEntry struct {
	Rel   string
	IsDir bool
	MTime string
	Size  int64
}

// propfindTree wandert begrenzten rekursiv (BFS) ab rootRel ab:
//
//   - maxDepth:    Anzahl der Verzeichnisebenen, die UNTERHALB von rootRel
//     gelistet werden (rootRel selbst = Ebene 0).
//   - maxEntries:  Obergrenze für die Gesamtzahl der gelisteten Einträge.
//
// depthCut  = es existieren Verzeichnisse auf der maxDepth-Ebene, deren Inhalt
//             nicht gelistet wurde;
// entryCut  = die Eintragsgrenze (oder die PROPFIND-Sicherungsgrenze) wurde
//             erreicht — es gibt weitere Einträge.
func (d *shareWebDav) propfindTree(rootRel string, maxDepth, maxEntries int) ([]walkEntry, bool, bool, error) {
	const maxCalls = 60 // Sicherung gegen pathologische Verzeichnisbäume
	type qitem struct {
		rel   string
		depth int
	}
	joinRel := func(parent, child string) string {
		if parent == "" {
			return child
		}
		return parent + "/" + child
	}

	var entries []walkEntry
	var depthCut, entryCut bool
	calls := 0
	queue := []qitem{{rel: rootRel, depth: 0}}

	for len(queue) > 0 {
		it := queue[0]
		queue = queue[1:]

		if it.depth >= maxDepth {
			depthCut = true // existiert, wurde aber nicht ausgewertet
			continue
		}
		calls++
		if calls > maxCalls {
			entryCut = true
			break
		}
		kids, err := d.propfind(it.rel)
		if err != nil {
			if it.rel == rootRel {
				return nil, false, false, err
			}
			continue // nicht listbares Unterverzeichnis — überspringen
		}
		for _, k := range kids {
			if len(entries) >= maxEntries {
				entryCut = true
				break
			}
			child := joinRel(it.rel, k.Name)
			entries = append(entries, walkEntry{Rel: child, IsDir: k.IsDir, MTime: k.MTime, Size: k.Size})
			if k.IsDir {
				queue = append(queue, qitem{rel: child, depth: it.depth + 1})
			}
		}
		if entryCut {
			break
		}
	}
	return entries, depthCut, entryCut, nil
}

// propfindMeta holt die Arbitrary-Metadaten (KI-Enrichment) einer Datei:
// PROPFIND Depth 0 mit allprop. Reva liefert alle XAttr-Metadaten als
// <oc:doc.type>, <oc:sender.company>, … (Namespace http://owncloud.org/ns).
// Schema-agnostisch: alles im oc:-Namespace mit Punkt im Namen (doc.*,
// sender.*, amounts.*, libre.graph.*, …) sowie summary/subject/tags.
func (d *shareWebDav) propfindMeta(relPath string) (map[string]string, error) {
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:allprop/></d:propfind>`)

	resp, err := d.do("PROPFIND", d.urlFor(relPath), 0, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if msg, ok := shareAuthError(resp); ok {
		return nil, fmt.Errorf("%s", msg)
	}
	if resp.StatusCode != 207 {
		return nil, fmt.Errorf("PROPFIND: unerwarteter HTTP %d", resp.StatusCode)
	}

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))

	const ocNS = "http://owncloud.org/ns"
	const davNS = "DAV:"
	wantBare := map[string]bool{"summary": true, "subject": true, "tags": true}
	// DAV-Elemente die wir ausserhalb des oc-NS brauchen
	wantDAV := map[string]bool{"getcontentlength": true, "getcontenttype": true}
	meta := map[string]string{}

	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		// oc-NS: doc.* Felder + bare summary/subject/tags
		if start.Name.Space == ocNS {
			if !strings.Contains(start.Name.Local, ".") && !wantBare[start.Name.Local] {
				continue
			}
		} else if start.Name.Space == davNS {
			if !wantDAV[start.Name.Local] {
				continue
			}
		} else {
			continue
		}
		// Elementinhalt lesen (bis zum passenden EndElement)
		var val string
		depth := 1
		for depth > 0 {
			t, err := dec.Token()
			if err != nil {
				depth = 0
				break
			}
			switch tt := t.(type) {
			case xml.StartElement:
				depth++
			case xml.EndElement:
				depth--
			case xml.CharData:
				if depth == 1 {
					val += string(tt)
				}
			}
		}
		if v := strings.TrimSpace(val); v != "" {
			meta[start.Name.Local] = v
		}
	}
	return meta, nil
}

// getfile lädt eine Datei (max. maxShareBytes) und liefert Bytes + Content-Type.
func (d *shareWebDav) getfile(relPath string) ([]byte, string, error) {
	resp, err := d.do("GET", d.urlFor(relPath), -1, nil)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if msg, ok := shareAuthError(resp); ok {
		return nil, "", fmt.Errorf("%s", msg)
	}
	if resp.StatusCode != 200 {
		return nil, "", fmt.Errorf("GET: unerwarteter HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxShareBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(data) > maxShareBytes {
		return nil, "", fmt.Errorf("Datei zu groß (>200MB) für die Auswertung")
	}
	return data, resp.Header.Get("Content-Type"), nil
}

// ── Write-Operationen auf dem Share (Blank-Chat) ─────────────
//
// Die Extension erzeugt einen Public-Link-Share auf /workspace im
// persönlichen Space. Taki nutzt dieselbe shareWebDav-Auth (Basic Auth
// "public:<password>") für PUT/MKCOL/DELETE. Der Share-Scope begrenzt
// die Schreiboperationen strukturell auf den Workspace-Ordner.

func (d *shareWebDav) doWrite(method, uURL string, body io.Reader) error {
	req, err := http.NewRequest(method, uURL, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	req.SetBasicAuth("public", d.passwd)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case method == "MKCOL" && (resp.StatusCode == 201 || resp.StatusCode == 405):
		return nil // 405 = bereits existiert → no-op
	case method == "PUT" && (resp.StatusCode == 201 || resp.StatusCode == 204):
		return nil
	case method == "DELETE" && resp.StatusCode == 204:
		return nil
	default:
		return fmt.Errorf("Share %s: HTTP %d", method, resp.StatusCode)
	}
}

// sharePutFile legt eine Datei im Share-Ordner an oder überschreibt sie.
func (d *shareWebDav) sharePutFile(relPath, content string) error {
	return d.doWrite("PUT", d.urlFor(relPath), strings.NewReader(content))
}

// shareMkdir legt ein Verzeichnis im Share-Ordner an (MKCOL).
func (d *shareWebDav) shareMkdir(relPath string) error {
	return d.doWrite("MKCOL", d.urlFor(relPath), nil)
}

// shareDeletePath entfernt ein leeres Verzeichnis im Share-Ordner.
func (d *shareWebDav) shareDeletePath(relPath string) error {
	return d.doWrite("DELETE", d.urlFor(relPath), nil)
}

// ── WebDAV-Client (User-JWT, Search) ─────────────────────────
//
// Der Proxy (OIDC-gate) prägt pro Request einen User-Reva-JWT und übergibt
// ihn an Taki als x-access-token. Damit kann Taki den webdav-Service DIREKT
// (pod-intern, nicht über den Proxy) für die REPORT-Suche nutzen. Der JWT
// gilt 1 Tag; wird nie geloggt. Der Scope (Resource-ID + Ordner-Pfad) kommt
// vom Client und verengt die serverseitig bereits user-gefilterte Suche —
// er ist KEIN Privilege-Eskalationshebel.

type searchHit struct {
	SpaceID string // <storageid>$<spaceid>[!<opaqueid>]
	Path    string // relativ zur Space-Root
	IsDir   bool
	MTime   string // "2006-01-02" (UTC), "" wenn unbekannt
	Size    int64
}

type userWebDav struct {
	client    *http.Client
	base      string // http://127.0.0.1:9115
	jwt       string // User-Reva-JWT (nie loggen)
	scopeID   string // <storageid>$<spaceid>!<opaqueid>, "" = keine Suche
	scopePath string // geteilter Ordner relativ zur Space-Root, "" = Root
	// Write-Root (Blank-Chat): context.write.root, "" = keine Write-Tools
	writeRoot string
	// writeSpaceID: lazily aufgelöste Space-ID des persönlichen Spaces
	writeSpaceID string
	// Referenz auf den Server für API-Calls (validateSpaceAccess)
	s *Server
}

func newUserWebDav(s *Server, jwt string) *userWebDav {
	return &userWebDav{
		client: &http.Client{Timeout: 30 * time.Second},
		base:   strings.TrimRight(s.cfg.Chat.Search.WebDavURL, "/"),
		jwt:    jwt,
		s:      s,
	}
}

// validateSpaceAccess prüft per OpenCloud Graph API, ob der User auf den
// angegebenen Space zugreifen darf. true = Zugriff erlaubt, false = nicht.
func (u *userWebDav) validateSpaceAccess(spaceID string) bool {
	apiURL := strings.TrimRight(u.s.cfg.OpenCloud.URL, "/") + "/graph/v1.0/drives/" + url.PathEscape(spaceID)
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		log.Printf("chat write: validateSpaceAccess: %v", err)
		return false
	}
	req.Header.Set("Authorization", "Bearer "+u.jwt)
	resp, err := u.client.Do(req)
	if err != nil {
		log.Printf("chat write: validateSpaceAccess: %v", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("chat write: validateSpaceAccess: HTTP %d für spaceID=%s", resp.StatusCode, spaceID)
		return false
	}
	return true
}

// resolvePersonalSpaceID liefert die Space-Resource-ID des persönlichen
// Spaces des Users (<storageid>$<spaceid>!) per PROPFIND auf /dav/spaces.
// Leerer String = kein persönlicher Space auffindbar.
func (u *userWebDav) resolvePersonalSpaceID() string {
	const propfindBody = `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/><oc:drive-type xmlns:oc="http://owncloud.org/ns"/></d:prop></d:propfind>`
	req, err := http.NewRequest("PROPFIND", u.base+"/dav/spaces", strings.NewReader(propfindBody))
	if err != nil {
		return ""
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	req.Header.Set("x-access-token", u.jwt)
	resp, err := u.client.Do(req)
	if err != nil {
		log.Printf("chat write: PROPFIND /dav/spaces: %v", err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		log.Printf("chat write: PROPFIND /dav/spaces: HTTP %d", resp.StatusCode)
		return ""
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return ""
	}
	return spaceIDFromPropfindBody(raw)
}

// spaceIDFromPropfindBody holt die Space-Resource-ID aus einer
// /dav/spaces-PROPFIND-Antwort (d:resourcetype mit <d:collection/> +
// oc:drive-type = "personal", href = /dav/spaces/<id>/).
func spaceIDFromPropfindBody(body []byte) string {
	type propfindRest struct {
		Href  string `xml:"href"`
		Raw   string `xml:",innerxml"`
	}
	type propfindML struct {
		XMLName xml.Name     `xml:"multistatus"`
		Rests   []propfindRest `xml:"response"`
	}
	var ml propfindML
	if err := xml.Unmarshal(body, &ml); err != nil {
		return ""
	}
	for _, rest := range ml.Rests {
		if !strings.Contains(rest.Href, "/dav/spaces/") {
			continue
		}
		// resourcetype mit <d:collection/>: Space (vs. einzelner Resource)
		if !strings.Contains(rest.Raw, "collection") {
			continue
		}
		// oc:drive-type = "personal"
		if !strings.Contains(rest.Raw, "personal") {
			continue
		}
		return path.Base(strings.TrimSuffix(rest.Href, "/"))
	}
	return ""
}

// ── Write-Tools (Blank-Chat) ─────────────────────────────────
//
// Write-Aktionen laufen gegen den persönlichen Space des Users
// (userJWT, PROPFIND-Resolved Space-ID), relativ zu einer von der
// Extension mitgegebenen Root (context.write.root). safeWritePath
// ist die Path-Safety-Schicht: alles außerhalb der Root oder tiefer
// als MaxDepth wird blockiert.

// safeWritePath normalisiert einen relativen Pfad unter root (z. B.
// "projekt/datei.html" → "workspace/projekt/datei.html") und prüft
// MaxDepth. Liefert "" bei leerem Input (die Root selbst).
func safeWritePath(root, relPath string, maxDepth int) (string, error) {
	if relPath == "" {
		return strings.TrimRight(root, "/"), nil
	}
	p := strings.TrimLeft(relPath, "/")
	if strings.Contains(p, "..") {
		return "", fmt.Errorf("Pfade mit '..' sind nicht erlaubt")
	}
	p = path.Clean(p)
	depth := strings.Count(p, "/") + 1
	if depth > maxDepth {
		return "", fmt.Errorf("Pfad zu tief (max. %d Ebenen unter der Root)", maxDepth)
	}
	return strings.TrimRight(root, "/") + "/" + p, nil
}

// spaceURL baut den vollständigen WebDAV-Pfad für ein Resource im
// persönlichen Space (root + relPath → /dav/spaces/<spaceID>/<root>/<rel>).
func (u *userWebDav) spaceURL(spaceID, root, relPath string) string {
	base := strings.TrimRight(u.base, "/")
	root = strings.TrimRight(strings.TrimLeft(root, "/"), "/")
	if relPath == "" {
		return base + "/dav/spaces/" + url.PathEscape(spaceID) + "/" + root + "/"
	}
	seg := strings.Split(strings.TrimLeft(relPath, "/"), "/")
	for i, s := range seg {
		seg[i] = url.PathEscape(s)
	}
	return base + "/dav/spaces/" + url.PathEscape(spaceID) + "/" + root + "/" + strings.Join(seg, "/")
}

// doWrite führt eine WebDAV-Write-Aktion (MKCOL/PUT/DELETE) gegen
// den persönlichen Space aus.
func (u *userWebDav) doWrite(method, uURL string, body io.Reader) error {
	req, err := http.NewRequest(method, uURL, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	req.Header.Set("x-access-token", u.jwt)
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case method == "MKCOL" && (resp.StatusCode == 201 || resp.StatusCode == 405):
		return nil // 405 = bereits existiert → no-op
	case method == "PUT" && (resp.StatusCode == 201 || resp.StatusCode == 204):
		return nil
	case method == "DELETE" && resp.StatusCode == 204:
		return nil
	default:
		return fmt.Errorf("WebDAV %s: HTTP %d", method, resp.StatusCode)
	}
}

// mkdir legt ein Verzeichnis im persönlichen Space an (MKCOL).
// Existiert es bereits → no-op (405).
func (u *userWebDav) mkdir(spaceID, root, relPath string) error {
	return u.doWrite("MKCOL", u.spaceURL(spaceID, root, relPath), nil)
}

// putFile legt eine Datei im persönlichen Space an oder überschreibt
// sie (PUT). content wird als application/octet-stream übertragen.
func (u *userWebDav) putFile(spaceID, root, relPath, content string) error {
	return u.doWrite("PUT", u.spaceURL(spaceID, root, relPath), strings.NewReader(content))
}

// deletePath entfernt ein LEERES Verzeichnis (DELETE, nicht rekursiv).
func (u *userWebDav) deletePath(spaceID, root, relPath string) error {
	return u.doWrite("DELETE", u.spaceURL(spaceID, root, relPath), nil)
}

// escapeBlevePattern escapiert Bleve-Spezialzeichen — das Pattern wird als
// Phrase in Anführungszeichen an Bleve übergeben.
func escapeBlevePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `"`, `\"`)
}

// escapeXML escapiert XML-Reservatezeichen (&, <, >).
func escapeXML(s string) string {
	var sb strings.Builder
	_ = xml.EscapeText(&sb, []byte(s))
	return sb.String()
}

// stripScopePrefix übersetzt einen Treffer-Pfad (relativ zur Space-Root) in
// einen Pfad relativ zum geteilten Ordner (konsistent mit read_file).
func stripScopePrefix(hitPath, scopeFolder string) string {
	prefix := strings.TrimSuffix(strings.TrimPrefix(scopeFolder, "/"), "/")
	if prefix == "" || prefix == "." {
		return hitPath
	}
	if hitPath == prefix {
		return ""
	}
	if strings.HasPrefix(hitPath, prefix+"/") {
		return hitPath[len(prefix)+1:]
	}
	return hitPath // außerhalb des Scope (serverseitig ausgeschlossen)
}

// buildSearchQuery komponiert die KQL-Suchabfrage: Name-Teilstring
// (+ optionaler extra-Filter per AND) + Scope (Resource-ID).
func buildSearchQuery(pattern, extra, scopeID string) string {
	q := `name:"*` + escapeBlevePattern(pattern) + `*"`
	if extra = strings.TrimSpace(extra); extra != "" {
		q += ` AND (` + extra + `)`
	}
	return q + ` scope:` + scopeID
}

// search führt eine WebDAV-REPORT-Suche aus, auf den geteilten Ordner
// beschränkt (scope: Resource-ID). Liefert Treffer (Pfade relativ zur
// Space-Root) + Gesamtzahl (Content-Range, 0 wenn nicht bekannt).
func (u *userWebDav) search(pattern, extra string, limit int) ([]searchHit, int, error) {
	blevePattern := buildSearchQuery(pattern, extra, u.scopeID)
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="utf-8"?>\n<oc:search-files xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">\n  <oc:search>\n    <oc:pattern>%s</oc:pattern>\n    <oc:limit>%d</oc:limit>\n  </oc:search>\n</oc:search-files>`,
		escapeXML(blevePattern), limit)
	req, err := http.NewRequest("REPORT", u.base+"/dav/spaces", strings.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	req.Header.Set("Depth", "0")
	req.Header.Set("x-access-token", u.jwt)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("Suche nicht möglich: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 {
		return nil, 0, fmt.Errorf("Suche nicht möglich (HTTP %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusMultiStatus {
		return nil, 0, fmt.Errorf("Suche: unerwarteter HTTP %d", resp.StatusCode)
	}

	// Gesamtzahl aus Content-Range: rows 0-N/Total (nur bei >0 Treffern)
	total := 0
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if idx := strings.LastIndex(cr, "/"); idx >= 0 && idx+1 < len(cr) {
			if n, err := strconv.Atoi(cr[idx+1:]); err == nil {
				total = n
			}
		}
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, 0, err
	}
	return parseSearchResponse(raw), total, nil
}

type searchResponse struct {
	XMLName xml.Name             `xml:"multistatus"`
	Rests   []searchResponseRest `xml:"response"`
}

type searchResponseRest struct {
	Href  string           `xml:"href"`
	Props []searchPropstat `xml:"propstat"`
}

type searchPropstat struct {
	Prop searchProp `xml:"prop"`
}

type searchProp struct {
	// Pointer statt bool: Go setzt bool-Felder nur aus Zeichendaten,
	// <d:collection/> ist aber ein leeres Element → Präsenz prüfen.
	Collection    *struct{} `xml:"resourcetype>collection"`
	LastModified  string    `xml:"getlastmodified"` // RFC1123
	ContentLength string    `xml:"getcontentlength"`
	DirSize       string    `xml:"size"`
}

// parseSearchResponse zerlegt die 207-MULTISTATUS-Antwort in Treffer.
// Href-Format: /dav/spaces/<storageid>$<spaceid>[!<opaqueid>]/<Space-Root-relativer-Pfad>
func parseSearchResponse(raw []byte) []searchHit {
	var rsp searchResponse
	if err := xml.Unmarshal(raw, &rsp); err != nil {
		log.Printf("chat/search: Antwort nicht parsebar: %v", err)
		return nil
	}
	const hrefPrefix = "/dav/spaces/"
	hits := make([]searchHit, 0, len(rsp.Rests))
	for _, rest := range rsp.Rests {
		if !strings.HasPrefix(rest.Href, hrefPrefix) {
			continue
		}
		restPath := rest.Href[len(hrefPrefix):]
		slash := strings.Index(restPath, "/")
		if slash < 0 {
			continue // Space-Root selbst, kein Eintrag
		}
		hitPath, err := url.PathUnescape(restPath[slash+1:])
		if err != nil {
			continue
		}
		hit := searchHit{
			SpaceID: restPath[:slash],
			Path:    strings.TrimPrefix(hitPath, "/"),
		}
		for _, ps := range rest.Props {
			if ps.Prop.Collection != nil {
				hit.IsDir = true
			}
			if v := ps.Prop.LastModified; v != "" {
				if t, err := http.ParseTime(v); err == nil {
					hit.MTime = t.UTC().Format("2006-01-02")
				}
			}
			if v := ps.Prop.ContentLength; v != "" {
				hit.Size, _ = strconv.ParseInt(v, 10, 64)
			} else if v := ps.Prop.DirSize; v != "" {
				hit.Size, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		hits = append(hits, hit)
	}
	return hits
}

// ── Tool-Ausführung ──────────────────────────────────────────

type toolTrace struct {
	Tool      string `json:"tool"`
	Path      string `json:"path,omitempty"`
	Pattern   string `json:"pattern,omitempty"`
	Extra     string `json:"extra,omitempty"`
	Method    string `json:"method,omitempty"`
	Chars     int    `json:"chars"`
	Truncated bool   `json:"truncated,omitempty"`
	Error     string `json:"error,omitempty"`
	MS        int64  `json:"ms"`
	// FileSize: tatsächliche Dateigröße (PROPFIND getcontentlength).
	// Bei Read: Wahrheit über die Datei, unabhängig vom empfangenen Bruchteil.
	FileSize int64 `json:"file_size,omitempty"`
}

// runChatTool führt einen Tool-Call aus und liefert (tool-Result, Trace).
// u (userWebDav) darf fehlen, wenn der Request kein User-JWT mitschickt —
// die Such-Tools melden dann "Suche nicht verfügbar".
func (s *Server) runChatTool(d *shareWebDav, u *userWebDav, name, argsJSON string) (string, toolTrace) {
	start := time.Now()
	trace := toolTrace{Tool: name}

	var args struct {
		Path    string `json:"path"`
		Pattern string `json:"pattern"`
		Extra   string `json:"extra"`
		Limit   int    `json:"limit"`
		Offset  int    `json:"offset"`
		Content string `json:"content"`
		Type    string `json:"type"`
		Patch   string `json:"patch"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		trace.Error = "ungültige Argumente: " + err.Error()
		trace.MS = time.Since(start).Milliseconds()
		return "Fehler: ungültige Tool-Argumente: " + err.Error(), trace
	}
	isSearch := name == "Search"
	relPath := ""
	if !isSearch {
		cleanPath, err := safeRelPath(args.Path)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		relPath = cleanPath
	}
	trace.Path = relPath

	switch name {
	case "Grep":
		if d == nil {
			trace.Error = "nicht verfügbar"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Grep ist für diesen Chat nicht verfügbar.", trace
		}
		pattern := strings.TrimSpace(args.Pattern)
		if pattern == "" {
			trace.Error = "Pattern erforderlich"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: das Such-Pattern darf nicht leer sein.", trace
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			trace.Error = "ungültiger Regex"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: ungültige reguläre Expression: " + err.Error(), trace
		}
		limit := 50
		if args.Limit >= 1 {
			limit = args.Limit
			if limit > 200 {
				limit = 200
			}
		}
		results, filesScanned, cut := s.egrepWalk(d, relPath, re, limit)
		trace.Pattern = pattern
		trace.Method = "egrep"
		trace.Truncated = cut
		trace.MS = time.Since(start).Milliseconds()
		if len(results) == 0 {
			return fmt.Sprintf("Keine Treffer für /%s/ in %d Datei(en) unter %s.", pattern, filesScanned, relPathOrRoot(relPath)), trace
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Treffer für /%s/:\n", pattern))
		for _, hit := range results {
			sb.WriteString(fmt.Sprintf("%s:%d: %s\n", hit.path, hit.lineNo, hit.line))
		}
		if cut {
			sb.WriteString(fmt.Sprintf("… (auf %d Treffer begrenzt, weitere vorhanden)\n", limit))
		}
		sb.WriteString(fmt.Sprintf("  found: %d Treffer in %d Datei(en) unter %s", len(results), filesScanned, relPathOrRoot(relPath)))
		trace.Chars = len(sb.String())
		return sb.String(), trace

	case "Search":
		if u == nil || u.scopeID == "" {
			trace.Error = "Suche nicht verfügbar"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Suche ist für diesen Ordner nicht verfügbar.", trace
		}
		pattern := strings.TrimSpace(args.Pattern)
		extra := strings.TrimSpace(args.Extra)
		trace.Pattern = pattern
		trace.Extra = extra
		if len(pattern) < 2 {
			trace.Error = "Pattern zu kurz"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Such-Pattern braucht mindestens 2 Zeichen.", trace
		}
		limit := s.cfg.Chat.Search.MaxResults
		if args.Limit >= 1 {
			limit = args.Limit
			if limit > 100 {
				limit = 100
			}
		}
		wantDir := args.Type == "dir"
		hits, total, err := u.search(pattern, extra, limit*3)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		kind := "Datei-Suche"
		if wantDir {
			kind = "Ordner-Suche"
		}
		var sb strings.Builder
		sb.WriteString(kind + " „" + pattern + "“")
		if extra != "" {
			sb.WriteString(" (Filter: " + extra + ")")
		}
		sb.WriteString(" in diesem Ordner:\n")
		shown := 0
		for _, hit := range hits {
			if hit.IsDir != wantDir {
				continue
			}
			shown++
			if shown > limit {
				break
			}
			rel := stripScopePrefix(hit.Path, u.scopePath)
			line := "  " + rel
			if hit.IsDir {
				line += "/"
			}
			if hit.MTime != "" {
				line += "  " + hit.MTime
			}
			if !hit.IsDir && hit.Size > 0 {
				line += "  " + humanSize(hit.Size)
			}
			sb.WriteString(line + "\n")
		}
		if shown == 0 {
			sb.WriteString("  (keine Treffer — anderes Pattern versuchen)\n")
		}
		if total > limit {
			trace.Truncated = true
		}
		// Qualifizierte Zählung: das Modell erkennt selbst, ob das
		// Ergebnis vollständig (found ≤ limit) oder unvollständig ist.
		sb.WriteString(fmt.Sprintf("  found: %d, limit: %d", total, limit))
		if total > limit {
			sb.WriteString(" (unvollständig — es gibt weitere Treffer)")
		}
		sb.WriteString("\n")
		trace.Method = "report"
		trace.Chars = len(sb.String())
		trace.MS = time.Since(start).Milliseconds()
		return sb.String(), trace

	case "List":
		maxDepth := s.cfg.Chat.ListDepth
		entries, depthCut, entryCut, err := d.propfindTree(relPath, maxDepth, s.cfg.Chat.MaxListings)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		var sb strings.Builder
		sb.WriteString("Inhalt von ")
		if relPath == "" {
			sb.WriteString("dem geteilten Ordner")
		} else {
			sb.WriteString(relPath)
		}
		sb.WriteString(fmt.Sprintf(" (rekursiv, max. Tiefe %d):\n", maxDepth))
		if len(entries) == 0 {
			sb.WriteString("  (leerer Ordner)\n")
		}
		for _, e := range entries {
			line := "  " + e.Rel
			if e.IsDir {
				line += "/"
			}
			if e.MTime != "" {
				line += "  " + e.MTime
			}
			if !e.IsDir && e.Size > 0 {
				line += "  " + humanSize(e.Size)
			}
			sb.WriteString(line + "\n")
		}
		// Qualifizierte Zählung wie bei der Suche (found = gezeigte
		// Einträge, limit = max_listings; das Listing kennt die echte
		// Gesamtzahl nicht, daher der Kürzungs-Marker).
		sb.WriteString(fmt.Sprintf("  found: %d, limit: %d", len(entries), s.cfg.Chat.MaxListings))
		if entryCut {
			sb.WriteString(" (unvollständig — weitere Einträge vorhanden)")
		}
		sb.WriteString("\n")
		if depthCut {
			sb.WriteString(fmt.Sprintf("  … Verzeichnisse tiefer als Ebene %d wurden NICHT gelistet.\n", maxDepth))
		}
		trace.Method = "propfind"
		trace.Chars = len(sb.String())
		trace.Truncated = entryCut || depthCut
		trace.MS = time.Since(start).Milliseconds()
		return sb.String(), trace

	case "Read":
		// PROPFIND für die tatsächliche Dateigröße (Blind-Read liefert
		// oft nur einen Bruchteil — der Size-Header ist die Wahrheit).
		fileSize := int64(-1)
		if meta, metaErr := d.propfindMeta(relPath); metaErr == nil {
			if sz := meta["getcontentlength"]; sz != "" {
				if n, convErr := strconv.ParseInt(sz, 10, 64); convErr == nil {
					fileSize = n
				}
			}
		}
		data, ct, err := d.getfile(relPath)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			// 404: verfügbare Einträge der übergeordneten Ebene liefern
			if strings.Contains(err.Error(), "nicht gefunden") && relPath != "" {
				parentPath := relPath
				if idx := strings.LastIndex(relPath, "/"); idx > 0 {
					parentPath = relPath[:idx]
				} else {
					parentPath = ""
				}
				if entries, _, _, listErr := d.propfindTree(parentPath, 1, 50); listErr == nil && len(entries) > 0 {
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("Fehler: %s nicht gefunden.\nVerfügbare Einträge in %s:\n", relPath, relPathOrRoot(parentPath)))
					for _, e := range entries {
						line := "  " + e.Rel
						if e.IsDir {
							line += "/"
						}
						sb.WriteString(line + "\n")
					}
					return sb.String(), trace
				}
			}
			return "Fehler: " + err.Error(), trace
		}
		if len(data) == 0 {
			trace.Method = "empty"
			trace.MS = time.Since(start).Milliseconds()
			return "Die Datei existiert, ist aber leer (0 Bytes).", trace
		}
		// Bild: VLM-Beschreibung
		if strings.HasPrefix(strings.ToLower(ct), "image/") || looksLikeImage(data) {
			text, method := s.extractImage(data, ct)
			if text == "" {
				trace.Method = method
				trace.Error = "Bild konnte nicht beschrieben werden"
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: Bild konnte nicht beschrieben werden", trace
			}
			trace.Method = method
			trace.Chars = len(text)
			trace.MS = time.Since(start).Milliseconds()
			return text, trace
		}
		// Editierbare Texttypen: raw (mit optionaler Segmentierung)
		if s.cfg.Chat.isEditableFile(relPath) {
			text, method := s.readSegment(string(data), args.Offset, args.Limit)
			if text == "" {
				trace.Method = method
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: " + method, trace
			}
			trace.Method = method
			trace.Chars = len(text)
			trace.FileSize = fileSize
			trace.MS = time.Since(start).Milliseconds()
			// Warnung: empfangene Daten kleiner als PROPFIND-Size →
			// unvollständiges Read (Modell sieht den Hinweis im Text).
			if fileSize > 0 && int64(len(data)) < fileSize {
				text += fmt.Sprintf("\n\n[Hinweis: Nur %d von %d Bytes empfangen — die Datei ist unvollständig geladen. "+
					"Versuche es erneut oder lies mit offset/limit in Abschnitten.]", len(data), fileSize)
			}
			return text, trace
		}
		// Nicht-editierbar: s.extract (pandoc, pdftotext, etc.)
		text, method := s.extract(data, ct)
		if strings.HasPrefix(method, "error") || text == "" {
			trace.Method = method
			trace.Error = "Inhalt konnte nicht extrahiert werden"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Inhalt der Datei konnte nicht extrahiert werden", trace
		}
		max := s.cfg.Chat.MaxFileChars
		if len(text) > max {
			text = text[:max]
			trace.Truncated = true
			text += fmt.Sprintf("\n\n[Hinweis: Die Datei ist länger als %d Zeichen — der Rest ist NICHT enthalten. "+
				"Wenn die Frage den fehlenden Teil betreffen könnte, erwähne das in der Antwort, statt zu raten.]", max)
		}
		trace.Method = method
		trace.Chars = len(text)
		trace.MS = time.Since(start).Milliseconds()
		return text, trace

	case "Meta":
		meta, err := d.propfindMeta(relPath)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		var sb strings.Builder
		sb.WriteString("KI-Metadaten von ")
		sb.WriteString(relPath)
		sb.WriteString(":\n")
		if len(meta) == 0 {
			sb.WriteString("  (keine — die Datei wurde vermutlich noch nicht von der KI angereichert)\n")
		} else {
			keys := make([]string, 0, len(meta))
			for k := range meta {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				sb.WriteString("  " + k + " = " + meta[k] + "\n")
			}
		}
		trace.Method = "propfind"
		trace.Chars = len(sb.String())
		trace.MS = time.Since(start).Milliseconds()
		return sb.String(), trace

	case "Edit":
		// Unified-Diff-Patch auf eine Datei anwenden (nur Blank-Chat, editierbare Typen).
		if d == nil {
			trace.Error = "nicht verfügbar"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Edit ist nur im Arbeitsbereich verfügbar.", trace
		}
		if !s.cfg.Chat.isEditableFile(relPath) {
			trace.Error = "nicht editierbar"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Edit unterstützt nur editierbare Dateitypen.", trace
		}
		if strings.TrimSpace(args.Patch) == "" {
			trace.Error = "patch erforderlich"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: der 'patch'-Parameter darf nicht leer sein.", trace
		}
		data, _, err := d.getfile(relPath)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		newContent, changedLines, patchErr := applyUnifiedDiff(string(data), args.Patch)
		if patchErr != nil {
			trace.Error = patchErr.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Patch konnte nicht angewendet werden: " + patchErr.Error() +
				" Nutze Read, um den aktuellen Zeileninhalt zu prüfen.", trace
		}
		if err := d.sharePutFile(relPath, newContent); err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		trace.Method = "patch"
		trace.Path = relPath
		trace.Chars = len(args.Patch)
		trace.MS = time.Since(start).Milliseconds()
		return fmt.Sprintf("Patch angewendet: %d geänderte Zeilen in %s", changedLines, relPath), trace

	case "Write", "Mkdir", "Rmdir":
		// Write-Tools: nur bei Blank-Chat (d != nil, Share auf /workspace)
		if d == nil {
			trace.Error = "Schreiben nicht verfügbar"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: Schreiben ist für diesen Chat nicht verfügbar.", trace
		}
		fullPath, err := safeWritePath("", relPath, s.cfg.Chat.Write.MaxDepth)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		trace.Path = fullPath
		switch name {
		case "Write":
			if len(args.Content) == 0 {
				trace.Error = "leerer Content"
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: Content ist leer. Sendung abgelehnt — eine leere Datei würde die bestehende Datei überschreiben. Lade den aktuellen Stand mit Read und sende den vollständigen Inhalt erneut.", trace
			}
			if len(args.Content) > s.cfg.Chat.Write.MaxFileBytes {
				trace.Error = "Datei zu groß"
				trace.MS = time.Since(start).Milliseconds()
				return fmt.Sprintf("Fehler: Dateiinhalt zu groß (max. %d Bytes).", s.cfg.Chat.Write.MaxFileBytes), trace
			}
			if err := d.sharePutFile(relPath, args.Content); err != nil {
				trace.Error = err.Error()
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: " + err.Error(), trace
			}
			trace.Method = "put"
			trace.Chars = len(args.Content)
			trace.FileSize = int64(len(args.Content))
			trace.MS = time.Since(start).Milliseconds()
			return "Datei erfolgreich erstellt: " + fullPath, trace
		case "Mkdir":
			if err := d.shareMkdir(relPath); err != nil {
				trace.Error = err.Error()
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: " + err.Error(), trace
			}
			trace.Method = "mkcol"
			trace.MS = time.Since(start).Milliseconds()
			return "Verzeichnis erfolgreich angelegt: " + fullPath + "/", trace
		case "Rmdir":
			if err := d.shareDeletePath(relPath); err != nil {
				trace.Error = err.Error()
				trace.MS = time.Since(start).Milliseconds()
				return "Fehler: " + err.Error() + " (nur leere Verzeichnisse können entfernt werden)", trace
			}
			trace.Method = "delete"
			trace.MS = time.Since(start).Milliseconds()
			return "Verzeichnis erfolgreich entfernt: " + fullPath, trace
		}
		fallthrough
	default:
		trace.Error = "unbekanntes Tool"
		trace.MS = time.Since(start).Milliseconds()
		return "Fehler: unbekanntes Tool: " + name, trace
	}
}

// looksLikeImage: Magic-Bytes-Check für den Fall, dass Content-Type fehlt.
func looksLikeImage(data []byte) bool {
	if len(data) < 12 {
		return false
	}
	switch {
	case data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF: // JPEG
		return true
	case data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G': // PNG
		return true
	case data[0] == 'G' && data[1] == 'I' && data[2] == 'F': // GIF
		return true
	case data[0] == 'R' && data[1] == 'I' && data[2] == 'F' && data[3] == 'F': // WebP
		return true
	case data[0] == 0x42 && data[1] == 0x4D: // BMP
		return true
	case bytes.HasPrefix(data, []byte("<?xml")) || bytes.HasPrefix(data, []byte("<svg")): // SVG
		return true
	}
	return false
}

// readSegment liefert einen zeilenweisen Ausschnitt eines Text-Dokuments.
// offset/limit sind 1-basiert. Tolerant: außerhalb des Bereichs liefert
// die verfügbaren Zeilen + Hinweis statt Fehler. Bei Kürzung gibt der
// Hinweis die exakte Fortsetzungs-Position an, damit das Modell die
// Datei vollständig in Abschnitten lesen kann.
func (s *Server) readSegment(data string, offset, limit int) (string, string) {
	lines := strings.Split(data, "\n")
	totalLines := len(lines)
	if offset < 1 {
		offset = 1
	}
	if limit < 1 {
		limit = 500
	}
	if offset > totalLines {
		// Tolerant: letzte `limit` Zeilen + Hinweis
		start := totalLines - limit + 1
		if start < 1 {
			start = 1
		}
		var sb strings.Builder
		for i := start - 1; i < totalLines; i++ {
			fmt.Fprintf(&sb, "%5d  %s\n", i+1, lines[i])
		}
		sb.WriteString(fmt.Sprintf("\n[Datei hat %d Zeilen. offset=%d war außerhalb des Bereichs — hier sind die letzten %d Zeilen (%d–%d)]",
			totalLines, offset, totalLines-start+1, start, totalLines))
		return sb.String(), "segment"
	}
	end := offset + limit - 1
	if end > totalLines {
		end = totalLines
	}
	var sb strings.Builder
	for i := offset - 1; i < end; i++ {
		fmt.Fprintf(&sb, "%5d  %s\n", i+1, lines[i])
	}
	if end < totalLines {
		// Angekürzt: exakte Fortsetzungs-Position nennen
		sb.WriteString(fmt.Sprintf("\n[Zeilen %d–%d von %d Zeilen — FORTSETZUNG: Read mit offset=%d, limit=%d für den Rest]",
			offset, end, totalLines, end+1, totalLines-end))
	} else {
		sb.WriteString(fmt.Sprintf("\n[Alle %d Zeilen gezeigt (Zeilen %d–%d)]", end-offset+1, offset, end))
	}
	return sb.String(), "segment"
}

// ── Request/Response ─────────────────────────────────────────

type chatAskShare struct {
	Token    string `json:"token"`
	Password string `json:"password"`
}

type chatAskRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Context struct {
		Share      chatAskShare `json:"share"`
		FolderName string       `json:"folder_name"`
		// Scope: der geteilte Ordner als Resource-ID (
		// <storageid>$<spaceid>!<opaqueid>) + Pfad relativ zur Space-Root.
		// Beide optional — ohne Scope keine Such-Tools.
		Scope struct {
			ResourceID string `json:"resource_id"`
			Path       string `json:"path"`
		} `json:"scope"`
		// Write: Blank-Chat — schreiben in den Workspace-Ordner des
		// persönlichen Spaces. Die Extension erzeugt einen Public-Link-Share
		// auf /workspace und schickt token+password. Taki nutzt denselben
		// shareWebDav-Mechanismus wie der Folder-Chat (Basic Auth).
		// Leer = keine Write-Tools (Share/Scope-Chat bleibt read-only).
		Write struct {
			Share chatAskShare `json:"share"`
		} `json:"write"`
	} `json:"context"`
	// Stream: live Fortschritt per Server-Sent-Events
	// (start/phase/tool/error/done). Default false = finale JSON-Antwort.
	Stream bool `json:"stream"`
}

type chatAskResponse struct {
	Answer     string      `json:"answer"`
	ToolTrace  []toolTrace `json:"tool_trace"`
	Iterations int         `json:"iterations"`
	Model      string      `json:"model"`
	// Vom Modell per present_options gereichte (bzw. beim Loop-Abbruch
	// serverseitig gesetzte) Antwort-Optionen — klickbar in der Chat-UI.
	Options []string `json:"options,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// handleChatTools: GET /chat/tools — liefert die Tool-Definitionen.
func (s *Server) handleChatTools(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.chatTools())
}

// ── Chat-Token (langlebig, Taki-validiert) ───────────────────
//
// Hintergrund: Das OIDC-Gate des Proxys akzeptiert nur IDP-Tokens — den
// vom Proxy pro Request geprägten 1-Day-User-JWT kann die Extension nicht
// als Bearer präsentieren (würde am Gate 401 werden). Langlaufende
// Folder-Chats (10–30 min) sollen daher nicht auf den 5-minütigen
// Browser-Bearer setzen. Lösung in zwei Endpunkten:
//
//	GET /chat/token        (OIDC-gated Route): wrappt den vom Proxy
//	                       geprägten User-JWT in ein HMAC-Chat-Token.
//	POST /chat-direct/ask  (unprotected Route): validiert das Token,
//	                       stellt den User-JWT wieder her, delegiert an
//	                       handleChatAsk.
//
// Token-Format: base64url(userJWT) + "." + exp(unix) + "." +
// base64url(HMAC-SHA256(secret, userJWT + "." + exp)).

// parseJWTExp liest die exp-Claim aus einem JWT-Payload (Base64-URL-JSON),
// ohne die Signatur zu prüfen. 0 = nicht vorhanden/ungültig.
func parseJWTExp(jwt string) int64 {
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return 0
	}
	return claims.Exp
}

func chatTokenSign(secret, userJWT string, exp int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(userJWT + "." + strconv.FormatInt(exp, 10)))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(userJWT)) +
		"." + strconv.FormatInt(exp, 10) + "." + sig
}

// chatTokenVerify prüft Format, Expiry und HMAC (konstantzeitig) und
// liefert den eingebetteten User-JWT.
func chatTokenVerify(secret, token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	userJWT, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(userJWT) == 0 {
		return "", false
	}
	exp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || exp < time.Now().Unix() {
		return "", false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(string(userJWT) + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, mac.Sum(nil)) {
		return "", false
	}
	return string(userJWT), true
}

// bearerFromHeader entnimmt den Token aus "Authorization: Bearer <token>".
func bearerFromHeader(h string) string {
	const bearerPrefix = "Bearer "
	return strings.TrimPrefix(h, bearerPrefix)
}

// handleChatToken: GET /chat/token (OIDC-gated Proxy-Route) — wrappt den
// vom Proxy als x-access-token geprägten 1-Day-User-JWT in ein langlebiges
// Chat-Token. Läuft spätestens mit dem eingebetteten User-JWT aus.
func (s *Server) handleChatToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.Chat.ChatToken.Secret == "" {
		writeChatError(w, http.StatusServiceUnavailable, "Chat-Token nicht konfiguriert")
		return
	}
	jwt := r.Header.Get("x-access-token")
	if jwt == "" {
		jwt = bearerFromHeader(r.Header.Get("Authorization"))
	}
	if jwt == "" {
		writeChatError(w, http.StatusUnauthorized, "kein User-JWT vorhanden")
		return
	}
	exp := time.Now().Add(time.Duration(s.cfg.Chat.ChatToken.TTLHours) * time.Hour).Unix()
	if userExp := parseJWTExp(jwt); userExp > 0 && userExp < exp {
		exp = userExp
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token": chatTokenSign(s.cfg.Chat.ChatToken.Secret, jwt, exp),
		"exp":   exp,
	})
}

// handleChatDirectAsk: POST /chat-direct/ask (unprotected Proxy-Route) —
// validiert das Chat-Token, stellt den User-JWT als x-access-token wieder
// her und delegiert an handleChatAsk (Share-Zugriff bleibt Share-Token-
// gebunden, Suche läuft über den wiederhergestellten User-JWT).
// chatVerifyToken validiert das langlebige Chat-Token und stellt den
// User-JWT als x-access-token wieder her. ok=false → Auth-Fehler ist
// schon an den Client geschickt.
func (s *Server) chatVerifyToken(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.Chat.ChatToken.Secret == "" {
		writeChatError(w, http.StatusServiceUnavailable, "Chat-Token nicht konfiguriert")
		return false
	}
	token := bearerFromHeader(r.Header.Get("Authorization"))
	jwt, ok := chatTokenVerify(s.cfg.Chat.ChatToken.Secret, token)
	if !ok {
		writeChatError(w, http.StatusUnauthorized, "ungültiges oder abgelaufenes Chat-Token")
		return false
	}
	r.Header.Set("x-access-token", jwt)
	return true
}

func (s *Server) handleChatDirectAsk(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}
	s.handleChatAsk(w, r)
}

// maxChatTranscribeBytes: Cap für Audio-Aufnahmen (WebM/OGG ~150 kB/min,
// 50 MB ≈ 5 h Aufnahme — reichlich für Chat-Zwecke).
const maxChatTranscribeBytes = 50 * 1024 * 1024

// handleChatTranscribe: POST /chat-direct/transcribe (unprotected Proxy-Route,
// Chat-Token-Auth) — leitet eine Audio-Aufnahme (Multipart-Feld "file") über
// Whisper (microllm llm-stt) an und liefert den Transkript-Text zurück.
// Antwort: {"text": "…", "method": "whisper"}.
func (s *Server) handleChatTranscribe(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.Whisper.APIBase == "" {
		writeChatError(w, http.StatusServiceUnavailable, "Whisper nicht konfiguriert")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxChatTranscribeBytes+1024*1024)
	mr, err := r.MultipartReader()
	if err != nil {
		writeChatError(w, http.StatusBadRequest, "multipart/form-data erwartet: "+err.Error())
		return
	}

	var audioData []byte
	var audioName, audioCT string
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeChatError(w, http.StatusBadRequest, "multipart-Fehler: "+err.Error())
			return
		}
		if part.FormName() != "file" {
			_, _ = io.Copy(io.Discard, part)
			continue
		}
		// Cap auf maxChatTranscribeBytes (Buffer-Overrun → 413)
		audioData, err = io.ReadAll(io.LimitReader(part, maxChatTranscribeBytes+1))
		if err != nil {
			writeChatError(w, http.StatusBadRequest, "Audio-Fehler: "+err.Error())
			return
		}
		if len(audioData) > maxChatTranscribeBytes {
			writeChatError(w, http.StatusRequestEntityTooLarge, "Audio-Datei zu groß (max. 50 MB)")
			return
		}
		audioName = part.FileName()
		audioCT = part.Header.Get("Content-Type")
		break
	}
	if audioData == nil {
		writeChatError(w, http.StatusBadRequest, "Feld 'file' fehlt")
		return
	}

	text, method := s.transcribeAudioBytes(audioData, audioCT, audioName)
	if method != "whisper" {
		status := http.StatusBadGateway
		if method == "skipped" {
			status = http.StatusServiceUnavailable
		}
		writeChatError(w, status, text)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"text": text, "method": method})
}

// truncateChars kürzt einen String auf n Runes, mit Kürzungs-Hinweis.
func truncateChars(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + " … (gekürzt)"
}

// loopBreakAnswer baut die finale Antwort nach einem serverseitigen
// Loop-Stopp: eine Nachfrage, kein Fehler. Der Server meldet, was
// bisher gesehen wurde, der User entscheidet, wie es weitergeht.
func loopBreakAnswer(reason, lastTool, lastResult string) string {
	s := "Ich komme mit der aktuellen Vorgabe nicht weiter: " + reason + ".\n"
	if lastResult != "" {
		toolNote := ""
		if lastTool != "" {
			toolNote = " (" + lastTool + ")"
		}
		s += "\nBisher gesehen" + toolNote + ":\n" + lastResult + "\n"
	}
	s += "\nWie möchtest du weitermachen? Zum Beispiel:\n" +
		"1) Konkretisieren: präzisere Begriffe, Dokumenttyp (z. B. Rechnung, Bescheid) oder Zeitraum angeben.\n" +
		"2) Limitieren: auf einen bestimmten Unterordner, Dateityp oder eine Ergebnisanzahl beschränken.\n" +
		"3) Durchlaufen lassen: ich bewerte den Ordner komplett — das kann mehrere Minuten dauern."
	return s
}

// loopBreakOptions: die drei Standard-Optionen des serverseitigen
// Loop-Abbruchs — klickbar für Extensions, die das "options"-Event
// kennen. Der Text von loopBreakAnswer bleibt für ältere Clients
// selbsterklärend.
var loopBreakOptions = []string{
	"Konkretisieren: Ich gebe jetzt präzisere Begriffe, Dokumenttyp (z. B. Rechnung, Bescheid) oder einen Zeitraum an.",
	"Limitieren: Beschränke die Auswertung auf einen bestimmten Unterordner, Dateityp oder eine Ergebnisanzahl.",
	"Durchlaufen lassen: Bewerte den Ordner komplett, das kann mehrere Minuten dauern.",
}

// parsePresentOptions liest die Argumente von present_options
// ({"options": [ ... ]}) und liefert 1-5 getrimmte, nicht-leere
// Strings. ok=false → die Argumente sind kein gültiges
// Options-Array (das Modell bekommt einen Fehler-Hint).
func parsePresentOptions(argsJSON string) ([]string, bool) {
	var args struct {
		Options []string `json:"options"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return nil, false
	}
	options := make([]string, 0, len(args.Options))
	for _, option := range args.Options {
		option = strings.TrimSpace(option)
		if option == "" {
			continue
		}
		options = append(options, option)
		if len(options) == 5 {
			break
		}
	}
	if len(options) == 0 {
		return nil, false
	}
	return options, true
}

// handleChatAsk: POST /chat/ask — Tool-Loop gegen den ephemeralen Share.
func (s *Server) handleChatAsk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	var req chatAskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeChatError(w, http.StatusBadRequest, "ungültiger Request: " + err.Error())
		return
	}
	// Blank-Chat: context.write.share (token+password) — Public-Link-Share
	// auf /workspace im persönlichen Space. Die Extension erzeugt den Share
	// (OIDC-gated) und Taki nutzt dieselbe shareWebDav-Auth wie Folder-Chat.
	hasWriteShare := req.Context.Write.Share.Token != "" && req.Context.Write.Share.Password != ""
	if len(req.Messages) == 0 {
		writeChatError(w, http.StatusBadRequest, "messages erforderlich")
		return
	}

	model := req.Model
	if model == "" {
		model = s.cfg.Chat.DefaultModel
	}

	tools := s.chatTools()

	// User-JWT (vom Proxy als x-access-token, Bearer-Fallback für
	// Direkt-Tests) → WebDAV-REPORT-Suche auf den mitgeschickten Scope.
	// Im Blank-Chat ist er zwingend — ohne JWT keine Write-Tools.
	jwt := r.Header.Get("x-access-token")
	if jwt == "" {
		const bearerPrefix = "Bearer "
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, bearerPrefix) {
			jwt = strings.TrimPrefix(h, bearerPrefix)
		}
	}
	// Blank-Chat: context.write.share — Write-Tools gegen /workspace.
	// Der Share wird von der Extension erzeugt (OIDC-gated); Taki nutzt
	// denselben shareWebDav-Mechanismus wie der Folder-Chat (Basic Auth).
	isBlankChat := hasWriteShare
	if isBlankChat {
		tools = append(tools, chatWriteTools()...)
	}
	if !isBlankChat && (req.Context.Share.Token == "" || req.Context.Share.Password == "") {
		writeChatError(w, http.StatusBadRequest, "context.share (token + password) erforderlich")
		return
	}
	var u *userWebDav
	if jwt != "" {
		u = newUserWebDav(s, jwt)
		if isBlankChat {
			// Blank-Chat: Workspace als Such-Scope
			if spaceID := u.resolvePersonalSpaceID(); spaceID != "" {
				u.scopeID = spaceID
				u.scopePath = "workspace"
			}
		} else {
			u.scopeID = req.Context.Scope.ResourceID
			u.scopePath = req.Context.Scope.Path
		}
	}
	searchAvailable := u != nil && u.scopeID != ""

	// System-Prompt (serverseitig, deterministisch)
	var sysPrompt string
	if isBlankChat {
		writeTools := "Du kannst Dateien in deinem Arbeitsbereich mit den Tools Mkdir (Verzeichnis anlegen), Write (Datei erstellen/überschreiben), Edit (kleine Änderungen per Unified-Diff) und Rmdir (leeres Verzeichnis entfernen) lesen und schreiben. "
		sysPrompt = renderChatBlankSystemPrompt(s, "workspace", writeTools)
	} else {
		folder := req.Context.FolderName
		if folder == "" {
			folder = "der geteilte Ordner"
		}
		searchTools := ""
		if searchAvailable {
			searchTools = "Bei großen Ordnern: erst mit Search (type=\"file\" für Dateien, type=\"dir\" für Verzeichnisse) " +
				"suchen, statt alles aufzulisten. Suchtreffer sind mit Read lesbar " +
				"(Pfad relativ zum Ordner). " +
				"Strukturierte Suche: der optionale extra-Parameter verknüpft eine zusätzliche " +
				"Bedingung mit AND, z. B. extra=\"metadata.doc.type:rechnung\" (KI-Dokumenttyp), " +
				"extra=\"mediatype:pdf\" (Dateityp), extra=\"metadata.doc.date:*2025*\" (Dokumentenjahr), " +
				"extra=\"tag:privat\" (Tag) — Wildcards OHNE Anführungszeichen. Metadaten-Felder " +
				"(doc.type, doc.date, doc.title, doc.subject, sender.name, amounts.total) existieren " +
				"nur bei KI-angereicherten Dateien. Metadaten-Werte sind exakt indiziert: ein Wert " +
				"ohne Wildcard muss stimmen, mit Wildcard (*wert*) matcht er als Substring im " +
				"gesamten Feldwert. Mehrwort-Begriffe in extra gehören in Anführungszeichen. "
		}
		sysPrompt = renderChatSystemPrompt(s, folder, searchTools)
	}

	messages := make([]chatToolMessage, 0, len(req.Messages)+4)
	messages = append(messages, chatToolMessage{Role: "system", Content: strPtr(sysPrompt)})
	for _, m := range req.Messages {
		role := m.Role
		if role != "user" && role != "assistant" {
			role = "user"
		}
		messages = append(messages, chatToolMessage{Role: role, Content: strPtr(m.Content)})
	}

	var d *shareWebDav
	if isBlankChat {
		d = newShareWebDav(s, req.Context.Write.Share.Token, req.Context.Write.Share.Password)
	} else {
		d = newShareWebDav(s, req.Context.Share.Token, req.Context.Share.Password)
	}
	toolTraces := make([]toolTrace, 0, 8)
	answer := ""
	iterations := 0
	// Loop-Schutz: Tool-Calls mit identischen Parametern werden nicht
	// erneut ausgeführt — sie liefern per Definition dasselbe Ergebnis.
	// stattdessen erhält das Modell einen Hinweis, die Parameter zu
	// ändern oder den User zu fragen.
	seenToolCalls := map[string]int{}
	// Nach 3 aufeinanderfolgenden Duplikaten bricht der Loop
	// serverseitig ab — unabhängig vom Modell-Verhalten. Das letzte
	// tatsächlich ausgeführte Tool + sein (gekürztes) Ergebnis werden
	// im Abbruch-Report als Zwischenergebnis gemeldet.
	consecutiveDuplicates := 0
	lastRealTool := ""
	lastRealResult := ""
	// present_options beendet den Turn: das Modell reicht dem User
	// konkrete, anklickbare Optionen (SSE-Event "options" + JSON-Feld
	// "options"). Beim serverseitigen Loop-Abbruch liefert der Server
	// selbst die drei Standard-Optionen (loopBreakOptions).
	loopOptions := []string{}
	optionsDone := false

	lastUserQuestion := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			lastUserQuestion = req.Messages[i].Content
			break
		}
	}
	sessionID := fmt.Sprintf("c%04x", time.Now().UnixNano()&0xffffff)
	log.Printf("chat/ask [%s]: blank=%v folder=%q model=%s messages=%d stream=%v search=%v question=%q",
		sessionID, isBlankChat, req.Context.FolderName, model, len(req.Messages), req.Stream, searchAvailable, truncateChars(lastUserQuestion, 200))

	// Stream: Live-Fortschritt per SSE. Client-Disconnect bricht die
	// Iterationen ab. Nicht-flushbarer Writer → Fallback auf JSON-Antwort.
	// Der Heartbeat (SSE-Kommentar ": ping") hält die Verbindung über
	// Traefik-Idle-Timeouts am Leben, während das Modell wartet.
	stream := false
	var sse *sseStream
	if req.Stream {
		if f, ok := w.(http.Flusher); ok {
			stream = true
			sse = &sseStream{w: w, fl: f}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(http.StatusOK)
			if err := sse.event("start", map[string]string{"model": model, "folder": req.Context.FolderName}); err != nil {
				return
			}
			heartbeatDone := make(chan struct{})
			go func() {
				ticker := time.NewTicker(time.Duration(s.cfg.Chat.Heartbeat.IntervalSeconds) * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						sse.ping()
					case <-heartbeatDone:
						return
					}
				}
			}()
			defer close(heartbeatDone)
		}
	}

	for i := 0; i < s.cfg.Chat.MaxIterations; i++ {
		iterations = i + 1
		// Toter Client: abgebrochene Verbindung soll keine LLM-Iterationen
		// mehr verbrennen.
		if r.Context().Err() != nil {
			log.Printf("chat/ask [%s]: Client-Verbindung abgebrochen, Abbruch nach %d Iteration(en)", sessionID, iterations-1)
			return
		}
		if stream {
			if err := sse.event("phase", map[string]string{"phase": "thinking", "iteration": strconv.Itoa(iterations)}); err != nil {
				return
			}
		}
		msg, finishReason, err := s.llmChatTools(sessionID, model, messages, tools)
		if err != nil {
			if stream {
				sse.event("error", map[string]string{"error": err.Error()})
				return
			}
			writeChatError(w, http.StatusBadGateway, err.Error())
			return
		}
		messages = append(messages, *msg)

		content := ""
		if msg.Content != nil {
			content = *msg.Content
		}

		if len(msg.ToolCalls) == 0 {
			answer = content
			if answer == "" {
				answer = "(Modell hat keine Antwort geliefert, FinishReason: " + finishReason + ")"
			}
			break
		}

		for _, tc := range msg.ToolCalls {
			// present_options ist terminal: gültige Optionen beenden den
			// Turn sofort (der User klickt, die Option kommt als nächste
			// User-Nachricht zurück). Ungültige Argumente bekommen einen
			// Fehler-Hint, damit das Modell den Call korrigieren kann.
			if tc.Function.Name == "present_options" {
				options, optionsValid := parsePresentOptions(tc.Function.Arguments)
				trace := toolTrace{Tool: "present_options", Method: "options"}
				var result string
				if !optionsValid {
					result = "Fehler: present_options erwartet JSON im Format " +
						"{\"options\": [\"Option 1\", \"Option 2\"]} mit 1-5 kurzen Strings. " +
						"Übergib die Optionen bitte erneut in diesem Format."
					trace.Error = "ungültige Optionen"
				} else {
					loopOptions = options
					answer = content
					if answer == "" {
						answer = "Wähle eine Option oder antworte frei:"
					}
					result = "Die Optionen werden dem User angezeigt; der Turn endet."
					trace.Chars = len(answer)
				}
				log.Printf("chat/ask [%s]: tool=present_options valid=%v options=%d args=%q",
					sessionID, optionsValid, len(options), tc.Function.Arguments)
				toolTraces = append(toolTraces, trace)
				if stream {
					if err := sse.event("tool", map[string]interface{}{
						"index":     len(toolTraces),
						"tool":      "present_options",
						"method":    "options",
						"chars":     trace.Chars,
						"error":     trace.Error,
						"ms":        trace.MS,
					}); err != nil {
						return
					}
				}
				messages = append(messages, chatToolMessage{
					Role:       "tool",
					Content:    strPtr(result),
					ToolCallID: tc.ID,
				})
				if optionsValid {
					optionsDone = true
					break
				}
				continue
			}
			callKey := tc.Function.Name + "\x00" + strings.TrimSpace(tc.Function.Arguments)
			// Read: nur path als Key (offset/limit-Variationen umgehen sonst den Protector)
			if tc.Function.Name == "Read" {
				var readArgs struct {
					Path string `json:"path"`
				}
				if json.Unmarshal([]byte(tc.Function.Arguments), &readArgs) == nil && readArgs.Path != "" {
					callKey = "Read\x00" + readArgs.Path
				}
			}
			var result string
			var trace toolTrace
			if prevChars, alreadyRun := seenToolCalls[callKey]; alreadyRun {
				consecutiveDuplicates++
				// Write-Tools sind idempotent: gleiche Parameter = gleiches
				// Ergebnis. Statt "Wiederholung bringt nichts" melden wir
				// Erfolg, damit das Modell den Turn beenden kann.
				if tc.Function.Name == "Write" || tc.Function.Name == "Edit" ||
					tc.Function.Name == "Mkdir" || tc.Function.Name == "Rmdir" {
					result = fmt.Sprintf("OK: Dieser %s-Call mit identischen Parametern wurde bereits "+
						"erfolgreich ausgeführt (letzte Ausführung: %d Zeichen). Die Datei/der "+
						"Zustand ist bereits aktualisiert. Beende den Turn mit einer Antwort "+
						"an den User.",
						tc.Function.Name, prevChars)
				} else {
					result = fmt.Sprintf("Wiederholung: Dieser Tool-Call (tool=%s, identische Parameter) "+
						"wurde bereits ausgeführt und bringt kein neues Ergebnis (letzte Ausführung: "+
						"%d Zeichen). Ändere die Parameter (z. B. anderes Pattern, anderes extra, "+
						"anderer Pfad) oder beende den Turn mit einer Antwort bzw. Frage an den User.",
						tc.Function.Name, prevChars)
				}
				trace = toolTrace{Tool: tc.Function.Name, Method: "duplicate", Chars: len(result)}
			} else {
				consecutiveDuplicates = 0
				result, trace = s.runChatTool(d, u, tc.Function.Name, tc.Function.Arguments)
				seenToolCalls[callKey] = len(result)
				if result != "" {
					lastRealTool = tc.Function.Name
					lastRealResult = truncateChars(result, 1500)
				}
			}
			// args = die exakte Anfrage (auch bei Duplikaten, die runChatTool
			// nie erreichen und daher leere trace-Felder haben).
			log.Printf("chat/ask [%s]: tool=%s args=%q path=%q pattern=%q extra=%q ms=%d chars=%d file_size=%d truncated=%v err=%q",
				sessionID, tc.Function.Name, tc.Function.Arguments, trace.Path, trace.Pattern, trace.Extra, trace.MS, trace.Chars, trace.FileSize, trace.Truncated, trace.Error)
			// Duplikate erreichen nur Modell (als Hint) und Journal —
			// nicht den UI-Trace, um verwirrende Wiedereinträge zu vermeiden.
			if trace.Method != "duplicate" {
				toolTraces = append(toolTraces, trace)
				if stream {
					if err := sse.event("tool", map[string]interface{}{
						"index":     len(toolTraces),
						"tool":      tc.Function.Name,
						"path":      trace.Path,
						"pattern":   trace.Pattern,
						"extra":     trace.Extra,
						"method":    trace.Method,
						"chars":     trace.Chars,
						"truncated": trace.Truncated,
						"error":     trace.Error,
						"ms":        trace.MS,
					}); err != nil {
						return
					}
				}
			}
			messages = append(messages, chatToolMessage{
				Role:       "tool",
				Content:    strPtr(result),
				ToolCallID: tc.ID,
			})
		}

		if optionsDone {
			break
		}

		// Serverseitiger Abbruch: das Modell wiederholt sich und liefert
		// kein neues Ergebnis. Statt blind weiterzulaufen (970 Iterationen
		// im August-Fall) antworten mit Zwischenergebnis + Optionen.
		if consecutiveDuplicates >= 3 {
			log.Printf("chat/ask [%s]: loop-break nach %d aufeinanderfolgenden Duplikaten (iteration %d)", sessionID, consecutiveDuplicates, iterations)
			answer = loopBreakAnswer("die gleiche Anfrage wiederholt lieferte kein neues Ergebnis (Wiederholungsschleife)", lastRealTool, lastRealResult)
			loopOptions = loopBreakOptions
			break
		}

		// Wenn das Modell nach dem letzten Tool-Block nichts mehr sagt
		// und Iterationen aufgebraucht sind, Antwort unten zusammenführen.
		if content != "" && len(msg.ToolCalls) == 0 {
			answer = content
			break
		}
	}

	if answer == "" && iterations > 0 {
		answer = loopBreakAnswer("die Maximalzahl an Iterationen erreicht wurde", lastRealTool, lastRealResult)
		loopOptions = loopBreakOptions
	}

	resp := chatAskResponse{
		Answer:     answer,
		ToolTrace:  toolTraces,
		Iterations: iterations,
		Model:      model,
		Options:    loopOptions,
	}
	if stream {
		// Vor "done": die Extension kann die Buttons mit der Antwort
		// rendern, ohne das done-Payload warten zu müssen.
		if len(loopOptions) > 0 {
			if err := sse.event("options", map[string]interface{}{"options": loopOptions}); err != nil {
				return
			}
		}
		if err := sse.event("done", resp); err != nil {
			return
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func writeChatError(w http.ResponseWriter, status int, msg string) {
	log.Printf("chat/ask error (%d): %s", status, msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(chatAskResponse{Error: msg})
}

// sseStream serialisiert alle SSE-Write (Event-Daten + Heartbeat-Goroutine).
type sseStream struct {
	w  http.ResponseWriter
	fl http.Flusher
	mu sync.Mutex
}

// event schreibt einen SSE-Frame und flusht ihn sofort.
// Fehler = Client hat die Verbindung geschlossen → aufrufende Seite bricht ab.
func (s *sseStream) event(name string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, payload); err != nil {
		return err
	}
	s.fl.Flush()
	return nil
}

// ping schreibt einen SSE-Kommentar-Frame — von allen Client-Parsers
// ignoriert, hält aber die TCP-Verbindung über Reverse-Proxy-Idle-Timeouts
// am Leben. Bei totem Client fehlerstill (Goroutine endet ohnehin beim
// Request-Ende).
func (s *sseStream) ping() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprint(s.w, ": ping\n\n")
	s.fl.Flush()
}
