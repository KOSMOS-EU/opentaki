package main

// chat.go — Ordner-Chat via Tool-Loop (Phase 1, ohne Streaming)
//
// Das Modell beschafft sich den Dokumenteninhalt selbst über Tools:
//
//	list_directory(path) → WebDAV PROPFIND depth=1, rekursiv (BFS, begrenzt:
//	                       list_depth Ebenen, max_listings Einträge gesamt)
//	read_file(path)      → WebDAV GET + s.extract (pdf/office/svg/text/…)
//	read_image(path)     → WebDAV GET + VLM-Beschreibung
//
// Zugriffsmodell: Taki bekommt KEIN User-Token. Der Client (Web-Extension)
// legt einen Public-Link-Share (view, password, 1h) auf den Ordner an —
// derselbe Mechanismus wie die Jobs/Pipelines (origin_url). Der Share-Token
// begrenzt den Zugriff strukturell auf den Ordner-Teilbaum (OpenCloud erzwung,
// nicht Taki-App-Logik). Taki hält Token/Password nur in-memory pro Request
// und LOGGT sie nie.

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
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
			Name:        "list_directory",
			Description: "Listet den Inhalt eines Verzeichnisses im geteilten Ordner REKURSIV (begrenzt durch Tiefe und Eintragszahl, Kürzungen werden angegeben): Unterverzeichnisse (mit /) und Dateien, jeweils mit relativem Pfad — der Pfad ist direkt als read_file-/read_image-Pfad nutzbar. Liefert KEINE Dateiinhalte. Leerer Pfad = der geteilte Ordner selbst.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad relativ zum geteilten Ordner, leer = Basisordner"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "read_file",
			Description: "Liest den Textinhalt einer Datei (txt, md, pdf, office, csv, svg, code, …). Große Dateien werden gekürzt. Pfade relativ zum geteilten Ordner.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Datei relativ zum geteilten Ordner"}},"required":["path"]}`),
		}},
		{Type: "function", Function: toolFunction{
			Name:        "read_image",
			Description: "Liest eine Bilddatei (jpg, png, …) und liefert eine detaillierte Beschreibung inkl. aller lesbaren Texte (VLM). Pfade relativ zum geteilten Ordner.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"Pfad der Bilddatei relativ zum geteilten Ordner"}},"required":["path"]}`),
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
func (s *Server) llmChatTools(model string, messages []chatToolMessage, tools []toolDefinition) (*chatToolMessage, string, error) {
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
			log.Printf("chat LLM error (attempt %d/%d): %v", attempt+1, maxRetries, err)
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
			log.Printf("chat LLM response parse error (HTTP %d, attempt %d/%d): %v (raw: %.300s)",
				resp.StatusCode, attempt+1, maxRetries, err, string(respBody))
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
			log.Printf("chat LLM HTTP %d (attempt %d/%d), retrying in %v",
				resp.StatusCode, attempt+1, maxRetries, backoff[attempt])
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return nil, "", fmt.Errorf("LLM-Backend fehlerhaft (HTTP %d)", resp.StatusCode)
		}

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

// shareAuthError erkennt Share-Auth-/Scope-Fehler (401 = password/abgelaufen,
// 404 = außerhalb des Share-Scope oder nicht vorhanden).
func shareAuthError(resp *http.Response) (string, bool) {
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return "Share-Zugriff verweigert (Password falsch oder Share abgelaufen — bitte Chat neu starten)", true
	}
	if resp.StatusCode == 404 {
		return "Datei/Ordner nicht gefunden (außerhalb des geteilten Ordners?)", true
	}
	return "", false
}

// listingEntry ist ein Eintrag in einem Verzeichnis (Name relativ zum
// gelisteten Ordner, isDir laut resourcetype bzw. Href-Endschiff).
type listingEntry struct {
	Name  string
	IsDir bool
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
		entries = append(entries, listingEntry{Name: name, IsDir: isDir})
	}
	return entries, nil
}

// walkEntry ist ein Eintrag im rekursiven Listing (Pfad relativ zur
// Share-Root — direkt als read_file-Pfad verwendbar).
type walkEntry struct {
	Rel   string
	IsDir bool
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
			entries = append(entries, walkEntry{Rel: child, IsDir: k.IsDir})
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

// ── Tool-Ausführung ──────────────────────────────────────────

type toolTrace struct {
	Tool      string `json:"tool"`
	Path      string `json:"path,omitempty"`
	Method    string `json:"method,omitempty"`
	Chars     int    `json:"chars"`
	Truncated bool   `json:"truncated,omitempty"`
	Error     string `json:"error,omitempty"`
	MS        int64  `json:"ms"`
}

// runChatTool führt einen Tool-Call aus und liefert (tool-Result, Trace).
func (s *Server) runChatTool(d *shareWebDav, name, argsJSON string) (string, toolTrace) {
	start := time.Now()
	trace := toolTrace{Tool: name}

	var args struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		trace.Error = "ungültige Argumente: " + err.Error()
		trace.MS = time.Since(start).Milliseconds()
		return "Fehler: ungültige Tool-Argumente: " + err.Error(), trace
	}
	relPath, err := safeRelPath(args.Path)
	if err != nil {
		trace.Error = err.Error()
		trace.MS = time.Since(start).Milliseconds()
		return "Fehler: " + err.Error(), trace
	}
	trace.Path = relPath

	switch name {
	case "list_directory":
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
			sb.WriteString(line + "\n")
		}
		if entryCut {
			sb.WriteString(fmt.Sprintf("  … Listing gekürzt: max. %d Einträge — es gibt WEITERE Dateien/Verzeichnisse. "+
				"Falls die Frage darauf ankommt, liste ein bestimmtes Unterverzeichnis einzeln auf.\n", s.cfg.Chat.MaxListings))
		}
		if depthCut {
			sb.WriteString(fmt.Sprintf("  … Verzeichnisse tiefer als Ebene %d wurden NICHT gelistet.\n", maxDepth))
		}
		trace.Method = "propfind"
		trace.Chars = len(sb.String())
		trace.Truncated = entryCut || depthCut
		trace.MS = time.Since(start).Milliseconds()
		return sb.String(), trace

	case "read_file":
		data, ct, err := d.getfile(relPath)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		if len(data) == 0 {
			trace.Method = "empty"
			trace.MS = time.Since(start).Milliseconds()
			return "Die Datei existiert, ist aber leer (0 Bytes).", trace
		}
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

	case "read_image":
		data, ct, err := d.getfile(relPath)
		if err != nil {
			trace.Error = err.Error()
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: " + err.Error(), trace
		}
		if !strings.HasPrefix(strings.ToLower(ct), "image/") && !looksLikeImage(data) {
			trace.Error = "keine Bilddatei"
			trace.MS = time.Since(start).Milliseconds()
			return "Fehler: keine Bilddatei (nutze read_file für Dokumente)", trace
		}
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
	} `json:"context"`
}

type chatAskResponse struct {
	Answer     string      `json:"answer"`
	ToolTrace  []toolTrace `json:"tool_trace"`
	Iterations int         `json:"iterations"`
	Model      string      `json:"model"`
	Error      string      `json:"error,omitempty"`
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
	if req.Context.Share.Token == "" || req.Context.Share.Password == "" {
		writeChatError(w, http.StatusBadRequest, "context.share (token + password) erforderlich")
		return
	}
	if len(req.Messages) == 0 {
		writeChatError(w, http.StatusBadRequest, "messages erforderlich")
		return
	}

	model := req.Model
	if model == "" {
		model = s.cfg.Chat.DefaultModel
	}

	tools := s.chatTools()

	// System-Prompt (serverseitig, deterministisch)
	folder := req.Context.FolderName
	if folder == "" {
		folder = "der geteilte Ordner"
	}
	sysPrompt := fmt.Sprintf(
		"Du bist ein Assistent, der mit dem Inhalt des Cloud-Ordners „%s“ arbeitet. "+
			"Du kannst dessen Inhalte mit den Tools list_directory (Verzeichnisinhalt), "+
			"read_file (Dateitext, auch PDF/Office/SVG) und read_image (Bildbeschreibung) lesen. "+
			"Pfade sind immer relativ zum Ordner (leerer Pfad = der Ordner selbst). "+
			"Beantworte auf Basis dessen, was du tatsächlich aus den Dateien erlesen hast. "+
			"Lies nur Dateien, die für die Frage relevant sind. "+
			"Wenn eine Datei gekürzt wurde (Kürzungs-Hinweis im Tool-Ergebnis), erwähne in der Antwort "+
			"explizit, dass dieses Dokument nur teilweise ausgewertet wurde. "+
			"Wenn du alle nötigen Informationen hast, antworte direkt und strukturiert.",
		folder)

	messages := make([]chatToolMessage, 0, len(req.Messages)+4)
	messages = append(messages, chatToolMessage{Role: "system", Content: strPtr(sysPrompt)})
	for _, m := range req.Messages {
		role := m.Role
		if role != "user" && role != "assistant" {
			role = "user"
		}
		messages = append(messages, chatToolMessage{Role: role, Content: strPtr(m.Content)})
	}

	d := newShareWebDav(s, req.Context.Share.Token, req.Context.Share.Password)
	toolTrace := make([]toolTrace, 0, 8)
	answer := ""
	iterations := 0

	log.Printf("chat/ask: folder=%q model=%s messages=%d", folder, model, len(req.Messages))

	for i := 0; i < s.cfg.Chat.MaxIterations; i++ {
		iterations = i + 1
		msg, finishReason, err := s.llmChatTools(model, messages, tools)
		if err != nil {
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
			result, trace := s.runChatTool(d, tc.Function.Name, tc.Function.Arguments)
			toolTrace = append(toolTrace, trace)
			log.Printf("chat/ask: tool=%s path=%q ms=%d chars=%d truncated=%v err=%q",
				tc.Function.Name, trace.Path, trace.MS, trace.Chars, trace.Truncated, trace.Error)
			messages = append(messages, chatToolMessage{
				Role:       "tool",
				Content:    strPtr(result),
				ToolCallID: tc.ID,
			})
		}

		// Wenn das Modell nach dem letzten Tool-Block nichts mehr sagt
		// und Iterationen aufgebraucht sind, Antwort unten zusammenführen.
		if content != "" && len(msg.ToolCalls) == 0 {
			answer = content
			break
		}
	}

	if answer == "" && iterations > 0 {
		answer = "Maximalzahl an Tool-Schritten erreicht, ohne finale Antwort. Bitte Frage konkreter stellen."
	}

	resp := chatAskResponse{
		Answer:     answer,
		ToolTrace:  toolTrace,
		Iterations: iterations,
		Model:      model,
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
