// open_taki — lightweight Tika replacement with LLM-powered extraction
//
// Drop-in replacement for Apache Tika with protocol extension.
//
// Endpoints:
//   PUT /tika/text       → plain text extraction (Tika-compatible)
//   PUT /rmeta/text      → metadata + content (Tika MetaRecursive compatible)
//   GET /tika            → health check
//   GET /version         → version info
//
// Protocol extension (X-Taki-Protocol: v1):
//   Request headers:
//     X-Taki-Protocol: v1
//     X-Taki-Features: meta,entities,summary,embedding,transcription
//   Response: structured JSON with requested features
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const version = "0.9.0"

type Config struct {
	Listen string `yaml:"listen"`
	LLM    struct {
		APIBase   string `yaml:"api_base"`
		Model     string `yaml:"model"`
		MaxTokens int    `yaml:"max_tokens"`
	} `yaml:"llm"`
	Whisper struct {
		APIBase string `yaml:"api_base"`
		Model   string `yaml:"model"`
	} `yaml:"whisper"`
	Embedding struct {
		APIBase string `yaml:"api_base"`
		Model   string `yaml:"model"`
	} `yaml:"embedding"`
	Vector struct {
		URL        string `yaml:"url"`
		Collection string `yaml:"collection"`
	} `yaml:"vector"`
	Collabora struct {
		URL      string `yaml:"url"`      // e.g. http://collabora:9980
		Insecure bool   `yaml:"insecure"` // skip TLS verification
	} `yaml:"collabora"`
	PDF struct {
		DPI      int `yaml:"dpi"`
		MaxPages int `yaml:"max_pages"`
	} `yaml:"pdf"`
	Fallback struct {
		MinChars        int `yaml:"min_chars"`
		MinCharsPerPage int `yaml:"min_chars_per_page"`
	} `yaml:"fallback"`
	Routing RoutingConfig `yaml:"routing"`
	DocMeta DocMetaConfig `yaml:"docmeta"`
}

// DocMetaConfig controls structured metadata extraction from documents.
type DocMetaConfig struct {
	Enabled          bool     `yaml:"enabled"`
	ModelVersion     string   `yaml:"model_version"`      // doctype model version (e.g. "qwen3-doctype-1.0")
	RescuePass       bool     `yaml:"rescue_pass"`
	RequiredFields   []string `yaml:"required_fields"`    // fields that trigger rescue if null
	SchemaFile       string   `yaml:"schema_file"`        // path to guided_json schema (default: /etc/open_taki/docmeta_schema.json)
	PromptFile       string   `yaml:"prompt_file"`        // path to vision prompt (default: /etc/open_taki/docmeta_prompt.txt)
	RescuePromptFile string   `yaml:"rescue_prompt_file"` // path to rescue prompt (default: /etc/open_taki/docmeta_rescue_prompt.txt)

	// Loaded at startup (not from YAML)
	schema       json.RawMessage
	prompt       string
	rescuePrompt string
}

// ── Routing ──────────────────────────────────────────────────
//
// Principle:  * → vectordb (always),  bleve → selective per rules

type RoutingConfig struct {
	Vector     string       `yaml:"vector"`      // "always" or ""
	BleveRules []BleveRule  `yaml:"bleve_rules"`
}

type BleveRule struct {
	Match    BleveMatch `yaml:"match"`
	Features []string   `yaml:"features"`
}

type BleveMatch struct {
	Mime      []string `yaml:"mime"`
	SpaceType []string `yaml:"space_type"`
}

// resolveRoute determines features and targets for a given content type + space type.
// Returns: features to extract, whether content goes to bleve, always vector.
func (cfg *Config) resolveRoute(contentType, spaceType string) (features map[string]bool, bleveContent bool) {
	features = map[string]bool{}
	bleveContent = false

	ct := strings.ToLower(contentType)
	st := strings.ToLower(spaceType)

	for _, rule := range cfg.Routing.BleveRules {
		if rule.matches(ct, st) {
			for _, f := range rule.Features {
				features[f] = true
			}
			bleveContent = true
			return
		}
	}
	return
}

func (r *BleveRule) matches(ct, spaceType string) bool {
	mimeOK := len(r.Match.Mime) == 0
	for _, pattern := range r.Match.Mime {
		pattern = strings.ToLower(pattern)
		if pattern == "*" {
			mimeOK = true
			break
		}
		if strings.HasSuffix(pattern, "*") {
			if strings.HasPrefix(ct, strings.TrimSuffix(pattern, "*")) {
				mimeOK = true
				break
			}
		}
		if strings.Contains(ct, pattern) {
			mimeOK = true
			break
		}
	}

	spaceOK := len(r.Match.SpaceType) == 0
	for _, st := range r.Match.SpaceType {
		if strings.ToLower(st) == spaceType {
			spaceOK = true
			break
		}
	}

	return mimeOK && spaceOK
}

type Server struct {
	cfg    Config
	client *http.Client

	// In-flight LLM request tracking
	llmMu       sync.Mutex
	llmInFlight int
	llmOldest   time.Time // zero if no requests in flight
}

// OpenAI chat completion request/response
type chatRequest struct {
	Model          string          `json:"model"`
	Messages       []chatMessage   `json:"messages"`
	MaxTokens      int             `json:"max_tokens"`
	Temp           float64         `json:"temperature"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
}

// responseFormat for structured output (OpenAI json_schema format, supported by vLLM)
type responseFormat struct {
	Type       string            `json:"type"`                  // "json_schema"
	JSONSchema *jsonSchemaRef    `json:"json_schema,omitempty"` // wrapper with name + schema
}

type jsonSchemaRef struct {
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
}

type chatMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
}

type contentBlock struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *imageURL `json:"image_url,omitempty"`
}

type imageURL struct {
	URL string `json:"url"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

type whisperResponse struct {
	Text string `json:"text"`
}

// ── Response types ───────────────────────────────────────────

// Classic Tika MetaRecursive response (array of metadata maps)
// Used by go-tika library: PUT /rmeta/text
type tikaMetaResponse []map[string]interface{}

// Extended Taki v1 response
type takiResponse struct {
	// Tika-compatible fields
	Content     string `json:"X-TIKA:content"`
	ContentType string `json:"Content-Type"`

	// Taki method info
	Method string `json:"X-TAKI:method"`

	// Routing decision (tells caller where to store what)
	Routing *takiRouting `json:"X-TAKI:routing,omitempty"`

	// Extended fields (only if X-Taki-Protocol: v1)
	Meta     *takiMeta     `json:"X-TAKI:meta,omitempty"`
	Entities []takiEntity  `json:"X-TAKI:entities,omitempty"`
	Summary  string        `json:"X-TAKI:summary,omitempty"`
	Embed    []float64     `json:"X-TAKI:embedding,omitempty"`
	Audio    *takiAudio    `json:"X-TAKI:audio,omitempty"`
	Transcr  string        `json:"X-TAKI:transcription,omitempty"`
	DocMeta  *takiDocMeta  `json:"X-TAKI:docmeta,omitempty"`
}

type takiRouting struct {
	// Where to store content: bleve, vector, both, none
	ContentTarget string `json:"content_target"`
	// Meta always goes to bleve index
	MetaTarget    string `json:"meta_target"`
	// Embedding always goes to vector (with source reference)
	VectorTarget  string `json:"vector_target"`
	// Source reference for vector DB lookback
	SourceRef     string `json:"source_ref,omitempty"`
}

type takiMeta struct {
	Title    string `json:"title,omitempty"`
	Author   string `json:"author,omitempty"`
	Created  string `json:"created,omitempty"`
	Language string `json:"language,omitempty"`
	Pages    int    `json:"pages,omitempty"`
	DocType  string `json:"doc_type,omitempty"`
}

type takiEntity struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type takiAudio struct {
	Title    string  `json:"title,omitempty"`
	Artist   string  `json:"artist,omitempty"`
	Album    string  `json:"album,omitempty"`
	Genre    string  `json:"genre,omitempty"`
	Duration float64 `json:"duration,omitempty"`
	Year     int     `json:"year,omitempty"`
}

// ── DocMeta types (structured document metadata from letterhead) ──

// takiDocMeta is a dynamic map — schema defines the structure, not Go types.
// This allows adding fields in docmeta_schema.json without code changes.
type takiDocMeta map[string]interface{}

// loadDocMetaFiles loads schema, prompt and rescue prompt from external files.
// Falls back to built-in defaults if files are not found.
func (cfg *Config) loadDocMetaFiles(configDir string) {
	if !cfg.DocMeta.Enabled {
		return
	}

	// Defaults for file paths
	if cfg.DocMeta.SchemaFile == "" {
		cfg.DocMeta.SchemaFile = filepath.Join(configDir, "docmeta_schema.json")
	}
	if cfg.DocMeta.PromptFile == "" {
		cfg.DocMeta.PromptFile = filepath.Join(configDir, "docmeta_prompt.txt")
	}
	if cfg.DocMeta.RescuePromptFile == "" {
		cfg.DocMeta.RescuePromptFile = filepath.Join(configDir, "docmeta_rescue_prompt.txt")
	}

	// Load schema
	if data, err := os.ReadFile(cfg.DocMeta.SchemaFile); err == nil {
		// Validate it's valid JSON
		var test json.RawMessage
		if json.Unmarshal(data, &test) == nil {
			cfg.DocMeta.schema = data
			log.Printf("  DocMeta schema: %s (%d bytes)", cfg.DocMeta.SchemaFile, len(data))
		} else {
			log.Fatalf("docmeta: invalid JSON in %s: %v", cfg.DocMeta.SchemaFile, err)
		}
	} else {
		log.Printf("  DocMeta schema: using built-in default (%s not found)", cfg.DocMeta.SchemaFile)
		cfg.DocMeta.schema = docMetaDefaultSchema
	}

	// Load prompt
	if data, err := os.ReadFile(cfg.DocMeta.PromptFile); err == nil {
		cfg.DocMeta.prompt = strings.TrimSpace(string(data))
		log.Printf("  DocMeta prompt: %s (%d chars)", cfg.DocMeta.PromptFile, len(cfg.DocMeta.prompt))
	} else {
		log.Printf("  DocMeta prompt: using built-in default (%s not found)", cfg.DocMeta.PromptFile)
		cfg.DocMeta.prompt = docMetaDefaultPrompt
	}

	// Load rescue prompt
	if data, err := os.ReadFile(cfg.DocMeta.RescuePromptFile); err == nil {
		cfg.DocMeta.rescuePrompt = strings.TrimSpace(string(data))
		log.Printf("  DocMeta rescue: %s (%d chars)", cfg.DocMeta.RescuePromptFile, len(cfg.DocMeta.rescuePrompt))
	} else {
		log.Printf("  DocMeta rescue: using built-in default (%s not found)", cfg.DocMeta.RescuePromptFile)
		cfg.DocMeta.rescuePrompt = docMetaDefaultRescuePrompt
	}
}

// Built-in defaults (used when external files are not found)
var docMetaDefaultSchema = json.RawMessage(`{"type":"object","additionalProperties":false,"required":["doc","sender","uncertain"],"properties":{"doc":{"type":"object","additionalProperties":false,"required":["subject","subject_inferred","type","date","reference"],"properties":{"subject":{"type":["string","null"]},"subject_inferred":{"type":"boolean"},"type":{"type":["string","null"],"enum":["brief","bescheid","verfuegung","satzung","niederschrift","antrag","vertrag","mitteilung","einladung","kuendigung","angebot","rechnung","lieferschein","kontoauszug","kassenbon","ec_bon","mahnung","gutschrift","urkunde","zeugnis","foto","skizze","tabelle","praesentation","email","sonstiges",null]},"date":{"type":["string","null"],"pattern":"^\\d{4}-\\d{2}-\\d{2}$"},"reference":{"type":["string","null"]}}},"sender":{"type":"object","additionalProperties":false,"required":["company","given_name","family_name","street","house_number","postal_code","sub_locality","city","country","email","phone"],"properties":{"company":{"type":["string","null"]},"given_name":{"type":["string","null"]},"family_name":{"type":["string","null"]},"street":{"type":["string","null"]},"house_number":{"type":["string","null"]},"postal_code":{"type":["string","null"]},"sub_locality":{"type":["string","null"]},"city":{"type":["string","null"]},"country":{"type":["string","null"],"pattern":"^[A-Z]{2}$"},"email":{"type":["string","null"]},"phone":{"type":["string","null"]}}},"recipient":{"type":"object","additionalProperties":false,"required":["company","given_name","family_name"],"properties":{"company":{"type":["string","null"]},"given_name":{"type":["string","null"]},"family_name":{"type":["string","null"]}}},"amounts":{"type":"object","additionalProperties":false,"required":["total","tax","currency","payment_due"],"properties":{"total":{"type":["string","null"]},"tax":{"type":["string","null"]},"currency":{"type":["string","null"],"pattern":"^[A-Z]{3}$"},"payment_due":{"type":["string","null"],"pattern":"^\\d{4}-\\d{2}-\\d{2}$"}}},"uncertain":{"type":"array","items":{"type":"string"}}}}`)

const docMetaDefaultPrompt = "Du bist ein Extraktionssystem. Extrahiere Metadaten als JSON. Gib NUR das JSON zurück."

const docMetaDefaultRescuePrompt = "Bestimme die offenen Felder aus dem OCR-Text. Gib NUR JSON zurück.\n\nOFFENE FELDER:\n%s\n\nBEREITS EXTRAHIERT:\n%s\n\nOCR-TEXT:\n%s"

// ── Config ───────────────────────────────────────────────────

func loadConfig(path string) Config {
	var cfg Config
	cfg.Listen = ":9998"
	cfg.LLM.APIBase = "http://localhost:8012/v1"
	cfg.LLM.Model = "local"
	cfg.LLM.MaxTokens = 4096
	cfg.Whisper.APIBase = ""
	cfg.Whisper.Model = "whisper-large-v3"
	cfg.PDF.DPI = 150
	cfg.PDF.MaxPages = 20
	cfg.Fallback.MinChars = 200
	cfg.Fallback.MinCharsPerPage = 50
	cfg.DocMeta.Enabled = true
	cfg.DocMeta.ModelVersion = "qwen3-doctype-1.0"
	cfg.DocMeta.RescuePass = true
	cfg.DocMeta.RequiredFields = []string{"doc.date", "sender.company"}

	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("config %s not found, using defaults", path)
		cfg.loadDocMetaFiles(filepath.Dir(path))
		return cfg
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("config parse error: %v", err)
	}
	cfg.loadDocMetaFiles(filepath.Dir(path))
	return cfg
}

func NewServer(cfg Config) *Server {
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	if cfg.Collabora.Insecure {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	return &Server{
		cfg:    cfg,
		client: client,
	}
}

// ── Main handlers ────────────────────────────────────────────

// handleTikaText handles PUT /tika/text — plain text extraction
func (s *Server) handleTikaText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 200*1024*1024))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	ct := r.Header.Get("Content-Type")
	text, method := s.extract(body, ct)
	log.Printf("/tika/text: %d chars via %s (%s)", len(text), method, ct)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(text))
}

// handleRmetaText handles PUT /rmeta/text — Tika MetaRecursive compatible
// This is what OpenCloud's go-tika library actually calls.
// Without X-Taki-Protocol: returns classic Tika metadata array
// With X-Taki-Protocol: v1: returns extended structured response
func (s *Server) handleRmetaText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 200*1024*1024))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	ct := r.Header.Get("Content-Type")
	protocol := r.Header.Get("X-Taki-Protocol")
	features := parseFeatures(r.Header.Get("X-Taki-Features"))

	text, method := s.extract(body, ct)
	log.Printf("/rmeta/text: %d chars via %s (%s) proto=%s", len(text), method, ct, protocol)

	w.Header().Set("Content-Type", "application/json")

	if protocol == "v2" {
		// Resolve routing from config
		spaceType := r.Header.Get("X-Taki-Space-Type")
		routedFeatures, bleveContent := s.cfg.resolveRoute(ct, spaceType)

		// Header overrides config features
		if len(features) > 0 {
			routedFeatures = features
		}

		// Invariants: meta always, embedding always
		routedFeatures["meta"] = true
		routedFeatures["embedding"] = true

		contentTarget := "vector"
		if bleveContent {
			contentTarget = "both"
		}

		resp := takiResponse{
			Content:     text,
			ContentType: ct,
			Method:      method,
			Routing: &takiRouting{
				ContentTarget: contentTarget,
				MetaTarget:    "bleve",
				VectorTarget:  "vector",
				SourceRef:     r.Header.Get("X-Taki-Source-Ref"),
			},
		}

		// DocMeta extraction — structured metadata from letterhead (BEFORE general enrichment)
		if routedFeatures["docmeta"] && s.cfg.DocMeta.Enabled {
			resp.DocMeta = s.extractDocMeta(body, ct, text)
		}

		// LLM enrichment
		if routedFeatures["meta"] || routedFeatures["entities"] || routedFeatures["summary"] {
			s.enrichWithLLM(&resp, text, ct, routedFeatures)
		}

		if routedFeatures["transcription"] && (strings.HasPrefix(ct, "audio/") || strings.HasPrefix(ct, "video/")) {
			resp.Transcr = text
		}

		if routedFeatures["embedding"] {
			resp.Embed = s.getEmbedding(text)
		}

		log.Printf("  routing: content→%s bleve=%v space=%s features=%v",
			contentTarget, bleveContent, spaceType, routedFeatures)

		json.NewEncoder(w).Encode([]takiResponse{resp})
	} else {
		// Classic Tika MetaRecursive response
		meta := map[string]interface{}{
			"X-TIKA:content": text,
			"Content-Type":   ct,
			"X-TAKI:method":  method,
		}

		// Add basic Tika-compatible metadata for images
		if strings.HasPrefix(ct, "image/") {
			// Tika returns tiff:ImageWidth etc — we could parse from image
			// but for now just pass content
		}

		json.NewEncoder(w).Encode(tikaMetaResponse{meta})
	}
}

func parseFeatures(header string) map[string]bool {
	features := map[string]bool{}
	if header == "" {
		return features
	}
	for _, f := range strings.Split(header, ",") {
		features[strings.TrimSpace(f)] = true
	}
	return features
}

// enrichWithLLM asks the LLM to extract structured metadata from content
func (s *Server) enrichWithLLM(resp *takiResponse, content, ct string, features map[string]bool) {
	if content == "" || len(content) < 50 {
		return
	}

	// Truncate for LLM context
	truncated := content
	if len(truncated) > 8000 {
		truncated = truncated[:8000]
	}

	var prompt strings.Builder
	prompt.WriteString("Analyze this document content and return a JSON object with the following fields:\n")

	if features["meta"] {
		prompt.WriteString(`"meta": {"title": "...", "author": "...", "created": "YYYY-MM-DD or empty", "language": "de/en/...", "doc_type": "contract/invoice/letter/report/photo/other"}` + "\n")
	}
	if features["entities"] {
		prompt.WriteString(`"entities": [{"type": "person/org/location/date/id/amount", "value": "..."}] (max 20 entities)` + "\n")
	}
	if features["summary"] {
		prompt.WriteString(`"summary": "1-2 sentence summary in the document's language"` + "\n")
	}

	prompt.WriteString("\nDocument content:\n")
	prompt.WriteString(truncated)
	prompt.WriteString("\n\nReturn ONLY valid JSON, no explanation.")

	llmResp := s.llmChat(prompt.String())
	if llmResp == "" {
		return
	}

	// Parse LLM JSON response
	// Find the JSON object in the response
	jsonStr := extractJSON(llmResp)
	if jsonStr == "" {
		return
	}

	var parsed struct {
		Meta     *takiMeta    `json:"meta"`
		Entities []takiEntity `json:"entities"`
		Summary  string       `json:"summary"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		log.Printf("LLM JSON parse error: %v", err)
		return
	}

	if features["meta"] && parsed.Meta != nil {
		resp.Meta = parsed.Meta
	}
	if features["entities"] {
		resp.Entities = parsed.Entities
	}
	if features["summary"] {
		resp.Summary = parsed.Summary
	}
}

// ── DocMeta extraction (structured metadata from document letterhead) ──
//
// takiDocMeta is map[string]interface{} — all field access is dynamic.
// Helper functions use dot-notation paths like "doc.subject" or "sender.company".

// dmGet retrieves a nested value from a docmeta map using dot-notation (e.g. "doc.subject").
func dmGet(dm takiDocMeta, path string) interface{} {
	parts := strings.SplitN(path, ".", 2)
	val, ok := dm[parts[0]]
	if !ok || val == nil {
		return nil
	}
	if len(parts) == 1 {
		return val
	}
	sub, ok := val.(map[string]interface{})
	if !ok {
		return nil
	}
	return sub[parts[1]]
}

// dmGetStr retrieves a string value, returns "" if null/missing.
func dmGetStr(dm takiDocMeta, path string) string {
	v := dmGet(dm, path)
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// dmGetBool retrieves a bool value, returns false if missing.
func dmGetBool(dm takiDocMeta, path string) bool {
	v := dmGet(dm, path)
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

// dmGetStrSlice retrieves a string slice (e.g. "uncertain").
func dmGetStrSlice(dm takiDocMeta, path string) []string {
	v := dmGet(dm, path)
	if v == nil {
		return nil
	}
	if arr, ok := v.([]interface{}); ok {
		var result []string
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

// dmSet sets a nested value using dot-notation. Creates intermediate maps as needed.
func dmSet(dm takiDocMeta, path string, val interface{}) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		dm[parts[0]] = val
		return
	}
	sub, ok := dm[parts[0]].(map[string]interface{})
	if !ok {
		sub = map[string]interface{}{}
		dm[parts[0]] = sub
	}
	sub[parts[1]] = val
}

// dmIsNull checks if a dot-notation field is null or missing.
func dmIsNull(dm takiDocMeta, path string) bool {
	return dmGet(dm, path) == nil
}

// docMetaFallback tries a text-based LLM pass when vision is unavailable.
// Returns nil if LLM is down (no fake metadata).
// Returns "none" placeholders if LLM answered but couldn't extract anything useful.
func (s *Server) docMetaFallback(reason string, fulltext string) *takiDocMeta {
	if fulltext == "" || len(fulltext) <= 20 {
		log.Printf("docmeta: no extraction possible (%s, no fulltext)", reason)
		return nil
	}

	dm, llmReached := s.docMetaTextPass(fulltext)
	if dm != nil {
		(*dm)["source"] = "text"
		if s.cfg.DocMeta.ModelVersion != "" {
			(*dm)["model"] = s.cfg.DocMeta.ModelVersion
		}
		log.Printf("docmeta: text pass (%s) → type=%s subject=%s",
			reason, dmGetStr(*dm, "doc.type"), dmGetStr(*dm, "doc.subject"))
		return dm
	}

	if !llmReached {
		// LLM down — no metadata at all
		log.Printf("docmeta: no extraction possible (%s, LLM unavailable)", reason)
		return nil
	}

	// LLM answered but extraction failed — provide "none" for manual editing
	log.Printf("docmeta: LLM reached but extraction empty (%s) → none placeholders", reason)
	fallback := takiDocMeta{
		"doc": map[string]interface{}{
			"subject":          "none",
			"subject_inferred": false,
			"type":             "sonstiges",
			"date":             nil,
			"reference":        nil,
		},
		"sender": map[string]interface{}{
			"company":      "none",
			"given_name":   nil,
			"family_name":  nil,
			"street":       nil,
			"house_number": nil,
			"postal_code":  nil,
			"sub_locality": nil,
			"city":         nil,
			"country":      nil,
			"email":        nil,
			"phone":        nil,
		},
		"uncertain": []interface{}{},
		"source":    "none",
	}
	if s.cfg.DocMeta.ModelVersion != "" {
		fallback["model"] = s.cfg.DocMeta.ModelVersion
	}
	return &fallback
}

// docMetaTextPass extracts metadata from fulltext (no vision) using guided_json.
// Returns (result, llmReached): result is the parsed docmeta, llmReached indicates
// whether the LLM responded at all (false = network/timeout error).
func (s *Server) docMetaTextPass(fulltext string) (*takiDocMeta, bool) {
	truncated := fulltext
	if len(truncated) > 4000 {
		truncated = truncated[:4000]
	}

	prompt := "Du bist ein Klassifikationssystem. Analysiere den folgenden Text und extrahiere Metadaten als JSON.\n\n" +
		"REGELN:\n" +
		"1. doc.type: Bestimme den Dokumenttyp. Im Zweifel \"sonstiges\".\n" +
		"2. doc.subject: Fasse den Inhalt in max. 10 Wörtern zusammen. Setze subject_inferred auf true.\n" +
		"3. doc.date: Dokumentdatum als YYYY-MM-DD, falls erkennbar. Sonst null.\n" +
		"4. sender: Nur setzen wenn ein Absender klar erkennbar ist. Sonst null.\n" +
		"5. Felder ohne Beleg sind null.\n" +
		"6. Gib NUR das JSON zurück.\n\n" +
		"TEXT:\n" + truncated

	messages := []chatMessage{{Role: "user", Content: prompt}}
	result := s.llmCompleteStructured(messages, s.cfg.DocMeta.schema)
	if result == "" {
		return nil, false // LLM not reached
	}

	var dm takiDocMeta
	if err := json.Unmarshal([]byte(result), &dm); err != nil {
		log.Printf("docmeta: text pass JSON parse error: %v (raw: %.200s)", err, result)
		return nil, true // LLM answered but unparsable
	}
	return &dm, true
}

// extractDocMeta extracts structured metadata from a document.
func (s *Server) extractDocMeta(body []byte, ct string, fulltext string) *takiDocMeta {
	if !s.cfg.DocMeta.Enabled {
		return nil
	}

	var dataURL string

	switch {
	case strings.Contains(strings.ToLower(ct), "pdf"):
		imgData := s.pdfPageToImage(body, 1)
		if imgData == nil {
			log.Printf("docmeta: failed to render PDF page 1")
			return s.docMetaFallback("pdf render failed", fulltext)
		}
		dataURL = "data:image/png;base64," + base64.StdEncoding.EncodeToString(imgData)

	case strings.HasPrefix(strings.ToLower(ct), "image/"):
		mediaType := "image/png"
		lct := strings.ToLower(ct)
		for _, t := range []string{"jpeg", "jpg", "webp", "gif", "tiff"} {
			if strings.Contains(lct, t) {
				if t == "jpg" {
					mediaType = "image/jpeg"
				} else {
					mediaType = "image/" + t
				}
				break
			}
		}
		dataURL = "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(body)

	default:
		log.Printf("docmeta: non-visual content type %s, using text pass", ct)
		return s.docMetaFallback("non-visual "+ct, fulltext)
	}

	// Pass 1: Vision extraction with guided_json
	dm := s.docMetaVisionPass(dataURL)
	if dm == nil {
		return s.docMetaFallback("vision failed", fulltext)
	}
	(*dm)["source"] = "vision"

	// If not a formal document type, try page 2 (cover sheet → content page)
	if !isDocMetaFormal(dmGetStr(*dm, "doc.type")) && strings.Contains(strings.ToLower(ct), "pdf") {
		imgData2 := s.pdfPageToImage(body, 2)
		if imgData2 != nil {
			dataURL2 := "data:image/png;base64," + base64.StdEncoding.EncodeToString(imgData2)
			dm2 := s.docMetaVisionPass(dataURL2)
			if dm2 != nil && isDocMetaFormal(dmGetStr(*dm2, "doc.type")) {
				dm = dm2
				(*dm)["source"] = "vision_p2"
			}
		}
	}

	// Rescue pass
	if s.cfg.DocMeta.RescuePass && fulltext != "" && s.docMetaNeedsRescue(*dm) {
		rescued := s.docMetaRescuePass(*dm, fulltext)
		if rescued != nil {
			dm = rescued
		}
	}

	if s.cfg.DocMeta.ModelVersion != "" {
		(*dm)["model"] = s.cfg.DocMeta.ModelVersion
	}

	log.Printf("docmeta: source=%s type=%s date=%s sender=%s uncertain=%d model=%s",
		dmGetStr(*dm, "source"),
		dmGetStr(*dm, "doc.type"), dmGetStr(*dm, "doc.date"),
		dmGetStr(*dm, "sender.company"), len(dmGetStrSlice(*dm, "uncertain")),
		s.cfg.DocMeta.ModelVersion)

	return dm
}

// docMetaVisionPass sends a single page image to the LLM with guided_json schema.
func (s *Server) docMetaVisionPass(dataURL string) *takiDocMeta {
	content := []contentBlock{
		{Type: "text", Text: s.cfg.DocMeta.prompt},
		{Type: "image_url", ImageURL: &imageURL{URL: dataURL}},
	}

	messages := []chatMessage{{Role: "user", Content: content}}
	result := s.llmCompleteStructured(messages, s.cfg.DocMeta.schema)
	if result == "" {
		return nil
	}

	var dm takiDocMeta
	if err := json.Unmarshal([]byte(result), &dm); err != nil {
		log.Printf("docmeta: vision pass JSON parse error: %v (raw: %.200s)", err, result)
		return nil
	}
	return &dm
}

// pdfPageToImage renders a single PDF page to a PNG image.
func (s *Server) pdfPageToImage(pdfData []byte, pageNum int) []byte {
	tmpDir, err := os.MkdirTemp("", "taki-docmeta-*")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "page")
	first := fmt.Sprintf("%d", pageNum)
	last := first

	cmd := exec.Command("pdftoppm", "-png", "-r",
		fmt.Sprintf("%d", s.cfg.PDF.DPI),
		"-f", first, "-l", last,
		"-", prefix)
	cmd.Stdin = bytes.NewReader(pdfData)
	if err := cmd.Run(); err != nil {
		return nil
	}

	pages, _ := filepath.Glob(prefix + "-*.png")
	if len(pages) == 0 {
		return nil
	}

	imgData, err := os.ReadFile(pages[0])
	if err != nil {
		return nil
	}
	return imgData
}

// isDocMetaFormal returns true for document types that have formal structure
// (letterhead, sender, dates) — triggers page-2 and rescue logic.
func isDocMetaFormal(docType string) bool {
	switch docType {
	case "brief", "bescheid", "verfuegung", "satzung", "niederschrift",
		"antrag", "vertrag", "mitteilung", "einladung", "kuendigung",
		"angebot", "rechnung", "lieferschein", "mahnung", "gutschrift",
		"urkunde", "zeugnis":
		return true
	}
	return false
}

// docMetaNeedsRescue checks if required fields are missing or uncertain.
func (s *Server) docMetaNeedsRescue(dm takiDocMeta) bool {
	if !isDocMetaFormal(dmGetStr(dm, "doc.type")) {
		return false
	}
	if len(dmGetStrSlice(dm, "uncertain")) > 0 {
		return true
	}
	for _, field := range s.cfg.DocMeta.RequiredFields {
		if dmIsNull(dm, field) {
			return true
		}
	}
	return false
}

// ── Rescue pass (text-based metadata recovery) ──────────────

// docMetaRescuePass attempts to fill missing fields from OCR fulltext.
func (s *Server) docMetaRescuePass(dm takiDocMeta, fulltext string) *takiDocMeta {
	missing := s.docMetaMissingFields(dm)
	if len(missing) == 0 {
		return nil
	}

	knownJSON, _ := json.MarshalIndent(dm, "", "  ")

	text := fulltext
	if len(text) > 12000 {
		text = text[:12000]
	}

	prompt := fmt.Sprintf(s.cfg.DocMeta.rescuePrompt,
		strings.Join(missing, ", "),
		string(knownJSON),
		text)

	result := s.llmChat(prompt)
	if result == "" {
		return nil
	}

	jsonStr := extractJSON(result)
	if jsonStr == "" {
		return nil
	}

	var rescue struct {
		Fields    map[string]interface{} `json:"fields"`
		Evidence  map[string]string      `json:"evidence"`
		Uncertain []string               `json:"uncertain"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &rescue); err != nil {
		log.Printf("docmeta: rescue parse error: %v", err)
		return nil
	}

	// Merge: pass 1 wins on secure values, rescue fills gaps
	merged := make(takiDocMeta)
	// Deep copy original
	origJSON, _ := json.Marshal(dm)
	json.Unmarshal(origJSON, &merged)

	mergedAny := false
	for _, field := range missing {
		val, ok := rescue.Fields[field]
		if !ok || val == nil {
			continue
		}
		if strVal, ok := val.(string); ok && strVal != "" {
			dmSet(merged, field, strVal)
			mergedAny = true
		}
	}

	if !mergedAny {
		return nil
	}

	// Merge uncertain lists
	uncertainSet := map[string]bool{}
	for _, u := range dmGetStrSlice(dm, "uncertain") {
		uncertainSet[u] = true
	}
	for _, u := range rescue.Uncertain {
		uncertainSet[u] = true
	}
	var newUncertain []string
	for u := range uncertainSet {
		if !dmIsNull(merged, u) {
			newUncertain = append(newUncertain, u)
		}
	}
	merged["uncertain"] = newUncertain
	merged["source"] = "merged"

	log.Printf("docmeta: rescue pass filled %d fields from fulltext", len(missing))
	return &merged
}

// docMetaMissingFields collects fields from the schema that are null or uncertain.
// Walks "doc.*" and "sender.*" sub-objects dynamically.
func (s *Server) docMetaMissingFields(dm takiDocMeta) []string {
	uncertainSet := map[string]bool{}
	for _, u := range dmGetStrSlice(dm, "uncertain") {
		uncertainSet[u] = true
	}

	var missing []string
	for _, prefix := range []string{"doc", "sender", "recipient", "amounts"} {
		sub, ok := dm[prefix].(map[string]interface{})
		if !ok {
			continue
		}
		for key, val := range sub {
			path := prefix + "." + key
			if val == nil || uncertainSet[path] {
				missing = append(missing, path)
			}
		}
	}
	return missing
}

func extractJSON(s string) string {
	// Find first { and last }
	start := strings.Index(s, "{")
	if start < 0 {
		return ""
	}
	depth := 0
	for i := start; i < len(s); i++ {
		if s[i] == '{' {
			depth++
		} else if s[i] == '}' {
			depth--
			if depth == 0 {
				return s[start : i+1]
			}
		}
	}
	return ""
}

func (s *Server) getEmbedding(text string) []float64 {
	if text == "" || len(text) < 10 {
		return nil
	}

	if len(text) > 4000 {
		text = text[:4000]
	}

	model := s.cfg.Embedding.Model
	if model == "" {
		model = "nomic-ai/nomic-embed-text-v1.5"
	}

	payload := map[string]interface{}{
		"model": model,
		"input": text,
	}
	jsonData, _ := json.Marshal(payload)

	apiBase := s.cfg.Embedding.APIBase
	if apiBase == "" {
		apiBase = s.cfg.LLM.APIBase
	}
	url := strings.TrimRight(apiBase, "/") + "/embeddings"

	resp, err := s.client.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var embResp struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	json.NewDecoder(resp.Body).Decode(&embResp)
	if len(embResp.Data) > 0 {
		return embResp.Data[0].Embedding
	}
	return nil
}

// ── Content extraction ───────────────────────────────────────

func (s *Server) extract(body []byte, ct string) (string, string) {
	ct = strings.ToLower(ct)

	switch {
	// ── PDF ──
	case strings.Contains(ct, "pdf"):
		return s.extractPDF(body)

	// ── Images ──
	case strings.HasPrefix(ct, "image/"):
		return s.extractImage(body, ct)

	// ── Audio ──
	case strings.HasPrefix(ct, "audio/"):
		return s.extractAudio(body, ct)

	// ── Video ──
	case strings.HasPrefix(ct, "video/"):
		return s.extractVideo(body, ct)

	// ── Word processing (DOCX, DOC, ODT, RTF, WordPerfect, Pages) ──
	case strings.Contains(ct, "wordprocessingml"),
		strings.Contains(ct, "msword"),
		strings.Contains(ct, "opendocument.text"),
		strings.Contains(ct, "rtf"),
		strings.Contains(ct, "wordperfect"),
		strings.Contains(ct, "apple.pages"):
		return s.extractOfficeDoc(body)

	// ── Spreadsheets (XLSX, XLS, ODS, Numbers) ──
	case strings.Contains(ct, "spreadsheetml"),
		strings.Contains(ct, "ms-excel"),
		strings.Contains(ct, "opendocument.spreadsheet"),
		strings.Contains(ct, "apple.numbers"):
		return s.extractSpreadsheet(body)

	// ── Presentations (PPTX, PPT, ODP, Keynote) ──
	case strings.Contains(ct, "presentationml"),
		strings.Contains(ct, "ms-powerpoint"),
		strings.Contains(ct, "opendocument.presentation"),
		strings.Contains(ct, "apple.keynote"):
		return s.extractPresentation(body)

	// ── EPUB ──
	case strings.Contains(ct, "epub"):
		return s.extractEpub(body)

	// ── Email (EML, RFC822) ──
	case strings.Contains(ct, "rfc822"),
		strings.Contains(ct, "message/"):
		return s.extractEmail(body)

	// ── Outlook MSG ──
	case strings.Contains(ct, "ms-outlook"),
		strings.Contains(ct, "x-tnef"),
		strings.Contains(ct, "ms-tnef"):
		return s.extractMSG(body)

	// ── Outlook PST (mailbox archive) ──
	case strings.Contains(ct, "outlook-pst"):
		return "[PST mailbox — too large for single extraction]", "skipped"

	// ── HTML / XHTML ──
	case strings.Contains(ct, "text/html"),
		strings.Contains(ct, "xhtml"):
		return s.extractHTML(body)

	// ── XML / SVG ──
	case strings.Contains(ct, "application/xml"),
		strings.Contains(ct, "text/xml"),
		strings.Contains(ct, "image/svg"):
		return s.extractXML(body)

	// ── RSS / Atom feeds ──
	case strings.Contains(ct, "rss+xml"),
		strings.Contains(ct, "atom+xml"):
		return s.extractXML(body)

	// ── Compressed single files (gzip, bz2, xz, zstd) ──
	case strings.Contains(ct, "gzip"),
		strings.Contains(ct, "x-bzip"),
		strings.Contains(ct, "x-xz"),
		strings.Contains(ct, "zstd"),
		strings.Contains(ct, "x-compress"):
		return s.extractCompressed(body, ct)

	// ── Archives (ZIP, 7z, TAR, RAR, JAR) ──
	case strings.Contains(ct, "zip"),
		strings.Contains(ct, "x-tar"),
		strings.Contains(ct, "x-7z"),
		strings.Contains(ct, "x-rar"),
		strings.Contains(ct, "java-archive"):
		return s.extractArchive(body, ct)

	// ── CAD (DWG) — render to image, then LLM Vision ──
	case strings.Contains(ct, "dwg"),
		strings.Contains(ct, "dgn"),
		strings.Contains(ct, "vnd.visio"):
		return s.extractCAD(body, ct)

	// ── Source code ──
	case strings.Contains(ct, "x-java-source"),
		strings.Contains(ct, "x-c++src"),
		strings.Contains(ct, "x-python"),
		strings.Contains(ct, "javascript"),
		strings.Contains(ct, "x-groovy"):
		return string(body), "passthrough"

	// ── Plain text / CSV / TSV / Markdown ──
	case strings.Contains(ct, "text/"):
		return string(body), "passthrough"

	// ── Fallback: magic bytes ──
	default:
		return s.extractByMagic(body)
	}
}

// ── PDF extraction ───────────────────────────────────────────

func (s *Server) extractPDF(data []byte) (string, string) {
	text := s.pdftotext(data)
	if s.isGoodText(text, data) {
		return text, "pdftotext"
	}
	llmText := s.llmOCR(data)
	if llmText != "" {
		return llmText, "llm_ocr"
	}
	if text != "" {
		return text, "pdftotext_partial"
	}
	return "[extraction failed]", "error"
}

func (s *Server) pdftotext(data []byte) string {
	tmp, err := os.CreateTemp("", "taki-*.pdf")
	if err != nil {
		return ""
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	cmd := exec.Command("pdftotext", "-layout", tmp.Name(), "-")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()
	return strings.TrimSpace(out.String())
}

func (s *Server) isGoodText(text string, pdfData []byte) bool {
	if len(text) < s.cfg.Fallback.MinChars {
		return false
	}
	pages := bytes.Count(pdfData, []byte("/Type /Page"))
	if pages < 1 {
		pages = 1
	}
	return len(text)/pages >= s.cfg.Fallback.MinCharsPerPage
}

func (s *Server) llmOCR(data []byte) string {
	tmpDir, err := os.MkdirTemp("", "taki-ocr-*")
	if err != nil {
		return ""
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "page")
	cmd := exec.Command("pdftoppm", "-png", "-r",
		fmt.Sprintf("%d", s.cfg.PDF.DPI), "-", prefix)
	cmd.Stdin = bytes.NewReader(data)
	if err := cmd.Run(); err != nil {
		return ""
	}

	pages, _ := filepath.Glob(prefix + "-*.png")
	if len(pages) == 0 {
		return ""
	}
	sort.Strings(pages)
	if len(pages) > s.cfg.PDF.MaxPages {
		pages = pages[:s.cfg.PDF.MaxPages]
	}

	// Parallel OCR — all pages concurrently (vLLM handles batching)
	results := make([]string, len(pages))
	var wg sync.WaitGroup
	for i, pagePath := range pages {
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()
			results[idx] = s.llmDescribe(path,
				"Extract all text from this scanned document page. "+
					"Return the complete text content, preserving structure. "+
					"If it's a form or contract, extract structured fields.")
		}(i, pagePath)
	}
	wg.Wait()

	var allText []string
	for _, text := range results {
		if text != "" {
			allText = append(allText, text)
		}
	}
	return strings.Join(allText, "\n\n")
}

// ── Image extraction (LLM Vision) ───────────────────────────

func (s *Server) extractImage(data []byte, ct string) (string, string) {
	mediaType := "image/png"
	for _, t := range []string{"jpeg", "jpg", "webp", "gif", "tiff", "bmp"} {
		if strings.Contains(ct, t) {
			if t == "jpg" {
				mediaType = "image/jpeg"
			} else {
				mediaType = "image/" + t
			}
			break
		}
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	text := s.llmVision("data:"+mediaType+";base64,"+b64,
		"Describe this image in detail. "+
			"If it contains text, extract all text. "+
			"If it's a document or scan, extract all structured content. "+
			"If it's a photo, describe what you see.")
	if text != "" {
		return text, "llm_vision"
	}
	return "[image not describable]", "error"
}

// ── Audio extraction (Whisper) ───────────────────────────────

func (s *Server) extractAudio(data []byte, ct string) (string, string) {
	if s.cfg.Whisper.APIBase == "" {
		return "[audio transcription not configured]", "skipped"
	}

	ext := ".wav"
	for _, pair := range [][2]string{{"mp3", ".mp3"}, {"mpeg", ".mp3"}, {"ogg", ".ogg"}, {"flac", ".flac"}, {"m4a", ".m4a"}, {"mp4", ".m4a"}} {
		if strings.Contains(ct, pair[0]) {
			ext = pair[1]
			break
		}
	}

	tmp, err := os.CreateTemp("", "taki-audio-*"+ext)
	if err != nil {
		return "[temp file error]", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	text := s.whisperTranscribe(tmp.Name())
	if text != "" {
		return text, "whisper"
	}
	return "[transcription failed]", "error"
}

func (s *Server) extractVideo(data []byte, ct string) (string, string) {
	if s.cfg.Whisper.APIBase == "" {
		return "[video transcription not configured]", "skipped"
	}

	tmpVideo, err := os.CreateTemp("", "taki-video-*")
	if err != nil {
		return "[temp file error]", "error"
	}
	defer os.Remove(tmpVideo.Name())
	tmpVideo.Write(data)
	tmpVideo.Close()

	tmpAudio := tmpVideo.Name() + ".wav"
	defer os.Remove(tmpAudio)

	cmd := exec.Command("ffmpeg", "-i", tmpVideo.Name(),
		"-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
		"-y", tmpAudio)
	if err := cmd.Run(); err != nil {
		return "[ffmpeg audio extraction failed]", "error"
	}

	text := s.whisperTranscribe(tmpAudio)
	if text != "" {
		return text, "whisper_video"
	}
	return "[video transcription failed]", "error"
}

func (s *Server) whisperTranscribe(audioPath string) string {
	url := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"

	f, err := os.Open(audioPath)
	if err != nil {
		return ""
	}
	defer f.Close()

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteField("model", s.cfg.Whisper.Model)
	w.WriteField("language", "de")
	w.WriteFile("file", filepath.Base(audioPath), f)
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return ""
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("whisper error: %v", err)
		return ""
	}
	defer resp.Body.Close()

	var wr whisperResponse
	json.NewDecoder(resp.Body).Decode(&wr)
	return strings.TrimSpace(wr.Text)
}

// ── Office documents ─────────────────────────────────────────

func (s *Server) extractOfficeDoc(data []byte) (string, string) {
	// 1. Collabora (if configured) — handles DOC, DOCX, ODT, RTF
	if text, ok := s.convertViaCollabora(data, "txt"); ok {
		return text, "collabora"
	}
	// 2. pandoc — handles DOCX, ODT, EPUB, RTF (not DOC)
	return s.convertViaPandoc(data, "docx")
}

func (s *Server) extractSpreadsheet(data []byte) (string, string) {
	// 1. Collabora → CSV
	if text, ok := s.convertViaCollabora(data, "csv"); ok {
		return text, "collabora"
	}
	return "[spreadsheet extraction requires collabora — set collabora.url in config]", "skipped"
}

func (s *Server) extractPresentation(data []byte) (string, string) {
	// 1. Collabora → PDF → pdftotext
	if pdfData, ok := s.convertViaCollaboraRaw(data, "pdf"); ok {
		text := s.pdftotext(pdfData)
		if text != "" {
			return text, "collabora_pdf"
		}
		// Scan-slides: LLM OCR
		llmText := s.llmOCR(pdfData)
		if llmText != "" {
			return llmText, "collabora_pdf_ocr"
		}
	}
	// 2. pandoc (PPTX only, limited)
	return s.convertViaPandoc(data, "pptx")
}

// convertViaCollabora sends data to Collabora's /cool/convert-to endpoint.
// Returns extracted text and true if successful.
func (s *Server) convertViaCollabora(data []byte, targetFormat string) (string, bool) {
	raw, ok := s.convertViaCollaboraRaw(data, targetFormat)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(string(raw)), true
}

// convertViaCollaboraRaw returns raw bytes from Collabora conversion.
func (s *Server) convertViaCollaboraRaw(data []byte, targetFormat string) ([]byte, bool) {
	if s.cfg.Collabora.URL == "" {
		return nil, false
	}

	url := strings.TrimRight(s.cfg.Collabora.URL, "/") + "/cool/convert-to/" + targetFormat

	// Build multipart form
	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteFile("data", "document", bytes.NewReader(data))
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("collabora error: %v", err)
		return nil, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("collabora returned %d", resp.StatusCode)
		return nil, false
	}

	result, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false
	}
	return result, len(result) > 0
}

func (s *Server) convertViaPandoc(data []byte, ext string) (string, string) {
	tmp, err := os.CreateTemp("", "taki-*."+ext)
	if err != nil {
		return "", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	cmd := exec.Command("pandoc", "--to=plain", "--wrap=none", tmp.Name())
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "[conversion failed — install collabora for full office support]", "error"
	}
	text := strings.TrimSpace(out.String())
	if text != "" {
		return text, "pandoc"
	}
	return "[no text extracted]", "error"
}

// ── Email ────────────────────────────────────────────────────

func (s *Server) extractEmail(data []byte) (string, string) {
	msg, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		if utf8.Valid(data) {
			return string(data), "raw_email"
		}
		return "[email parse error]", "error"
	}

	var parts []string
	if subj := msg.Header.Get("Subject"); subj != "" {
		decoded, err := decodeRFC2047(subj)
		if err == nil {
			parts = append(parts, "Subject: "+decoded)
		} else {
			parts = append(parts, "Subject: "+subj)
		}
	}
	if from := msg.Header.Get("From"); from != "" {
		parts = append(parts, "From: "+from)
	}
	if date := msg.Header.Get("Date"); date != "" {
		parts = append(parts, "Date: "+date)
	}
	parts = append(parts, "")

	body, _ := io.ReadAll(msg.Body)
	ct := msg.Header.Get("Content-Type")
	if strings.Contains(ct, "html") {
		text, _ := s.extractHTML(body)
		parts = append(parts, text)
	} else {
		parts = append(parts, string(body))
	}

	return strings.Join(parts, "\n"), "email"
}

func decodeRFC2047(s string) (string, error) {
	dec := new(mime.WordDecoder)
	return dec.DecodeHeader(s)
}

// ── HTML ─────────────────────────────────────────────────────

func (s *Server) extractHTML(data []byte) (string, string) {
	cmd := exec.Command("pandoc", "--from=html", "--to=plain", "--wrap=none")
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return string(data), "raw"
	}
	return strings.TrimSpace(out.String()), "pandoc"
}

// ── EPUB ─────────────────────────────────────────────────────

func (s *Server) extractEpub(data []byte) (string, string) {
	// EPUB is a ZIP with XHTML — pandoc handles it natively
	tmp, err := os.CreateTemp("", "taki-*.epub")
	if err != nil {
		return "", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	cmd := exec.Command("pandoc", "--from=epub", "--to=plain", "--wrap=none", tmp.Name())
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		// Fallback: unzip and extract HTML files
		return s.extractArchive(data, "application/zip")
	}
	text := strings.TrimSpace(out.String())
	if text != "" {
		return text, "pandoc_epub"
	}
	return s.extractArchive(data, "application/zip")
}

// ── Outlook MSG ──────────────────────────────────────────────

func (s *Server) extractMSG(data []byte) (string, string) {
	// MSG is OLE2 compound format — extract printable strings
	var parts []string
	var current []byte
	for _, b := range data {
		if b >= 0x20 && b < 0x7f || b == '\n' || b == '\r' || b == '\t' {
			current = append(current, b)
		} else {
			if len(current) > 20 {
				parts = append(parts, string(current))
			}
			current = current[:0]
		}
	}
	if len(current) > 20 {
		parts = append(parts, string(current))
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n"), "msg_strings"
	}
	return "[MSG not readable]", "error"
}

// ── XML / SVG ────────────────────────────────────────────────

func (s *Server) extractXML(data []byte) (string, string) {
	// Try pandoc for structured XML
	cmd := exec.Command("pandoc", "--from=html", "--to=plain", "--wrap=none")
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err == nil && strings.TrimSpace(out.String()) != "" {
		return strings.TrimSpace(out.String()), "pandoc_xml"
	}
	// Fallback: strip tags manually
	text := stripXMLTags(string(data))
	if text != "" {
		return text, "xml_strip"
	}
	return string(data), "raw"
}

func stripXMLTags(s string) string {
	var result strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			result.WriteRune(' ')
			continue
		}
		if !inTag {
			result.WriteRune(r)
		}
	}
	// Collapse whitespace
	text := result.String()
	for strings.Contains(text, "  ") {
		text = strings.ReplaceAll(text, "  ", " ")
	}
	return strings.TrimSpace(text)
}

// ── Compressed single files ──────────────────────────────────

func (s *Server) extractCompressed(data []byte, ct string) (string, string) {
	tmpDir, _ := os.MkdirTemp("", "taki-decompress-*")
	defer os.RemoveAll(tmpDir)

	compFile := filepath.Join(tmpDir, "compressed")
	outFile := filepath.Join(tmpDir, "decompressed")
	os.WriteFile(compFile, data, 0644)

	var cmd *exec.Cmd
	switch {
	case strings.Contains(ct, "gzip"):
		cmd = exec.Command("sh", "-c", fmt.Sprintf("gunzip -c %s > %s", compFile, outFile))
	case strings.Contains(ct, "x-bzip"):
		cmd = exec.Command("sh", "-c", fmt.Sprintf("bzcat %s > %s", compFile, outFile))
	case strings.Contains(ct, "x-xz"):
		cmd = exec.Command("sh", "-c", fmt.Sprintf("xzcat %s > %s", compFile, outFile))
	case strings.Contains(ct, "zstd"):
		cmd = exec.Command("sh", "-c", fmt.Sprintf("zstdcat %s > %s", compFile, outFile))
	default:
		return "[unsupported compression]", "error"
	}

	if err := cmd.Run(); err != nil {
		return fmt.Sprintf("[decompression failed: %v]", err), "error"
	}

	decompressed, err := os.ReadFile(outFile)
	if err != nil {
		return "[read decompressed failed]", "error"
	}

	// Detect inner content type and extract recursively
	innerCT := http.DetectContentType(decompressed)
	text, method := s.extract(decompressed, innerCT)
	return text, "decompress+" + method
}

// ── CAD (DWG/DGN/Visio) — experimental ──────────────────────

func (s *Server) extractCAD(data []byte, ct string) (string, string) {
	// Try to convert to image via libreoffice (works for Visio/VSD)
	tmp, err := os.CreateTemp("", "taki-cad-*")
	if err != nil {
		return "[CAD temp error]", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	tmpDir, _ := os.MkdirTemp("", "taki-cad-conv-*")
	defer os.RemoveAll(tmpDir)

	// Try libreoffice conversion to PNG
	cmd := exec.Command("libreoffice", "--headless", "--convert-to", "png",
		"--outdir", tmpDir, tmp.Name())
	if err := cmd.Run(); err == nil {
		pngs, _ := filepath.Glob(filepath.Join(tmpDir, "*.png"))
		if len(pngs) > 0 {
			// Send rendered image to LLM Vision
			text := s.llmDescribe(pngs[0],
				"Describe this technical drawing or diagram. "+
					"Extract any text, labels, dimensions, and structural information.")
			if text != "" {
				return text, "cad_vision"
			}
		}
	}

	// Fallback: extract strings from binary
	var parts []string
	var current []byte
	for _, b := range data {
		if b >= 0x20 && b < 0x7f {
			current = append(current, b)
		} else {
			if len(current) > 10 {
				parts = append(parts, string(current))
			}
			current = current[:0]
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n"), "cad_strings"
	}
	return "[CAD format not extractable]", "skipped"
}

// ── Archives ─────────────────────────────────────────────────

func (s *Server) extractArchive(data []byte, ct string) (string, string) {
	tmpDir, _ := os.MkdirTemp("", "taki-archive-*")
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, "archive")
	os.WriteFile(archivePath, data, 0644)

	outDir := filepath.Join(tmpDir, "out")
	os.MkdirAll(outDir, 0755)

	var extractCmd *exec.Cmd
	switch {
	case strings.Contains(ct, "zip"), strings.Contains(ct, "java-archive"):
		extractCmd = exec.Command("unzip", "-o", "-d", outDir, archivePath)
	case strings.Contains(ct, "x-7z"):
		extractCmd = exec.Command("7z", "x", "-o"+outDir, archivePath)
	case strings.Contains(ct, "x-rar"):
		extractCmd = exec.Command("unrar", "x", archivePath, outDir+"/")
	case strings.Contains(ct, "x-tar"):
		extractCmd = exec.Command("tar", "xf", archivePath, "-C", outDir)
	}

	if extractCmd == nil {
		return "[unsupported archive format]", "error"
	}

	if err := extractCmd.Run(); err != nil {
		return fmt.Sprintf("[archive extraction failed: %v]", err), "error"
	}

	var allText []string
	filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Size() == 0 || info.Size() > 50*1024*1024 {
			return nil
		}
		fileData, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		fileCT := http.DetectContentType(fileData)
		relPath, _ := filepath.Rel(outDir, path)
		text, method := s.extract(fileData, fileCT)
		if text != "" && text[0] != '[' {
			allText = append(allText, fmt.Sprintf("--- %s [%s] ---\n%s", relPath, method, text))
		}
		return nil
	})

	if len(allText) > 0 {
		return strings.Join(allText, "\n\n"), "archive"
	}
	return "[empty archive]", "archive"
}

// ── Magic byte detection ─────────────────────────────────────

func (s *Server) extractByMagic(body []byte) (string, string) {
	if len(body) < 4 {
		if utf8.Valid(body) {
			return string(body), "passthrough"
		}
		return "[too small]", "error"
	}

	magic := string(body[:4])

	switch {
	case magic == "%PDF":
		return s.extractPDF(body)
	case body[0] == 0x50 && body[1] == 0x4B: // PK = ZIP (also DOCX, XLSX, EPUB, JAR)
		// Check if it's an Office document
		if bytes.Contains(body[:min(len(body), 2000)], []byte("word/")) {
			return s.extractOfficeDoc(body)
		}
		if bytes.Contains(body[:min(len(body), 2000)], []byte("xl/")) {
			return s.extractSpreadsheet(body)
		}
		if bytes.Contains(body[:min(len(body), 2000)], []byte("ppt/")) {
			return s.extractPresentation(body)
		}
		if bytes.Contains(body[:min(len(body), 2000)], []byte("EPUB")) {
			return s.extractEpub(body)
		}
		return s.extractArchive(body, "application/zip")
	case magic == "Rar!":
		return s.extractArchive(body, "application/x-rar-compressed")
	case magic[:2] == "\x1f\x8b": // gzip
		return s.extractCompressed(body, "application/gzip")
	case magic == "\xfd7zX": // xz
		return s.extractCompressed(body, "application/x-xz")
	case body[0] == 0xD0 && body[1] == 0xCF: // OLE2 (DOC, XLS, PPT, MSG)
		// Try as office document first, then MSG
		text, method := s.extractOfficeDoc(body)
		if text != "" && method != "error" {
			return text, method
		}
		return s.extractMSG(body)
	case magic[:3] == "ID3" || (body[0] == 0xFF && body[1]&0xE0 == 0xE0): // MP3
		return s.extractAudio(body, "audio/mpeg")
	case magic == "fLaC": // FLAC
		return s.extractAudio(body, "audio/flac")
	case magic == "OggS": // OGG
		return s.extractAudio(body, "audio/ogg")
	case magic == "RIFF": // WAV/AVI
		if len(body) > 11 && string(body[8:12]) == "WAVE" {
			return s.extractAudio(body, "audio/wav")
		}
		return s.extractVideo(body, "video/avi")
	case magic[:3] == "\xff\xd8\xff": // JPEG
		return s.extractImage(body, "image/jpeg")
	case magic == "\x89PNG": // PNG
		return s.extractImage(body, "image/png")
	case magic[:3] == "GIF": // GIF
		return s.extractImage(body, "image/gif")
	}

	if utf8.Valid(body) {
		return string(body), "passthrough"
	}
	return "[unknown format]", "error"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ── LLM helpers ──────────────────────────────────────────────

func (s *Server) llmDescribe(imagePath, prompt string) string {
	imgData, err := os.ReadFile(imagePath)
	if err != nil {
		return ""
	}

	ext := strings.ToLower(filepath.Ext(imagePath))
	mediaType := "image/png"
	switch ext {
	case ".jpg", ".jpeg":
		mediaType = "image/jpeg"
	case ".webp":
		mediaType = "image/webp"
	}

	b64 := base64.StdEncoding.EncodeToString(imgData)
	return s.llmVision("data:"+mediaType+";base64,"+b64, prompt)
}

func (s *Server) llmVision(dataURL, prompt string) string {
	content := []contentBlock{
		{Type: "text", Text: prompt},
		{Type: "image_url", ImageURL: &imageURL{URL: dataURL}},
	}
	return s.llmComplete([]chatMessage{{Role: "user", Content: content}})
}

func (s *Server) llmChat(prompt string) string {
	return s.llmComplete([]chatMessage{{Role: "user", Content: prompt}})
}

func (s *Server) llmComplete(messages []chatMessage) string {
	return s.llmCompleteOpts(messages, nil)
}

// llmCompleteStructured sends a chat completion with guided_json schema enforcement.
// The schema must be a valid JSON schema as json.RawMessage.
func (s *Server) llmCompleteStructured(messages []chatMessage, schema json.RawMessage) string {
	rf := &responseFormat{
		Type: "json_schema",
		JSONSchema: &jsonSchemaRef{
			Name:   "docmeta",
			Schema: schema,
		},
	}
	return s.llmCompleteOpts(messages, rf)
}

func (s *Server) llmTrackStart() {
	s.llmMu.Lock()
	s.llmInFlight++
	if s.llmInFlight == 1 {
		s.llmOldest = time.Now()
	}
	s.llmMu.Unlock()
}

func (s *Server) llmTrackDone() {
	s.llmMu.Lock()
	s.llmInFlight--
	if s.llmInFlight <= 0 {
		s.llmInFlight = 0
		s.llmOldest = time.Time{}
	}
	s.llmMu.Unlock()
}

func (s *Server) llmQueueStats() (int, time.Duration) {
	s.llmMu.Lock()
	defer s.llmMu.Unlock()
	if s.llmInFlight == 0 {
		return 0, 0
	}
	return s.llmInFlight, time.Since(s.llmOldest)
}

func (s *Server) llmCompleteOpts(messages []chatMessage, rf *responseFormat) string {
	s.llmTrackStart()
	defer s.llmTrackDone()

	reqBody := chatRequest{
		Model:          s.cfg.LLM.Model,
		MaxTokens:      s.cfg.LLM.MaxTokens,
		Temp:           0.0,
		Messages:       messages,
		ResponseFormat: rf,
	}

	jsonData, _ := json.Marshal(reqBody)
	url := strings.TrimRight(s.cfg.LLM.APIBase, "/") + "/chat/completions"

	const maxRetries = 3
	backoff := [3]time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := s.client.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			log.Printf("LLM error (attempt %d/%d): %v", attempt+1, maxRetries, err)
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return ""
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var chatResp chatResponse
		if err := json.Unmarshal(respBody, &chatResp); err != nil {
			log.Printf("LLM response parse error (HTTP %d, attempt %d/%d): %v (raw: %.300s)",
				resp.StatusCode, attempt+1, maxRetries, err, string(respBody))
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return ""
		}

		if len(chatResp.Choices) > 0 {
			return stripThinkTags(chatResp.Choices[0].Message.Content)
		}

		// Server error or rate limit — retry with backoff
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			log.Printf("LLM HTTP %d (attempt %d/%d), retrying in %v (raw: %.200s)",
				resp.StatusCode, attempt+1, maxRetries, backoff[attempt], string(respBody))
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return ""
		}

		// 2xx but no choices — LLM returned empty, retry
		if attempt < maxRetries-1 {
			log.Printf("LLM empty response (attempt %d/%d), retrying in %v", attempt+1, maxRetries, backoff[attempt])
			time.Sleep(backoff[attempt])
			continue
		}

		return ""
	}
	return ""
}

func stripThinkTags(s string) string {
	for {
		start := strings.Index(s, "<think>")
		if start < 0 {
			break
		}
		end := strings.Index(s, "</think>")
		if end < 0 {
			s = s[:start]
			break
		}
		s = s[:start] + s[end+len("</think>"):]
	}
	return strings.TrimSpace(s)
}

// ── Multipart writer (for Whisper API) ───────────────────────

type MultipartWriter struct {
	buf      *bytes.Buffer
	boundary string
}

func NewMultipartWriter(buf *bytes.Buffer, boundary string) *MultipartWriter {
	return &MultipartWriter{buf: buf, boundary: boundary}
}

func (w *MultipartWriter) WriteField(name, value string) {
	fmt.Fprintf(w.buf, "--%s\r\n", w.boundary)
	fmt.Fprintf(w.buf, "Content-Disposition: form-data; name=%q\r\n\r\n", name)
	fmt.Fprintf(w.buf, "%s\r\n", value)
}

func (w *MultipartWriter) WriteFile(fieldName, fileName string, r io.Reader) {
	fmt.Fprintf(w.buf, "--%s\r\n", w.boundary)
	fmt.Fprintf(w.buf, "Content-Disposition: form-data; name=%q; filename=%q\r\n", fieldName, fileName)
	fmt.Fprintf(w.buf, "Content-Type: application/octet-stream\r\n\r\n")
	io.Copy(w.buf, r)
	fmt.Fprintf(w.buf, "\r\n")
}

func (w *MultipartWriter) Close() {
	fmt.Fprintf(w.buf, "--%s--\r\n", w.boundary)
}

// ── Health & main ────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"name":    "open_taki",
		"version": version,
		"llm":     s.cfg.LLM.APIBase,
		"whisper": s.cfg.Whisper.APIBase,
		"collabora": s.cfg.Collabora.URL,
		"features": map[string]bool{
			"pdf":       true,
			"office":    s.cfg.Collabora.URL != "",
			"image":     true,
			"audio":     s.cfg.Whisper.APIBase != "",
			"archive":   true,
			"email":     true,
			"taki_v2":   true,
			"docmeta":   s.cfg.DocMeta.Enabled,
		},
		"docmeta_model": s.cfg.DocMeta.ModelVersion,
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "open_taki %s\n", version)
}

// handleTest probes all subsystems and returns structured status.
func (s *Server) handleTest(w http.ResponseWriter, r *http.Request) {
	type subsystem struct {
		Name    string `json:"name"`
		Status  string `json:"status"` // "ok", "failed", "disabled"
		Detail  string `json:"detail,omitempty"`
		Latency string `json:"latency,omitempty"`
	}

	results := []subsystem{}

	// Health-Check Client: 30s Timeout statt 5min (Produktions-Client)
	testClient := &http.Client{Timeout: 30 * time.Second}
	if s.cfg.Collabora.Insecure {
		testClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	// 1. LLM (local-ocr or configured model)
	if s.cfg.LLM.APIBase != "" {
		t0 := time.Now()
		llmStatus := "ok"
		llmDetail := fmt.Sprintf("%s (model: %s)", s.cfg.LLM.APIBase, s.cfg.LLM.Model)
		body := fmt.Sprintf(`{"model":"%s","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}`, s.cfg.LLM.Model)
		req, _ := http.NewRequest("POST", s.cfg.LLM.APIBase+"/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := testClient.Do(req)
		if err != nil {
			llmStatus = "failed"
			llmDetail = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				llmStatus = "failed"
				llmDetail = fmt.Sprintf("HTTP %d from %s", resp.StatusCode, s.cfg.LLM.APIBase)
			}
		}
		results = append(results, subsystem{
			Name: "llm", Status: llmStatus, Detail: llmDetail,
			Latency: fmt.Sprintf("%dms", time.Since(t0).Milliseconds()),
		})
	} else {
		results = append(results, subsystem{Name: "llm", Status: "disabled"})
	}

	// 2. Embedding
	if s.cfg.Embedding.APIBase != "" {
		t0 := time.Now()
		embStatus := "ok"
		embDetail := fmt.Sprintf("%s (model: %s)", s.cfg.Embedding.APIBase, s.cfg.Embedding.Model)
		body := fmt.Sprintf(`{"model":"%s","input":"test"}`, s.cfg.Embedding.Model)
		req, _ := http.NewRequest("POST", s.cfg.Embedding.APIBase+"/embeddings", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := testClient.Do(req)
		if err != nil {
			embStatus = "failed"
			embDetail = err.Error()
		} else {
			var result struct {
				Data []struct {
					Embedding []float64 `json:"embedding"`
				} `json:"data"`
			}
			json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()
			if len(result.Data) > 0 && len(result.Data[0].Embedding) > 0 {
				embDetail = fmt.Sprintf("%s → %d dims", embDetail, len(result.Data[0].Embedding))
			} else if resp.StatusCode >= 400 {
				embStatus = "failed"
				embDetail = fmt.Sprintf("HTTP %d from %s", resp.StatusCode, s.cfg.Embedding.APIBase)
			} else {
				embStatus = "failed"
				embDetail = "no embedding returned"
			}
		}
		results = append(results, subsystem{
			Name: "embedding", Status: embStatus, Detail: embDetail,
			Latency: fmt.Sprintf("%dms", time.Since(t0).Milliseconds()),
		})
	} else {
		results = append(results, subsystem{Name: "embedding", Status: "disabled"})
	}

	// 3. Whisper
	if s.cfg.Whisper.APIBase != "" {
		t0 := time.Now()
		whisperStatus := "ok"
		whisperDetail := fmt.Sprintf("%s (model: %s)", s.cfg.Whisper.APIBase, s.cfg.Whisper.Model)
		req, _ := http.NewRequest("GET", s.cfg.Whisper.APIBase+"/models", nil)
		resp, err := testClient.Do(req)
		if err != nil {
			whisperStatus = "failed"
			whisperDetail = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				whisperStatus = "failed"
				whisperDetail = fmt.Sprintf("HTTP %d from %s", resp.StatusCode, s.cfg.Whisper.APIBase)
			}
		}
		results = append(results, subsystem{
			Name: "whisper", Status: whisperStatus, Detail: whisperDetail,
			Latency: fmt.Sprintf("%dms", time.Since(t0).Milliseconds()),
		})
	} else {
		results = append(results, subsystem{Name: "whisper", Status: "disabled"})
	}

	// 4. Collabora
	if s.cfg.Collabora.URL != "" {
		t0 := time.Now()
		collabStatus := "ok"
		collabDetail := s.cfg.Collabora.URL
		resp, err := testClient.Get(s.cfg.Collabora.URL)
		if err != nil {
			collabStatus = "failed"
			collabDetail = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				collabStatus = "failed"
				collabDetail = fmt.Sprintf("HTTP %d", resp.StatusCode)
			}
		}
		results = append(results, subsystem{
			Name: "collabora", Status: collabStatus, Detail: collabDetail,
			Latency: fmt.Sprintf("%dms", time.Since(t0).Milliseconds()),
		})
	} else {
		results = append(results, subsystem{Name: "collabora", Status: "disabled"})
	}

	// 5. DocMeta
	if s.cfg.DocMeta.Enabled {
		dmStatus := "ok"
		dmDetail := fmt.Sprintf("model: %s, schema: %d bytes, rescue: %v", s.cfg.DocMeta.ModelVersion, len(s.cfg.DocMeta.schema), s.cfg.DocMeta.RescuePass)
		results = append(results, subsystem{Name: "docmeta", Status: dmStatus, Detail: dmDetail})
	} else {
		results = append(results, subsystem{Name: "docmeta", Status: "disabled"})
	}

	// Overall
	allOK := true
	for _, r := range results {
		if r.Status == "failed" {
			allOK = false
		}
	}
	overall := "ok"
	if !allOK {
		overall = "degraded"
	}

	// Queue stats
	inFlight, oldest := s.llmQueueStats()
	queue := map[string]interface{}{
		"in_flight": inFlight,
	}
	if inFlight > 0 {
		queue["oldest_seconds"] = int(oldest.Seconds())
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     overall,
		"version":    version,
		"subsystems": results,
		"queue":      queue,
	})
}

// handleSchema returns the metadata keys that taki can enrich per MIME type.
// Used by the search service to determine which files need re-enrichment.
func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	schema := map[string][]string{}

	// DocMeta keys from docmeta_schema.json (PDF, Office, Message)
	if s.cfg.DocMeta.Enabled && len(s.cfg.DocMeta.schema) > 0 {
		docKeys := extractSchemaKeys(s.cfg.DocMeta.schema)
		// DocMeta applies to these MIME types (match routing rules with "meta" feature)
		docMimes := []string{
			"application/pdf",
			"application/msword",
			"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			"application/vnd.oasis.opendocument.text",
			"message/rfc822",
			"application/vnd.ms-outlook",
		}
		// Also check routing rules for additional MIME types with "meta" feature
		for _, rule := range s.cfg.Routing.BleveRules {
			for _, f := range rule.Features {
				if f == "meta" {
					docMimes = append(docMimes, rule.Match.Mime...)
				}
			}
		}
		for _, mime := range docMimes {
			schema[mime] = appendUnique(schema[mime], docKeys...)
		}
	}

	// Audio metadata (from whisper + LLM)
	if s.cfg.Whisper.APIBase != "" {
		audioKeys := []string{"libre.graph.audio.title", "libre.graph.audio.artist",
			"libre.graph.audio.album", "libre.graph.audio.genre", "libre.graph.audio.year",
			"libre.graph.audio.track", "libre.graph.audio.duration"}
		for _, mime := range []string{"audio/mpeg", "audio/flac", "audio/ogg", "audio/wav", "audio/aac", "audio/mp4"} {
			schema[mime] = appendUnique(schema[mime], audioKeys...)
		}
	}

	// Image/Photo metadata (from EXIF + LLM)
	imageKeys := []string{"libre.graph.image.width", "libre.graph.image.height"}
	photoKeys := []string{"libre.graph.photo.cameraMake", "libre.graph.photo.cameraModel",
		"libre.graph.photo.takenDateTime", "libre.graph.photo.iso"}
	locationKeys := []string{"libre.graph.location.latitude", "libre.graph.location.longitude"}
	for _, mime := range []string{"image/jpeg", "image/png", "image/tiff", "image/webp"} {
		schema[mime] = appendUnique(schema[mime], imageKeys...)
		schema[mime] = appendUnique(schema[mime], photoKeys...)
		schema[mime] = appendUnique(schema[mime], locationKeys...)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schema)
}

// extractSchemaKeys flattens the docmeta JSON schema into "prefix.key" strings.
func extractSchemaKeys(schemaJSON json.RawMessage) []string {
	var s struct {
		Properties map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schemaJSON, &s); err != nil {
		return nil
	}
	var keys []string
	for prefix, obj := range s.Properties {
		if prefix == "uncertain" {
			continue
		}
		for key := range obj.Properties {
			if key == "subject_inferred" {
				continue // internal flag, not a metadata key
			}
			keys = append(keys, prefix+"."+key)
		}
	}
	return keys
}

func appendUnique(slice []string, items ...string) []string {
	seen := map[string]bool{}
	for _, s := range slice {
		seen[s] = true
	}
	for _, item := range items {
		if !seen[item] {
			slice = append(slice, item)
			seen[item] = true
		}
	}
	return slice
}

func main() {
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()
	if flag.NArg() > 0 {
		*configPath = flag.Arg(0)
	}

	cfg := loadConfig(*configPath)

	log.Printf("open_taki %s starting on %s", version, cfg.Listen)
	log.Printf("  LLM:       %s (model: %s)", cfg.LLM.APIBase, cfg.LLM.Model)
	if cfg.Collabora.URL != "" {
		log.Printf("  Collabora: %s (office conversion)", cfg.Collabora.URL)
	} else {
		log.Printf("  Collabora: not configured (office: pandoc-only, no DOC/XLS/PPT)")
	}
	if cfg.Whisper.APIBase != "" {
		log.Printf("  Whisper:   %s (model: %s)", cfg.Whisper.APIBase, cfg.Whisper.Model)
	} else {
		log.Printf("  Whisper:   disabled")
	}
	if cfg.DocMeta.Enabled {
		log.Printf("  DocMeta:   enabled (model=%s, rescue=%v, required=%v)", cfg.DocMeta.ModelVersion, cfg.DocMeta.RescuePass, cfg.DocMeta.RequiredFields)
	}

	srv := NewServer(cfg)

	http.HandleFunc("/tika/text", srv.handleTikaText)
	http.HandleFunc("/rmeta/text", srv.handleRmetaText)
	http.HandleFunc("/schema", srv.handleSchema)
	http.HandleFunc("/test", srv.handleTest)
	http.HandleFunc("/tika", srv.handleHealth)
	http.HandleFunc("/version", srv.handleVersion)
	http.HandleFunc("/", srv.handleHealth)

	log.Fatal(http.ListenAndServe(cfg.Listen, nil))
}
