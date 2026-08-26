// open_taki — lightweight Tika replacement with LLM-powered extraction
//
// Drop-in replacement for Apache Tika with protocol extension.
//
// Endpoints:
//   PUT /tika/text       → plain text extraction (Tika-compatible)
//   PUT /tika/pdf2chat   → PDF for chat: page-marked text + rescued images
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
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	goexif "github.com/rwcarlsen/goexif/exif"
	"gopkg.in/yaml.v3"
)

const version = "0.9.0"

type Config struct {
	Listen string `yaml:"listen"`
	LLM struct {
		APIBase       string `yaml:"api_base"`
		Model         string `yaml:"model"`
		MaxTokens     int    `yaml:"max_tokens"`
		TimeoutS      int    `yaml:"timeout_s"`      // HTTP timeout in seconds (default: 1800)
		MaxConcurrent int    `yaml:"max_concurrent"`  // max parallel LLM calls (default: 8)
	} `yaml:"llm"`
	OCR struct {
		Model string `yaml:"model"` // OCR stage model (grounded bbox OCR + VLM text OCR); empty = llm.model
	} `yaml:"ocr"`
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
	OpenCloud struct {
		URL string `yaml:"url"` // Pod-Mode: http://127.0.0.1:9200
	} `yaml:"opencloud"`
	Chat ChatConfig `yaml:"chat"`
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
	FieldRulesFile      string   `yaml:"field_rules_file"`        // path to field rules (default: /etc/open_taki/field_rules.json)
	StoreDetectPrompt   string   `yaml:"store_detect_prompt"`     // path to store detection prompt (default: /etc/open_taki/store_detect_prompt.txt)
	StoreDetectPlanFile string   `yaml:"store_detect_plan_file"`  // path to aktenplan candidates (default: /etc/open_taki/aktenplan.txt)

	// Loaded at startup (not from YAML)
	schema           json.RawMessage
	prompt           string
	rescuePrompt     string
	fieldRules       map[string]map[string]string // doc.type → field → template
	storeDetectPrompt string
	aktenplan         string
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

type methodStat struct {
	Count      int64         `json:"count"`
	TotalMs    int64         `json:"total_ms"`
	AvgMs      int64         `json:"avg_ms"`
	MaxMs      int64         `json:"max_ms"`
	TotalChars int64         `json:"total_chars"`
}

type Server struct {
	cfg    Config
	client *http.Client
	llmSem chan struct{} // concurrency limiter for LLM calls

	// In-flight LLM request tracking
	llmMu        sync.Mutex
	llmStartTimes []time.Time // start time of each in-flight request

	// Per-method stats
	statsMu    sync.Mutex
	methodStats map[string]*methodStat

	// Debug work directory (TAKI_WORK_DIR env). When set, per-request
	// directories are created with all intermediate artifacts.
	workDir string // empty = disabled

	// PDF OCR enrichment (TAKI_OCR_PDF env). When set, /taki/enrich-pdf
	// adds invisible text layers to scanned PDF pages via grounded VLM OCR.
	ocrPDF bool

	// OCR layer filters and cache (TAKI_LAYER_CONF_MIN / TAKI_LAYER_CACHE_MAX)
	layerConfMin  int // drop VLM regions with self-reported conf below this
	layerCacheMax int // max cached OCR layers (LRU by mtime)
}

// traceCtx carries per-request debug context through the pipeline.
type traceCtx struct {
	id      string // trace ID (from X-Taki-Trace-Id header or generated)
	ref     string // source ref (from X-Taki-Source-Ref)
	dir     string // work directory for this request (empty if debug disabled)
	llmSeq  int    // LLM call sequence counter
	enabled bool   // whether debug output is active
}

func newTraceCtx(s *Server, traceID, sourceRef string) *traceCtx {
	tc := &traceCtx{id: traceID, ref: sourceRef}
	if s.workDir == "" {
		return tc
	}
	tc.enabled = true
	if tc.id == "" {
		tc.id = fmt.Sprintf("%d", time.Now().UnixMilli())
	}
	tc.dir = filepath.Join(s.workDir, tc.id)
	os.MkdirAll(tc.dir, 0755)
	os.MkdirAll(filepath.Join(tc.dir, "llm"), 0755)
	// Write metadata
	meta := map[string]string{"trace_id": tc.id, "source_ref": sourceRef, "timestamp": time.Now().Format(time.RFC3339)}
	if data, err := json.MarshalIndent(meta, "", "  "); err == nil {
		os.WriteFile(filepath.Join(tc.dir, "trace.json"), data, 0644)
	}
	return tc
}

func (tc *traceCtx) writeFile(name string, data []byte) {
	if !tc.enabled || tc.dir == "" {
		return
	}
	os.WriteFile(filepath.Join(tc.dir, name), data, 0644)
}

func (tc *traceCtx) writeJSON(name string, v interface{}) {
	if !tc.enabled {
		return
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return
	}
	tc.writeFile(name, data)
}

func (tc *traceCtx) logLLM(label string, request, response interface{}) {
	if !tc.enabled {
		return
	}
	tc.llmSeq++
	prefix := fmt.Sprintf("llm/%02d_%s", tc.llmSeq, label)
	tc.writeJSON(prefix+"_request.json", request)
	tc.writeJSON(prefix+"_response.json", response)
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

	// EXIF metadata (images only)
	Image    *takiImage    `json:"X-TAKI:image,omitempty"`
	Photo    *takiPhoto    `json:"X-TAKI:photo,omitempty"`
	Location *takiLocation `json:"X-TAKI:location,omitempty"`
}

type takiImage struct {
	Width  int32 `json:"width,omitempty"`
	Height int32 `json:"height,omitempty"`
}

type takiPhoto struct {
	CameraMake           string  `json:"cameraMake,omitempty"`
	CameraModel          string  `json:"cameraModel,omitempty"`
	FNumber              float64 `json:"fNumber,omitempty"`
	FocalLength          float64 `json:"focalLength,omitempty"`
	ISO                  int32   `json:"iso,omitempty"`
	Orientation          int32   `json:"orientation,omitempty"`
	TakenDateTime        string  `json:"takenDateTime,omitempty"`
	ExposureNumerator    float64 `json:"exposureNumerator,omitempty"`
	ExposureDenominator  float64 `json:"exposureDenominator,omitempty"`
}

type takiLocation struct {
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
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

	// Load field rules
	if cfg.DocMeta.FieldRulesFile == "" {
		cfg.DocMeta.FieldRulesFile = filepath.Join(configDir, "field_rules.json")
	}
	if data, err := os.ReadFile(cfg.DocMeta.FieldRulesFile); err == nil {
		var rules map[string]map[string]string
		if json.Unmarshal(data, &rules) == nil {
			cfg.DocMeta.fieldRules = rules
			log.Printf("  DocMeta field_rules: %s (%d types)", cfg.DocMeta.FieldRulesFile, len(rules))
		} else {
			log.Printf("  DocMeta field_rules: invalid JSON in %s", cfg.DocMeta.FieldRulesFile)
		}
	} else {
		log.Printf("  DocMeta field_rules: not found (%s), skipping", cfg.DocMeta.FieldRulesFile)
	}

	// Load store detection prompt
	if cfg.DocMeta.StoreDetectPrompt == "" {
		cfg.DocMeta.StoreDetectPrompt = filepath.Join(configDir, "store_detect_prompt.txt")
	}
	if data, err := os.ReadFile(cfg.DocMeta.StoreDetectPrompt); err == nil {
		cfg.DocMeta.storeDetectPrompt = strings.TrimSpace(string(data))
		log.Printf("  DocMeta store_detect: %s (%d chars)", cfg.DocMeta.StoreDetectPrompt, len(cfg.DocMeta.storeDetectPrompt))
	} else {
		log.Printf("  DocMeta store_detect: not found (%s), store detection disabled", cfg.DocMeta.StoreDetectPrompt)
	}

	// Load aktenplan candidates
	if cfg.DocMeta.StoreDetectPlanFile == "" {
		cfg.DocMeta.StoreDetectPlanFile = filepath.Join(configDir, "aktenplan.txt")
	}
	if data, err := os.ReadFile(cfg.DocMeta.StoreDetectPlanFile); err == nil {
		cfg.DocMeta.aktenplan = strings.TrimSpace(string(data))
		log.Printf("  DocMeta aktenplan: %s (%d lines)", cfg.DocMeta.StoreDetectPlanFile, strings.Count(cfg.DocMeta.aktenplan, "\n")+1)
	} else {
		log.Printf("  DocMeta aktenplan: not found (%s), store detection disabled", cfg.DocMeta.StoreDetectPlanFile)
	}
}

// loadChatSystemPrompt lädt das Chat-System-Prompt-Template
// (Platzhalter {{folder}} und {{tools}}). Default-Pfad:
// <configDir>/prompts/chat_system.txt (taka-prompts-Paket).
// Datei fehlt → Built-in-Template (chatSystemPromptBuiltin in chat.go).
func (cfg *Config) loadChatSystemPrompt(configDir string) {
	if cfg.Chat.SystemPromptFile == "" {
		cfg.Chat.SystemPromptFile = filepath.Join(configDir, "prompts", "chat_system.txt")
	}
	if data, err := os.ReadFile(cfg.Chat.SystemPromptFile); err == nil {
		cfg.Chat.systemPrompt = strings.TrimSpace(string(data))
		log.Printf("  Chat system prompt: %s (%d chars)", cfg.Chat.SystemPromptFile, len(cfg.Chat.systemPrompt))
	} else {
		log.Printf("  Chat system prompt: using built-in default (%s not found)", cfg.Chat.SystemPromptFile)
		cfg.Chat.systemPrompt = chatSystemPromptBuiltin
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
	cfg.OpenCloud.URL = "http://127.0.0.1:9200"

	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("config %s not found, using defaults", path)
		cfg.Chat.applyDefaults(&cfg)
		cfg.loadDocMetaFiles(filepath.Dir(path))
		cfg.loadChatSystemPrompt(filepath.Dir(path))
		return cfg
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("config parse error: %v", err)
	}
	cfg.Chat.applyDefaults(&cfg)
	cfg.loadDocMetaFiles(filepath.Dir(path))
	cfg.loadChatSystemPrompt(filepath.Dir(path))
	return cfg
}

func NewServer(cfg Config) *Server {
	timeout := 30 * time.Minute
	if cfg.LLM.TimeoutS > 0 {
		timeout = time.Duration(cfg.LLM.TimeoutS) * time.Second
	}
	client := &http.Client{
		Timeout: timeout,
	}
	if cfg.Collabora.Insecure {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	maxConcurrent := cfg.LLM.MaxConcurrent
	if maxConcurrent < 1 {
		maxConcurrent = 8
	}
	workDir := os.Getenv("TAKI_WORK_DIR")
	if workDir != "" {
		os.MkdirAll(workDir, 0755)
		log.Printf("  WorkDebug: %s (per-request artifacts)", workDir)
	}
	ocrPDF := os.Getenv("TAKI_OCR_PDF") != ""
	if ocrPDF {
		log.Printf("  OCR-PDF enrichment: enabled")
	}
	layerConfMin := 40
	if v := os.Getenv("TAKI_LAYER_CONF_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			layerConfMin = n
		}
	}
	layerCacheMax := 500
	if v := os.Getenv("TAKI_LAYER_CACHE_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			layerCacheMax = n
		}
	}
	if workDir != "" {
		log.Printf("  Layer cache: %s/layers (conf min %d, max %d entries)",
			workDir, layerConfMin, layerCacheMax)
	}
	return &Server{
		cfg:           cfg,
		methodStats:   make(map[string]*methodStat),
		client:        client,
		llmSem:        make(chan struct{}, maxConcurrent),
		workDir:       workDir,
		ocrPDF:        ocrPDF,
		layerConfMin:  layerConfMin,
		layerCacheMax: layerCacheMax,
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

	requestStart := time.Now()
	ct := r.Header.Get("Content-Type")
	protocol := r.Header.Get("X-Taki-Protocol")
	features := parseFeatures(r.Header.Get("X-Taki-Features"))

	sourceRef := r.Header.Get("X-Taki-Source-Ref")
	traceID := r.Header.Get("X-Taki-Trace-Id")

	// Per-request debug context (active if TAKI_WORK_DIR set or X-Taki-Debug header)
	tc := newTraceCtx(s, traceID, sourceRef)
	if r.Header.Get("X-Taki-Debug") == "true" && s.workDir != "" {
		tc.enabled = true
	}
	tc.writeFile("input.bin", body)
	tc.writeJSON("request_headers.json", map[string]string{
		"Content-Type":       ct,
		"X-Taki-Protocol":   protocol,
		"X-Taki-Features":   r.Header.Get("X-Taki-Features"),
		"X-Taki-Source-Ref": sourceRef,
		"X-Taki-Trace-Id":  traceID,
		"X-Taki-Space-Type": r.Header.Get("X-Taki-Space-Type"),
		"input_size":        fmt.Sprintf("%d", len(body)),
	})

	extractStart := time.Now()
	text, method := s.extract(body, ct)
	extractDur := time.Since(extractStart)
	log.Printf("/rmeta/text: %d chars via %s (%s) extract=%v proto=%s ref=%s trace=%s",
		len(text), method, ct, extractDur, protocol, sourceRef, tc.id)
	tc.writeFile("extracted.txt", []byte(text))

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

		// EXIF metadata for images
		if strings.HasPrefix(ct, "image/") {
			exifMeta := extractEXIF(body)
			if len(exifMeta) > 0 {
				resp.Image, resp.Photo, resp.Location = exifToStructs(exifMeta)
			}
		}

		// DocMeta extraction — structured metadata (BEFORE general enrichment)
		if routedFeatures["docmeta"] && s.cfg.DocMeta.Enabled {
			resp.DocMeta = s.extractDocMeta(body, ct, text)
			if resp.DocMeta != nil {
				log.Printf("  ref=%s trace=%s docmeta: type=%s subject=%s",
					sourceRef, tc.id, dmGetStr(*resp.DocMeta, "doc.type"), dmGetStr(*resp.DocMeta, "doc.subject"))
			}
			tc.writeJSON("docmeta.json", resp.DocMeta)
		}

		// Store detection — AZ classification (AFTER docmeta, uses its results)
		if routedFeatures["store_detect"] && resp.DocMeta != nil && s.cfg.DocMeta.storeDetectPrompt != "" && s.cfg.DocMeta.aktenplan != "" {
			s.storeDetect(&resp, text)
			tc.writeJSON("store_detect.json", map[string]interface{}{
				"doc.store":       dmGetStr(*resp.DocMeta, "doc.store"),
				"doc.storeReason": dmGetStr(*resp.DocMeta, "doc.storeReason"),
			})
		}

		// LLM enrichment
		if routedFeatures["meta"] || routedFeatures["entities"] || routedFeatures["summary"] {
			s.enrichWithLLM(&resp, text, ct, routedFeatures)
			tc.writeJSON("enrichment.json", map[string]interface{}{
				"meta":     resp.Meta,
				"entities": resp.Entities,
				"summary":  resp.Summary,
			})
		}

		if routedFeatures["transcription"] && (strings.HasPrefix(ct, "audio/") || strings.HasPrefix(ct, "video/")) {
			resp.Transcr = text
		}

		if routedFeatures["embedding"] {
			resp.Embed = s.getEmbedding(text)
			tc.writeJSON("embedding.json", map[string]interface{}{
				"input_len":  len(text),
				"dims":       len(resp.Embed),
				"has_vector": len(resp.Embed) > 0,
			})
		}

		log.Printf("  routing: content→%s bleve=%v space=%s features=%v trace=%s",
			contentTarget, bleveContent, spaceType, routedFeatures, tc.id)

		elapsed := time.Since(requestStart)
		docType := ""
		if resp.DocMeta != nil {
			if dt, ok := (*resp.DocMeta)["doc"]; ok {
				if dm, ok := dt.(map[string]interface{}); ok {
					if t, ok := dm["type"].(string); ok {
						docType = t
					}
				}
			}
		}
		log.Printf("  DONE: %v method=%s chars=%d type=%s embed=%d trace=%s",
			elapsed, method, len(text), docType, len(resp.Embed), tc.id)

		// record stats
		s.recordStat(method, elapsed, len(text))

		tc.writeJSON("response.json", resp)
		tc.writeJSON("routing.json", resp.Routing)
		json.NewEncoder(w).Encode([]takiResponse{resp})
	} else {
		// Classic Tika MetaRecursive response
		meta := map[string]interface{}{
			"X-TIKA:content": text,
			"Content-Type":   ct,
			"X-TAKI:method":  method,
		}

		// Add Tika-compatible EXIF metadata for images
		if strings.HasPrefix(ct, "image/") {
			for k, v := range extractEXIF(body) {
				meta[k] = v
			}
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

// applyFieldRules evaluates field_rules templates for the given doc.type
// and overwrites fields with the rendered result. Templates use {field.name}
// placeholders. {a|b} tries a first, falls back to b. Empty results are skipped.
func applyFieldRules(dm takiDocMeta, rules map[string]map[string]string) {
	if rules == nil {
		return
	}
	docType := dmGetStr(dm, "doc.type")

	// Collect applicable rules: type-specific first, then wildcard
	var applicable map[string]string
	if typeRules, ok := rules[docType]; ok {
		applicable = typeRules
	} else if fallback, ok := rules["*"]; ok {
		applicable = fallback
	}
	if applicable == nil {
		return
	}

	for field, tmpl := range applicable {
		result := renderFieldTemplate(dm, tmpl)
		if result != "" {
			dmSet(dm, field, result)
		}
	}
}

// renderFieldTemplate replaces {field.name} placeholders with values from dm.
// {a|b} tries field a first, falls back to field b. Consecutive spaces are collapsed.
func renderFieldTemplate(dm takiDocMeta, tmpl string) string {
	var result strings.Builder
	i := 0
	for i < len(tmpl) {
		if tmpl[i] == '{' {
			end := strings.IndexByte(tmpl[i:], '}')
			if end < 0 {
				result.WriteByte(tmpl[i])
				i++
				continue
			}
			expr := tmpl[i+1 : i+end]
			val := ""
			for _, alt := range strings.Split(expr, "|") {
				alt = strings.TrimSpace(alt)
				if v := dmGetStr(dm, alt); v != "" {
					val = v
					break
				}
			}
			result.WriteString(val)
			i += end + 1
		} else {
			result.WriteByte(tmpl[i])
			i++
		}
	}
	// Collapse multiple spaces, trim
	out := strings.Join(strings.Fields(result.String()), " ")
	return out
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

// storeDetect runs a second LLM pass to classify a document into an Aktenplan code.
// Uses docmeta results from stage 1 + OCR text + aktenplan candidates.
func (s *Server) storeDetect(resp *takiResponse, fulltext string) {
	dm := resp.DocMeta
	if dm == nil {
		return
	}

	// Build document summary from stage 1 metadata
	docInfo := fmt.Sprintf("- Typ: %s\n- Betreff: %s\n- Datum: %s\n",
		dmGetStr(*dm, "doc.type"),
		dmGetStr(*dm, "doc.subject"),
		dmGetStr(*dm, "doc.date"))
	if ref := dmGetStr(*dm, "doc.reference"); ref != "" {
		docInfo += fmt.Sprintf("- Bezugszeichen: %s\n", ref)
	}
	if company := dmGetStr(*dm, "sender.company"); company != "" {
		docInfo += fmt.Sprintf("- Absender: %s", company)
		if city := dmGetStr(*dm, "sender.city"); city != "" {
			docInfo += ", " + city
		}
		docInfo += "\n"
	} else if name := dmGetStr(*dm, "sender.family_name"); name != "" {
		docInfo += fmt.Sprintf("- Absender: %s, %s\n", name, dmGetStr(*dm, "sender.given_name"))
	}

	// Truncate fulltext for first page context
	pageText := fulltext
	if len(pageText) > 4000 {
		pageText = pageText[:4000]
	}

	// Assemble prompt: static instructions + document + first page + candidates
	prompt := fmt.Sprintf("%s\n\nDokument:\n%s\nErste Seite:\n%s\n\n----\n\nKandidaten:\n%s",
		s.cfg.DocMeta.storeDetectPrompt,
		docInfo,
		pageText,
		s.cfg.DocMeta.aktenplan)

	log.Printf("store_detect: starting for %s (type=%s subject=%s)",
		dmGetStr(*dm, "doc.title"), dmGetStr(*dm, "doc.type"), dmGetStr(*dm, "doc.subject"))

	result := s.llmChat(prompt)
	if result == "" {
		log.Printf("store_detect: LLM returned empty")
		return
	}

	// Parse JSON response
	var storeResult struct {
		Store            string `json:"doc.store"`
		StoreReason      string `json:"doc.storeReason"`
		StoreAlternative string `json:"doc.store_alternative"`
		MehrTextNoetig   bool   `json:"mehr_text_noetig"`
	}
	if err := json.Unmarshal([]byte(result), &storeResult); err != nil {
		log.Printf("store_detect: JSON parse error: %v (raw: %s)", err, result[:min(len(result), 200)])
		return
	}

	log.Printf("store_detect: result store=%s reason=%q alt=%s mehr=%v",
		storeResult.Store, storeResult.StoreReason, storeResult.StoreAlternative, storeResult.MehrTextNoetig)

	// Merge into docmeta
	if storeResult.Store != "" && storeResult.Store != "unklar" {
		dmSet(*dm, "doc.store", storeResult.Store)
	}
	if storeResult.StoreReason != "" {
		dmSet(*dm, "doc.storeReason", storeResult.StoreReason)
	}
	if storeResult.StoreAlternative != "" {
		dmSet(*dm, "doc.store_alternative", storeResult.StoreAlternative)
	}
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

	// Apply field rules (deterministic template overrides)
	applyFieldRules(*dm, s.cfg.DocMeta.fieldRules)

	// Sanitize doc.title — must not contain path separators (/ or \)
	// as it is used as filename in WebDAV MOVE operations
	if title := dmGetStr(*dm, "doc.title"); title != "" {
		title = strings.ReplaceAll(title, "/", " - ")
		title = strings.ReplaceAll(title, "\\", " - ")
		dmSet(*dm, "doc.title", title)
	}

	if s.cfg.DocMeta.ModelVersion != "" {
		(*dm)["model"] = s.cfg.DocMeta.ModelVersion
	}

	log.Printf("docmeta: source=%s type=%s date=%s sender=%s title=%q uncertain=%d model=%s",
		dmGetStr(*dm, "source"),
		dmGetStr(*dm, "doc.type"), dmGetStr(*dm, "doc.date"),
		dmGetStr(*dm, "sender.company"), dmGetStr(*dm, "doc.title"),
		len(dmGetStrSlice(*dm, "uncertain")),
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
	return s.pdfPageToImageDPI(pdfData, pageNum, s.cfg.PDF.DPI)
}

func (s *Server) pdfPageToImageDPI(pdfData []byte, pageNum, dpi int) []byte {
	tmpDir, err := os.MkdirTemp("", "taki-docmeta-*")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "page")
	first := fmt.Sprintf("%d", pageNum)
	last := first

	cmd := exec.Command("pdftoppm", "-png", "-r",
		fmt.Sprintf("%d", dpi),
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
		"urkunde", "zeugnis", "sicknote":
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

// handleEmbed handles POST /embed — query embedding for semantic search.
// Accepts plain text (Content-Type: text/plain) or JSON {"text": "..."}.
// Returns JSON {"embedding": [...], "model": "...", "dimensions": N}.
func (s *Server) handleEmbed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var text string
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		var req struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		text = req.Text
	} else {
		body, _ := io.ReadAll(r.Body)
		text = string(body)
	}
	text = strings.TrimSpace(text)

	if text == "" {
		http.Error(w, "empty text", http.StatusBadRequest)
		return
	}

	// Rate-limit via LLM semaphore (shared with other LLM calls)
	s.llmSem <- struct{}{}
	embedding := s.getEmbedding(text)
	<-s.llmSem

	if embedding == nil {
		http.Error(w, "embedding failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"embedding":  embedding,
		"model":      s.cfg.Embedding.Model,
		"dimensions": len(embedding),
	})
	log.Printf("/embed: %d chars → %d dims", len(text), len(embedding))
}

func (s *Server) getEmbedding(text string) []float64 {
	if text == "" || len(text) < 3 {
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
	tmp, err := os.CreateTemp("", "taki-*.pdf")
	if err != nil {
		return "[extraction failed]", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	pageCount := s.pdfPageCount(tmp.Name())

	text := s.pdftextAll(tmp.Name())
	if s.isGoodText(text, pageCount) {
		return text, "pdftotext"
	}
	// Per-page check: some pages may have text (e.g. scanner OCR on page 1)
	// while others are pure image scans. Only OCR the weak pages.
	if pageCount >= 2 {
		hybrid := s.hybridExtract(tmp.Name(), pageCount)
		if hybrid != "" {
			return hybrid, "pdftotext+llm_ocr"
		}
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

func (s *Server) pdfPageCount(path string) int {
	cmd := exec.Command("pdfinfo", path)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "Pages:") {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pages:")))
			return n
		}
	}
	return 1
}

func (s *Server) pdftextAll(path string) string {
	cmd := exec.Command("pdftotext", "-layout", path, "-")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()
	return strings.TrimSpace(out.String())
}

func (s *Server) pdftextPage(path string, page int) string {
	pg := fmt.Sprintf("%d", page)
	cmd := exec.Command("pdftotext", "-layout", "-f", pg, "-l", pg, path, "-")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Run()
	return strings.TrimSpace(out.String())
}

func (s *Server) isGoodText(text string, pageCount int) bool {
	if len(text) < s.cfg.Fallback.MinChars {
		return false
	}
	if pageCount < 1 {
		pageCount = 1
	}
	return len(text)/pageCount >= s.cfg.Fallback.MinCharsPerPage
}

// hybridExtract checks each page individually. Pages with enough text
// keep their pdftotext result; pages below the threshold get LLM-OCR.
func (s *Server) hybridExtract(pdfPath string, pageCount int) string {
	pageTexts := make([]string, pageCount)
	var weakPages []int
	for i := 0; i < pageCount; i++ {
		t := s.pdftextPage(pdfPath, i+1)
		pageTexts[i] = t
		if len(t) < s.cfg.Fallback.MinCharsPerPage {
			weakPages = append(weakPages, i)
		}
	}

	if len(weakPages) == 0 || len(weakPages) == pageCount {
		return "" // all good or all weak — let caller handle
	}

	log.Printf("[extractPDF] hybrid: %d/%d pages need OCR", len(weakPages), pageCount)

	tmpDir, err := os.MkdirTemp("", "taki-ocr-*")
	if err != nil {
		return ""
	}
	defer os.RemoveAll(tmpDir)

	var wg sync.WaitGroup
	ocrResults := make(map[int]string)
	var mu sync.Mutex

	for _, pi := range weakPages {
		wg.Add(1)
		go func(pageIdx int) {
			defer wg.Done()
			pg := fmt.Sprintf("%d", pageIdx+1)
			prefix := filepath.Join(tmpDir, fmt.Sprintf("page-%03d", pageIdx))
			cmd := exec.Command("pdftoppm", "-png", "-r",
				fmt.Sprintf("%d", s.cfg.PDF.DPI),
				"-f", pg, "-l", pg, pdfPath, prefix)
			if err := cmd.Run(); err != nil {
				return
			}
			rendered, _ := filepath.Glob(prefix + "*.png")
			if len(rendered) == 0 {
				return
			}
			text := s.llmDescribe(rendered[0],
				"Extract all text from this scanned document page. "+
					"Return the complete text content, preserving structure. "+
					"If it's a form or contract, extract structured fields.")
			mu.Lock()
			ocrResults[pageIdx] = text
			mu.Unlock()
		}(pi)
	}
	wg.Wait()

	var parts []string
	for i := 0; i < pageCount; i++ {
		if ocr, ok := ocrResults[i]; ok && ocr != "" {
			parts = append(parts, ocr)
		} else if pageTexts[i] != "" {
			parts = append(parts, pageTexts[i])
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n\n")
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

// ── PDF → Chat ───────────────────────────────────────────────
//
// /tika/pdf2chat prepares a PDF for LLM chat (called by microllm):
// per-page text with [Seite N/M] markers, weak (scanned) pages rescued
// as PNG images and/or LLM-described, weak pages OCR'd into the text.
// Embedded raster figures (diagrams, tables, charts inside the text
// flow — common in exported, non-scanned PDFs) are rescued as images
// on every page, deduplicated across pages by content hash.

func (s *Server) handlePdf2Chat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	rescueImages := q.Get("images") == "1"
	rescueVector := q.Get("vector") == "1"
	describe := q.Get("describe") == "1"
	// ocr=0: skip OCR of weak pages (vision clients read the scan image
	// themselves, no text needed); default 1 keeps old behavior.
	ocr := q.Get("ocr") != "0"
	dpi := s.cfg.PDF.DPI
	if v, err := strconv.Atoi(q.Get("dpi")); err == nil && v >= 50 && v <= 300 {
		dpi = v
	}
	maxPages := s.cfg.PDF.MaxPages
	if v, err := strconv.Atoi(q.Get("max_pages")); err == nil && v >= 1 {
		maxPages = v
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 200*1024*1024))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if !bytes.HasPrefix(body, []byte("%PDF")) {
		http.Error(w, "not a PDF", http.StatusBadRequest)
		return
	}

	// Content-addressed cache: chat clients re-send the same PDF on every
	// turn, and extraction (OCR, description) is the expensive part.
	var cacheKey string
	if s.workDir != "" {
		imagesFlag, vectorFlag, describeFlag, ocrFlag := 0, 0, 0, 0
		if rescueImages {
			imagesFlag = 1
		}
		if rescueVector {
			vectorFlag = 1
		}
		if describe {
			describeFlag = 1
		}
		if ocr {
			ocrFlag = 1
		}
		cacheKey = fmt.Sprintf("%s.i%d.v%d.d%d.o%d.p%d.r%d.json",
			sha256hex(body), imagesFlag, vectorFlag, describeFlag, ocrFlag, maxPages, dpi)
		cachedPath := filepath.Join(s.workDir, "pdf2chat", cacheKey)
		if cached, err := os.ReadFile(cachedPath); err == nil {
			var cachedResult map[string]interface{}
			if json.Unmarshal(cached, &cachedResult) == nil {
				log.Printf("/tika/pdf2chat: cache hit (%s)", cacheKey)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(cachedResult)
				return
			}
		}
	}

	tmp, err := os.CreateTemp("", "taki-pdf2chat-*.pdf")
	if err != nil {
		http.Error(w, "temp error", http.StatusInternalServerError)
		return
	}
	tmp.Write(body)
	tmp.Close()
	defer os.Remove(tmp.Name())

	pageCount := s.pdfPageCount(tmp.Name())
	processed := pageCount
	if maxPages < processed {
		processed = maxPages
	}

	type pageResult struct {
		text   string
		images []pdf2chatImage
		descs  []string
	}
	results := make([]pageResult, processed)
	renderDir, renderErr := os.MkdirTemp("", "taki-pdf2chat-img-*")
	if renderErr != nil {
		renderDir = ""
	}
	defer os.RemoveAll(renderDir)

	const ocrPrompt = "Extract all text from this scanned document page. " +
		"Return the complete text content, preserving structure. " +
		"If it's a form or contract, extract structured fields."
	// figurePrompt describes one embedded figure in the context of the
	// surrounding page text, so the model can link figure to text.
	const figurePrompt = "Describe this figure from a document page for an AI assistant: " +
		"what it shows (diagram, table, chart, photo, screenshot) and how it " +
		"relates to the surrounding text. Be specific and concise.\n\n" +
		"Surrounding page text:\n%s"

	// Deduplicate embedded figures across pages (the same figure often
	// appears on several pages, e.g. a repeated diagram).
	seenImageHashes := map[string]bool{}
	var imageHashMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < processed; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pageNum := idx + 1
			res := &results[idx]
			res.text = s.pdftextPage(tmp.Name(), pageNum)
			if rescueImages {
				res.images = append(res.images,
					s.rescueEmbeddedImages(tmp.Name(), renderDir,
						pageNum, seenImageHashes, &imageHashMu)...)
			}
			if rescueVector {
				res.images = append(res.images,
					s.rescueVectorDrawings(tmp.Name(), renderDir,
						pageNum, dpi, seenImageHashes, &imageHashMu)...)
			}
			if len(res.text) < s.cfg.Fallback.MinCharsPerPage {
				// Weak page: reuse a rescued full-page image (a scan),
				// otherwise render the page once (image rescue + OCR share it).
				scanPath := ""
				for _, im := range res.images {
					if im.kind == "scan" {
						scanPath = im.path
						break
					}
				}
				if scanPath == "" {
					rendered := s.pdfPageToImageDPI(body, pageNum, dpi)
					if rendered == nil || renderDir == "" {
						return
					}
					scanPath = filepath.Join(renderDir, fmt.Sprintf("page-%03d.png", pageNum))
					os.WriteFile(scanPath, rendered, 0644)
					if rescueImages && len(res.images) == 0 {
						// No figure rescued: the render is the fallback image.
						res.images = append(res.images, pdf2chatImage{path: scanPath, kind: "scan"})
					}
				}
				// Scans are read by the vision model directly (or the OCR
				// text below) — only OCR when the client wants text.
				if ocr {
					if ocrText := s.llmDescribe(scanPath, ocrPrompt); ocrText != "" {
						res.text = ocrText
					}
				}
			}
			if describe {
				// Only embedded figures get a description, linked to the
				// surrounding page text. Scan images are read by vision.
				for _, im := range res.images {
					if im.kind != "embedded" {
						res.descs = append(res.descs, "")
						continue
					}
					surrounding := res.text
					if len(surrounding) > 2000 {
						cut := 2000
						for cut > 0 && !utf8.RuneStart(surrounding[cut]) {
							cut--
						}
						surrounding = surrounding[:cut] + " …"
					}
					res.descs = append(res.descs,
						s.llmDescribe(im.path, fmt.Sprintf(figurePrompt, surrounding)))
				}
			}
		}(i)
	}
	wg.Wait()

	type chatImage struct {
		Page        int    `json:"page"`
		B64         string `json:"b64"`
		Mime        string `json:"mime"`
		Kind        string `json:"kind"`
		Description string `json:"description,omitempty"`
	}
	images := []chatImage{} // non-nil: JSON "[]", never null
	var parts []string
	for i := 0; i < processed; i++ {
		pageNum := i + 1
		res := results[i]
		if res.text != "" {
			parts = append(parts, fmt.Sprintf("[Seite %d/%d]\n%s", pageNum, pageCount, res.text))
		}
		for imgIdx, im := range res.images {
			imgData, err := os.ReadFile(im.path)
			if err != nil || len(imgData) == 0 {
				continue
			}
			img := chatImage{Page: pageNum, Kind: im.kind,
				B64:  base64.StdEncoding.EncodeToString(imgData),
				Mime: "image/png"}
			if imgIdx < len(res.descs) {
				img.Description = res.descs[imgIdx]
			}
			images = append(images, img)
		}
	}
	if processed < pageCount {
		parts = append(parts, fmt.Sprintf(
			"[PDF gekürzt: nur die ersten %d von %d Seiten verarbeitet]", processed, pageCount))
	}

	result := map[string]interface{}{
		"text":   strings.Join(parts, "\n\n"),
		"pages":  pageCount,
		"images": images,
	}
	if s.workDir != "" {
		s.pdf2chatCacheSave(filepath.Join(s.workDir, "pdf2chat", cacheKey), result)
	}
	log.Printf("/tika/pdf2chat: %d pages (%d processed), %d chars, %d rescued images [images=%v describe=%v dpi=%d]",
		pageCount, processed, len(result["text"].(string)), len(images), rescueImages, describe, dpi)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// pdf2chatImage is one rescued page image: a figure embedded in the text
// flow (kind "embedded") or an image covering the whole page, i.e. a scan
// (kind "scan", also used for page renders of weak pages).
type pdf2chatImage struct {
	path string
	kind string
}

// pdfPageSize returns the size of one page in points via pdfinfo.
func (s *Server) pdfPageSize(path string, pageNum int) (widthPt, heightPt float64, ok bool) {
	out, err := exec.Command("pdfinfo", "-f", strconv.Itoa(pageNum),
		"-l", strconv.Itoa(pageNum), path).Output()
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		// Whole doc: "Page size: 612 x 792 pts (letter)"
		// Per page:  "Page    1 size:  612 x 792 pts (letter)"
		if !strings.HasPrefix(line, "Page") {
			continue
		}
		idx := strings.Index(line, "size:")
		if idx < 0 {
			continue
		}
		if _, err := fmt.Sscanf(line[idx+len("size:"):], "%g x %g", &widthPt, &heightPt); err == nil &&
			widthPt > 0 && heightPt > 0 {
			return widthPt, heightPt, true
		}
	}
	return 0, 0, false
}

// rescueEmbeddedImages extracts the raster images embedded in one page
// (pdfimages). This matters for normal text PDFs (exports, not scans):
// figures, tables and diagrams are embedded as images inside the text
// flow and are lost by a plain text extraction. Small icons and logo
// marks are filtered out by size. An image covering (nearly) the whole
// page is a scan, not a figure: it is kept but labeled kind "scan"
// (placement scale vs. MediaBox from pdfinfo). Deduplication by content
// hash avoids resending the same figure from multiple pages.
func (s *Server) rescueEmbeddedImages(pdfPath, renderDir string, pageNum int,
	seenImageHashes map[string]bool, imageHashMu *sync.Mutex) []pdf2chatImage {
	if renderDir == "" {
		return nil
	}
	const (
		minImageSide      = 250  // px, below: icons / bullet marks
		fullPageFactor    = 0.85 // covers >= 85% of the page (both dims)
		unknownFullPagePx = 2500 // fallback if placement/page size is unknown
		minImageBytes     = 2048 // extracted PNG smaller than this is noise
		maxPerPage        = 4
	)
	pageArg := strconv.Itoa(pageNum)
	listOut, err := exec.Command("pdfimages", "-list",
		"-f", pageArg, "-l", pageArg, pdfPath).Output()
	if err != nil {
		return nil
	}

	// List rows (including smask rows) correspond 1:1 to the extracted
	// files; smasks are extracted as separate files, so keep them in
	// the index mapping even though they are not rescued themselves.
	// Column layout: page num type width height color comp bpc enc interp
	// objectID [gen] x-ppi y-ppi size ratio — the ppi columns are taken
	// from the end, the type/size columns from the front.
	type imageRow struct {
		rescue bool
		kind   string
		width  int
		height int
		xPpi   int
		yPpi   int
	}
	var rows []imageRow
	rescueCandidates := 0
	for _, line := range strings.Split(string(listOut), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 || (fields[2] != "image" && fields[2] != "smask") {
			continue
		}
		width, errW := strconv.Atoi(fields[3])
		height, errH := strconv.Atoi(fields[4])
		if errW != nil || errH != nil {
			continue
		}
		row := imageRow{width: width, height: height}
		if fields[2] == "image" && width >= minImageSide && height >= minImageSide {
			row.rescue = true
			row.kind = "embedded"
			row.xPpi, _ = strconv.Atoi(fields[len(fields)-4])
			row.yPpi, _ = strconv.Atoi(fields[len(fields)-3])
			rescueCandidates++
		}
		rows = append(rows, row)
	}
	if rescueCandidates == 0 {
		return nil
	}

	// Full-page check: image placement scale (px per inch, from the
	// pdfimages matrix columns) vs. page size in points (pdfinfo).
	// One pdfinfo call per page, only if a candidate reports a scale.
	pageWpt, pageHpt, pageOk := 0.0, 0.0, false
	for _, row := range rows {
		if row.rescue && row.xPpi > 0 && row.yPpi > 0 {
			pageWpt, pageHpt, pageOk = s.pdfPageSize(pdfPath, pageNum)
			break
		}
	}
	keptRows := make([]imageRow, 0, len(rows))
	keptCandidates := 0
	for _, row := range rows {
		if row.rescue {
			if row.xPpi > 0 && row.yPpi > 0 && pageOk {
				if float64(row.width)/float64(row.xPpi)*72 >= fullPageFactor*pageWpt &&
					float64(row.height)/float64(row.yPpi)*72 >= fullPageFactor*pageHpt {
					row.kind = "scan"
				}
			} else if row.width > unknownFullPagePx && row.height > unknownFullPagePx {
				// No reliable placement/page size: a huge image is almost
				// certainly a page scan, not a figure — leave it to the
				// render path.
				continue
			}
			keptCandidates++
		}
		keptRows = append(keptRows, row)
	}
	rows = keptRows
	if keptCandidates == 0 {
		return nil
	}

	prefix := filepath.Join(renderDir, fmt.Sprintf("emb-p%03d-", pageNum))
	if err := exec.Command("pdfimages", "-png",
		"-f", pageArg, "-l", pageArg, pdfPath, prefix).Run(); err != nil {
		return nil
	}
	files, err := filepath.Glob(prefix + "-*.png")
	if err != nil || len(files) == 0 {
		return nil
	}
	sort.Strings(files)

	var rescued []pdf2chatImage
	scansRescued := 0
	for i, row := range rows {
		if !row.rescue || i >= len(files) {
			continue
		}
		if len(rescued) >= maxPerPage {
			break
		}
		if row.kind == "scan" && scansRescued >= 1 {
			continue // one page image per page is enough
		}
		imgData, err := os.ReadFile(files[i])
		if err != nil || len(imgData) < minImageBytes {
			continue
		}
		imgHash := sha256hex(imgData)
		imageHashMu.Lock()
		duplicate := seenImageHashes[imgHash]
		if !duplicate {
			seenImageHashes[imgHash] = true
		}
		imageHashMu.Unlock()
		if !duplicate {
			if row.kind == "scan" {
				scansRescued++
			}
			rescued = append(rescued, pdf2chatImage{path: files[i], kind: row.kind})
		}
	}
	return rescued
}

// rescueVectorDrawings extracts vector-drawing clusters from one page via
// PyMuPDF (pymupdf) and renders them as cropped PNGs via pdftoppm.
// Vectors (diagrams, charts, schematics) are invisible to pdfimages.
// Returns kind="vector" images (no OCR, no LLM describe — vision reads them).
func (s *Server) rescueVectorDrawings(pdfPath, renderDir string, pageNum, dpi int,
	seenImageHashes map[string]bool, imageHashMu *sync.Mutex) []pdf2chatImage {
	if renderDir == "" {
		return nil
	}
	// Locate the helper script (installed next to the binary in the container)
	scriptPath := ""
	if s.workDir != "" {
		// Script lives in the work dir or next to the binary
		for _, p := range []string{
			filepath.Join(filepath.Dir(os.Args[0]), "taki-vector-crop.py"),
			"/usr/local/bin/taki-vector-crop.py",
		} {
			if _, err := os.Stat(p); err == nil {
				scriptPath = p
				break
			}
		}
	}
	if scriptPath == "" {
		return nil
	}

	out, err := exec.Command("python3", scriptPath,
		pdfPath, strconv.Itoa(pageNum), renderDir, strconv.Itoa(dpi)).Output()
	if err != nil {
		return nil
	}
	var clusters []struct {
		Path     string  `json:"path"`
		HasRaster bool   `json:"has_raster"`
		SizePt   float64 `json:"size_pt"`
	}
	if json.Unmarshal(out, &clusters) != nil || len(clusters) == 0 {
		return nil
	}

	var rescued []pdf2chatImage
	for _, c := range clusters {
		if c.Path == "" {
			continue
		}
		imgData, err := os.ReadFile(c.Path)
		if err != nil || len(imgData) == 0 {
			continue
		}
		imgHash := sha256hex(imgData)
		imageHashMu.Lock()
		duplicate := seenImageHashes[imgHash]
		if !duplicate {
			seenImageHashes[imgHash] = true
		}
		imageHashMu.Unlock()
		if !duplicate {
			rescued = append(rescued, pdf2chatImage{path: c.Path, kind: "vector"})
		}
	}
	return rescued
}

// pdf2chatCacheSave writes a pdf2chat result and prunes the cache
// (max 50 entries, oldest by mtime first).
func (s *Server) pdf2chatCacheSave(path string, result interface{}) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}
	data, err := json.Marshal(result)
	if err != nil {
		return
	}
	os.WriteFile(path+".tmp", data, 0644)
	os.Rename(path+".tmp", path)
	s.prunePdf2ChatCache(dir)
}

func (s *Server) prunePdf2ChatCache(dir string) {
	const maxEntries = 50
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) <= maxEntries {
		return
	}
	type cacheEntry struct {
		name string
		mt   time.Time
	}
	var list []cacheEntry
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		list = append(list, cacheEntry{e.Name(), info.ModTime()})
	}
	if len(list) <= maxEntries {
		return
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mt.Before(list[j].mt) })
	for i := 0; i < len(list)-maxEntries; i++ {
		os.Remove(filepath.Join(dir, list[i].name))
	}
}

// ── PDF OCR enrichment (TAKI_OCR_PDF) ────────────────────────

// ocrRegion is a text region with bounding box from grounded VLM OCR.
// VLM may return bbox_2d or bbox — we accept both.
type ocrRegion struct {
	BboxD2      [4]int  `json:"bbox_2d"`
	Bbox         [4]int `json:"bbox"`
	Text         string `json:"text"`
	TextContent  string `json:"text_content"`
	Conf         int    `json:"conf"` // 0-100; 0 = not provided
}

// TextOf returns the region text. Some VLMs report the key as
// `text_content` instead of `text` (observed with thinking-off mode),
// so both are accepted.
func (r ocrRegion) TextOf() string {
	if r.Text != "" {
		return r.Text
	}
	return r.TextContent
}

func (r ocrRegion) Box() [4]int {
	if r.BboxD2 != [4]int{} {
		return r.BboxD2
	}
	return r.Bbox
}

const groundedOCRPrompt = `Detect all text regions on this scanned document page. Return a JSON array of objects, each with bbox_2d ([x1,y1,x2,y2] in the normalized 0-1000 coordinate space of this image, where (0,0) is the top-left and (1000,1000) the bottom-right corner), text (the exact text, in reading order), and conf (your confidence in the text, 0-100). Return ONLY the JSON array, no explanation, no markdown fences.`

// groundedOCR sends a page image to the VLM and returns positioned text regions.
// Retries once on JSON parse failure (VLM hallucination).
func (s *Server) groundedOCR(imagePath string) ([]ocrRegion, error) {
	for attempt := 0; attempt < 3; attempt++ {
		raw, backend := s.llmDescribeWithBackend(imagePath, groundedOCRPrompt)
		if raw == "" {
			if attempt < 2 {
				log.Printf("[groundedOCR] empty response (attempt %d, backend=%s), retrying", attempt+1, backend)
				continue
			}
			return nil, fmt.Errorf("empty LLM response (backend=%s)", backend)
		}
		// Strip markdown fences if present
		raw = strings.TrimSpace(raw)
		if strings.HasPrefix(raw, "```") {
			lines := strings.Split(raw, "\n")
			if len(lines) > 2 {
				raw = strings.Join(lines[1:len(lines)-1], "\n")
			}
		}

		var regions []ocrRegion
		if err := json.Unmarshal([]byte(raw), &regions); err != nil {
			if attempt < 2 {
				log.Printf("[groundedOCR] JSON parse failed (attempt %d, backend=%s), retrying: %v", attempt+1, backend, err)
				continue
			}
			return nil, fmt.Errorf("JSON parse error (backend=%s): %v (raw: %.200s)", backend, err, raw)
		}
		// Check for empty-text regions (backend returned bboxes but no OCR text)
		withText := 0
		for _, r := range regions {
			if r.TextOf() != "" {
				withText++
			}
		}
		if len(regions) > 0 && withText == 0 {
			if attempt < 2 {
				log.Printf("[groundedOCR] %d regions but all empty text (attempt %d, backend=%s), retrying", len(regions), attempt+1, backend)
				continue
			}
			return nil, fmt.Errorf("all %d regions have empty text (backend=%s)", len(regions), backend)
		}
		return regions, nil
	}
	return nil, fmt.Errorf("groundedOCR failed after retries")
}

// ── Canonical OCR layer (taki-ocr-layer/1) ───────────────────

// OCRLayerFormat identifies the canonical layer format (v1).
const OCRLayerFormat = "taki-ocr-layer/1"

// ocrLayerRegion is one positioned text region. Bbox is [x0,y0,x1,y1] as
// 0-1 floats relative to the visible (rotation-applied) page — independent
// of the render DPI used for OCR.
type ocrLayerRegion struct {
	Bbox [4]float64 `json:"bbox"`
	Text string     `json:"text"`
	Conf int        `json:"conf"` // 0-100; 0 = model did not report
}

type ocrLayerPage struct {
	Index   int              `json:"index"` // 0-based
	Size    [2]float64       `json:"size"`  // visible page (w,h) in points
	Regions []ocrLayerRegion `json:"regions"`
}

// pdfLayer is the content-addressed OCR artifact for one source PDF
// (key = sha256 of the original bytes). Only pages that received at least
// one region are listed.
type pdfLayer struct {
	Format       string         `json:"format"`
	Engine       string         `json:"engine"`
	Created      string         `json:"created"`
	SourceSHA256 string         `json:"source_sha256"` // set before caching
	PageCount    int            `json:"page_count"`
	Pages        []ocrLayerPage `json:"pages"`
}

// normalizeBbox converts a VLM bbox to 0-1 floats of the visible page.
// Values <= 1000 are the VLM's [0..1000] convention; larger values are
// pixel coordinates of the rendered page (imgW x imgH).
func normalizeBbox(box [4]int, imgW, imgH int) [4]float64 {
	maxV := box[0]
	for _, v := range box[1:] {
		if v > maxV {
			maxV = v
		}
	}
	var out [4]float64
	if maxV > 1000 && imgW > 0 && imgH > 0 {
		out[0] = float64(box[0]) / float64(imgW)
		out[1] = float64(box[1]) / float64(imgH)
		out[2] = float64(box[2]) / float64(imgW)
		out[3] = float64(box[3]) / float64(imgH)
	} else {
		for i, v := range box {
			out[i] = float64(v) / 1000.0
		}
	}
	return out
}

func hasLetterOrDigit(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// filterRegions drops OCR noise: text without letters/digits, short
// fragments, low-confidence regions (when the model reported confidence),
// and degenerate boxes.
func (s *Server) filterRegions(regions []ocrRegion, imgW, imgH int) []ocrLayerRegion {
	var out []ocrLayerRegion
	for _, r := range regions {
		text := strings.TrimSpace(r.TextOf())
		if len(text) < 2 || !hasLetterOrDigit(text) {
			continue
		}
		if r.Conf > 0 && r.Conf < s.layerConfMin {
			continue
		}
		box := r.Box()
		if box == [4]int{} {
			continue
		}
		bb := normalizeBbox(box, imgW, imgH)
		if bb[2] <= bb[0] || bb[3] <= bb[1] {
			continue
		}
		out = append(out, ocrLayerRegion{Bbox: bb, Text: text, Conf: r.Conf})
	}
	return out
}

// ocrPDFPages finds pages without text and runs grounded VLM OCR on them.
// Returns a layer with zero pages when the PDF already has text.
func (s *Server) ocrPDFPages(data []byte) (*pdfLayer, error) {
	tmp, err := os.CreateTemp("", "taki-ocr-*.pdf")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return nil, err
	}
	tmp.Close()

	pageCount := s.pdfPageCount(tmp.Name())
	if pageCount < 1 {
		return nil, fmt.Errorf("cannot determine page count")
	}

	// Find weak pages (ocrStrategy: auto — pages with existing text are
	// skipped, so extraction and search results are not doubled up)
	var weakPages []int
	for i := 0; i < pageCount; i++ {
		t := s.pdftextPage(tmp.Name(), i+1)
		if len(t) < s.cfg.Fallback.MinCharsPerPage {
			weakPages = append(weakPages, i)
		}
	}

	layer := &pdfLayer{
		Format:    OCRLayerFormat,
		Engine:    "opentaki/grounded-vlm",
		Created:   time.Now().UTC().Format(time.RFC3339),
		PageCount: pageCount,
	}

	if len(weakPages) == 0 {
		log.Printf("[ocrPDFPages] all %d pages have text, nothing to OCR", pageCount)
		return layer, nil // already good
	}

	log.Printf("[ocrPDFPages] %d/%d pages need OCR", len(weakPages), pageCount)

	// Render weak pages to PNG and run grounded OCR
	tmpDir, err := os.MkdirTemp("", "taki-ocr-tmp-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	type pageResult struct {
		pageIdx int // 0-based
		regions []ocrLayerRegion
		wPt     float64 // visible page size in points
		hPt     float64
	}

	results := make(chan pageResult, len(weakPages))
	var wg sync.WaitGroup

	for _, pi := range weakPages {
		if pi >= s.cfg.PDF.MaxPages {
			continue
		}
		wg.Add(1)
		go func(pageIdx int) {
			defer wg.Done()
			pg := fmt.Sprintf("%d", pageIdx+1)
			prefix := filepath.Join(tmpDir, fmt.Sprintf("page-%03d", pageIdx))
			cmd := exec.Command("pdftoppm", "-png", "-r",
				fmt.Sprintf("%d", s.cfg.PDF.DPI),
				"-f", pg, "-l", pg, tmp.Name(), prefix)
			if err := cmd.Run(); err != nil {
				log.Printf("[ocrPDFPages] pdftoppm page %d failed: %v", pageIdx+1, err)
				return
			}
			rendered, _ := filepath.Glob(prefix + "*.png")
			if len(rendered) == 0 {
				return
			}
			// Rendered pixel size → visible page size in points
			// (pdftoppm applies /Rotate; 1pt = DPI/72 px)
			imgW, imgH := pngDimensions(rendered[0])
			wPt := float64(imgW) * 72.0 / float64(s.cfg.PDF.DPI)
			hPt := float64(imgH) * 72.0 / float64(s.cfg.PDF.DPI)
			raw, err := s.groundedOCR(rendered[0])
			if err != nil {
				log.Printf("[ocrPDFPages] grounded OCR page %d failed: %v", pageIdx+1, err)
				return
			}
			regions := s.filterRegions(raw, imgW, imgH)
			if len(regions) > 0 {
				results <- pageResult{pageIdx: pageIdx, regions: regions, wPt: wPt, hPt: hPt}
			}
		}(pi)
	}
	wg.Wait()
	close(results)

	for pr := range results {
		layer.Pages = append(layer.Pages, ocrLayerPage{
			Index:   pr.pageIdx,
			Size:    [2]float64{pr.wPt, pr.hPt},
			Regions: pr.regions,
		})
	}
	sort.Slice(layer.Pages, func(i, j int) bool {
		return layer.Pages[i].Index < layer.Pages[j].Index
	})

	totalRegions := 0
	for _, p := range layer.Pages {
		totalRegions += len(p.Regions)
		log.Printf("[ocrPDFPages] page %d: %d regions", p.Index+1, len(p.Regions))
	}
	log.Printf("[ocrPDFPages] %d pages, %d text regions", len(layer.Pages), totalRegions)

	// Debug: dump the layer to the work dir
	if s.workDir != "" {
		if layerJSON, err := json.Marshal(layer); err == nil {
			os.WriteFile(filepath.Join(s.workDir, "ocr-layer.json"), layerJSON, 0644)
		}
	}

	return layer, nil
}

// embedLayer writes invisible text layers into the PDF via the Python
// helper (PyMuPDF, render_mode=3) and returns the layered bytes.
func (s *Server) embedLayer(data []byte, layer *pdfLayer) ([]byte, error) {
	if len(layer.Pages) == 0 {
		return data, nil
	}
	tmpDir, err := os.MkdirTemp("", "taki-embed-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	inPath := filepath.Join(tmpDir, "input.pdf")
	if err := os.WriteFile(inPath, data, 0644); err != nil {
		return nil, err
	}
	layerJSON, err := json.Marshal(layer)
	if err != nil {
		return nil, err
	}
	layerFile := filepath.Join(tmpDir, "layer.json")
	if err := os.WriteFile(layerFile, layerJSON, 0644); err != nil {
		return nil, err
	}

	outPath := filepath.Join(tmpDir, "output.pdf")
	cmd := exec.Command("python3", "/usr/local/bin/taki-embed-ocr.py", inPath, outPath, layerFile)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("taki-embed-ocr.py failed: %v: %s", err, stderr.String())
	}
	return os.ReadFile(outPath)
}

// enrichPDF takes a PDF, finds pages without text, runs grounded OCR on
// them, and embeds invisible text layers via the Python helper.
// Stateless: no skip checks, no caching (that is /taki/remount-pdf).
func (s *Server) enrichPDF(data []byte) ([]byte, error) {
	layer, err := s.ocrPDFPages(data)
	if err != nil {
		return nil, err
	}
	if len(layer.Pages) == 0 {
		return data, nil
	}
	return s.embedLayer(data, layer)
}

// pngDimensions reads width and height from a PNG file header.
func pngDimensions(path string) (int, int) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	// PNG IHDR: bytes 16-19 = width (big-endian), 20-23 = height
	buf := make([]byte, 24)
	if _, err := f.Read(buf); err != nil {
		return 0, 0
	}
	w := int(buf[16])<<24 | int(buf[17])<<16 | int(buf[18])<<8 | int(buf[19])
	h := int(buf[20])<<24 | int(buf[21])<<16 | int(buf[22])<<8 | int(buf[23])
	return w, h
}

func (s *Server) handleEnrichPDF(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "PUT or POST required", http.StatusMethodNotAllowed)
		return
	}
	if !s.ocrPDF {
		http.Error(w, "TAKI_OCR_PDF not enabled", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if len(body) < 5 || string(body[:5]) != "%PDF-" {
		http.Error(w, "not a PDF", http.StatusBadRequest)
		return
	}

	enriched, err := s.enrichPDF(body)
	if err != nil {
		log.Printf("[enrichPDF] error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(enriched)))
	w.Header().Set("X-Taki-Method", "enrich-pdf")
	w.Write(enriched)
}

// ── OCR layer remount (content-addressed cache) ──────────────

// remountMaxUpload bounds /taki/remount-pdf request bodies. The OpenCloud
// side enforces the actual write-back cap; this only protects Taka itself.
const remountMaxUpload = 200 << 20 // 200 MB

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// pdfCheckResult is the output of taki-pdf-check.py — structural facts
// about a PDF before deciding whether it may be modified at all.
type pdfCheckResult struct {
	OK        bool         `json:"ok"`
	NeedsPass bool         `json:"needs_pass"` // user-password encryption
	Signed    bool         `json:"signed"`     // signature field (/Sig)
	PdfAPart  int          `json:"pdfa_part"`  // 0 = not PDF/A
	PageCount int          `json:"page_count"`
	Pages     [][2]float64 `json:"pages"` // visible (rotation-applied) size, points
}

// pdfCheck runs the Python helper; nil means the helper itself failed
// (fail-closed: callers must reject the PDF).
func (s *Server) pdfCheck(path string) *pdfCheckResult {
	cmd := exec.Command("python3", "/usr/local/bin/taki-pdf-check.py", path)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Printf("[pdfCheck] helper failed: %v: %s", err, stderr.String())
		return nil
	}
	var res pdfCheckResult
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		log.Printf("[pdfCheck] invalid JSON: %v (out: %.200s)", err, out.String())
		return nil
	}
	return &res
}

func layerCachePath(workDir string) string {
	if workDir == "" {
		return ""
	}
	return filepath.Join(workDir, "layers")
}

func loadLayer(dir, sha string) (*pdfLayer, bool) {
	if dir == "" || sha == "" {
		return nil, false
	}
	path := filepath.Join(dir, sha+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var l pdfLayer
	if err := json.Unmarshal(data, &l); err != nil || l.Format != OCRLayerFormat {
		log.Printf("[layers] dropping invalid cache entry %s: %v", sha, err)
		os.Remove(path)
		return nil, false
	}
	return &l, true
}

func saveLayer(dir string, l *pdfLayer) {
	if dir == "" || l.SourceSHA256 == "" {
		return
	}
	data, err := json.Marshal(l)
	if err != nil {
		return
	}
	os.MkdirAll(dir, 0755)
	if err := os.WriteFile(filepath.Join(dir, l.SourceSHA256+".json"), data, 0644); err != nil {
		log.Printf("[layers] save failed: %v", err)
	}
}

// pruneLayerCache keeps at most layerCacheMax entries (oldest by mtime).
func (s *Server) pruneLayerCache(dir string) {
	if dir == "" || s.layerCacheMax < 1 {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type entry struct {
		name string
		mt   time.Time
	}
	var list []entry
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		list = append(list, entry{e.Name(), info.ModTime()})
	}
	if len(list) <= s.layerCacheMax {
		return
	}
	sort.Slice(list, func(i, j int) bool { return list[i].mt.Before(list[j].mt) })
	for i := 0; i < len(list)-s.layerCacheMax; i++ {
		os.Remove(filepath.Join(dir, list[i].name))
	}
}

// validateLayer checks a cached layer against the PDF at hand before
// re-embedding it. False = layer must not be reused (page count/size
// mismatch, or a page now carries visible text — re-embedding would
// double the layer).
func (s *Server) validateLayer(l *pdfLayer, check *pdfCheckResult, pdfPath string) bool {
	if !check.OK || l.PageCount != check.PageCount {
		return false
	}
	for _, p := range l.Pages {
		if p.Index < 0 || p.Index >= len(check.Pages) {
			return false
		}
		if sz := check.Pages[p.Index]; sz[0] > 0 && sz[1] > 0 {
			if math.Abs(sz[0]-p.Size[0]) > 2 || math.Abs(sz[1]-p.Size[1]) > 2 {
				return false
			}
		}
		if len(s.pdftextPage(pdfPath, p.Index+1)) >= s.cfg.Fallback.MinCharsPerPage {
			return false
		}
	}
	return true
}

// skipRemount rejects the PDF with 409 + X-Taki-Skip reason.
func skipRemount(w http.ResponseWriter, reason string) {
	w.Header().Set("X-Taki-Skip", reason)
	http.Error(w, "skipped: "+reason, http.StatusConflict)
}

// handleRemountPDF handles POST/PUT /taki/remount-pdf.
//
// Body: the original (scanned) PDF. Response: the same PDF with invisible
// OCR text layers embedded — OpenCloud stores the result as a new file
// revision.
//
// Content-addressed: layers are cached under <workDir>/layers/<sha256>.json,
// so remounting an already-OCR'd original is a pure mount (no VLM calls).
//
// Response headers:
//
//	X-Taki-Layer: hit | miss | empty
//	X-Taki-Skip:  encrypted | signed | pdfa1  (409 only)
func (s *Server) handleRemountPDF(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "POST or PUT required", http.StatusMethodNotAllowed)
		return
	}
	if !s.ocrPDF {
		http.Error(w, "TAKI_OCR_PDF not enabled", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, remountMaxUpload))
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if len(body) < 5 || string(body[:5]) != "%PDF-" {
		http.Error(w, "not a PDF", http.StatusBadRequest)
		return
	}

	tmp, err := os.CreateTemp("", "taki-remount-*.pdf")
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	tmp.Close()

	// Skip checks (fail-closed): helper failure → 500, dangerous PDF → 409.
	check := s.pdfCheck(tmp.Name())
	if check == nil {
		http.Error(w, "pdf check failed", http.StatusInternalServerError)
		return
	}
	switch {
	case check.NeedsPass:
		skipRemount(w, "encrypted")
		return
	case check.Signed:
		skipRemount(w, "signed")
		return
	case check.PdfAPart == 1:
		skipRemount(w, "pdfa1")
		return
	}

	dir := layerCachePath(s.workDir)
	sha := sha256hex(body)

	var layer *pdfLayer
	hit := false
	if cached, ok := loadLayer(dir, sha); ok {
		if s.validateLayer(cached, check, tmp.Name()) {
			layer = cached
			hit = true
			log.Printf("[remount-pdf] layer cache hit %s (%d pages)", sha[:12], len(layer.Pages))
		} else {
			log.Printf("[remount-pdf] layer cache entry %s stale, re-OCRing", sha[:12])
			os.Remove(filepath.Join(dir, sha+".json"))
		}
	}
	if layer == nil {
		fresh, err := s.ocrPDFPages(body)
		if err != nil {
			log.Printf("[remount-pdf] OCR failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(fresh.Pages) == 0 {
			// PDF already has text — nothing to mount.
			w.Header().Set("Content-Type", "application/pdf")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Set("X-Taki-Layer", "empty")
			w.Write(body)
			return
		}
		fresh.SourceSHA256 = sha
		layer = fresh
		saveLayer(dir, layer)
		s.pruneLayerCache(dir)
	}

	enriched, err := s.embedLayer(body, layer)
	if err != nil {
		log.Printf("[remount-pdf] embed failed: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if hit {
		w.Header().Set("X-Taki-Layer", "hit")
	} else {
		w.Header().Set("X-Taki-Layer", "miss")
	}
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(enriched)))
	w.Header().Set("X-Taki-Method", "remount-pdf")
	w.Write(enriched)
}

// ── EXIF metadata extraction ─────────────────────────────────

// extractEXIF returns Tika-compatible metadata keys from JPEG/TIFF EXIF headers.
func extractEXIF(data []byte) map[string]string {
	meta := make(map[string]string)
	x, err := goexif.Decode(bytes.NewReader(data))
	if err != nil {
		return meta
	}

	getInt := func(field goexif.FieldName) (int, bool) {
		tag, err := x.Get(field)
		if err != nil {
			return 0, false
		}
		v, err := tag.Int(0)
		if err != nil {
			return 0, false
		}
		return v, true
	}

	getRat := func(field goexif.FieldName) (float64, bool) {
		tag, err := x.Get(field)
		if err != nil {
			return 0, false
		}
		num, den, err := tag.Rat2(0)
		if err != nil || den == 0 {
			return 0, false
		}
		return float64(num) / float64(den), true
	}

	getString := func(field goexif.FieldName) (string, bool) {
		tag, err := x.Get(field)
		if err != nil {
			return "", false
		}
		return strings.Trim(tag.String(), "\""), true
	}

	if v, ok := getInt(goexif.ImageWidth); ok {
		meta["tiff:ImageWidth"] = fmt.Sprintf("%d", v)
	} else if v, ok := getInt(goexif.PixelXDimension); ok {
		meta["tiff:ImageWidth"] = fmt.Sprintf("%d", v)
	}
	if v, ok := getInt(goexif.ImageLength); ok {
		meta["tiff:ImageLength"] = fmt.Sprintf("%d", v)
	} else if v, ok := getInt(goexif.PixelYDimension); ok {
		meta["tiff:ImageLength"] = fmt.Sprintf("%d", v)
	}

	if v, ok := getString(goexif.Make); ok {
		meta["tiff:Make"] = v
	}
	if v, ok := getString(goexif.Model); ok {
		meta["tiff:Model"] = v
	}
	if v, ok := getRat(goexif.FNumber); ok {
		meta["exif:FNumber"] = fmt.Sprintf("%.1f", v)
	}
	if v, ok := getRat(goexif.FocalLength); ok {
		meta["exif:FocalLength"] = fmt.Sprintf("%.1f", v)
	}
	if v, ok := getInt(goexif.ISOSpeedRatings); ok {
		meta["Base ISO"] = fmt.Sprintf("%d", v)
	}
	if v, ok := getInt(goexif.Orientation); ok {
		meta["tiff:Orientation"] = fmt.Sprintf("%d", v)
	}
	if v, ok := getRat(goexif.ExposureTime); ok {
		meta["exif:ExposureTime"] = fmt.Sprintf("%g", v)
	}

	if t, err := x.DateTime(); err == nil {
		meta["exif:DateTimeOriginal"] = t.Format("2006-01-02T15:04:05")
	}

	if lat, long, err := x.LatLong(); err == nil {
		if !math.IsNaN(lat) && !math.IsNaN(long) {
			meta["geo:lat"] = fmt.Sprintf("%f", lat)
			meta["geo:long"] = fmt.Sprintf("%f", long)
		}
	}

	return meta
}

// exifToStructs converts the flat Tika-compatible EXIF map to structured types.
func exifToStructs(meta map[string]string) (*takiImage, *takiPhoto, *takiLocation) {
	var img *takiImage
	var photo *takiPhoto
	var loc *takiLocation

	if w, ok := meta["tiff:ImageWidth"]; ok {
		if img == nil { img = &takiImage{} }
		fmt.Sscanf(w, "%d", &img.Width)
	}
	if h, ok := meta["tiff:ImageLength"]; ok {
		if img == nil { img = &takiImage{} }
		fmt.Sscanf(h, "%d", &img.Height)
	}

	if v, ok := meta["tiff:Make"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		photo.CameraMake = v
	}
	if v, ok := meta["tiff:Model"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		photo.CameraModel = v
	}
	if v, ok := meta["exif:FNumber"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		fmt.Sscanf(v, "%f", &photo.FNumber)
	}
	if v, ok := meta["exif:FocalLength"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		fmt.Sscanf(v, "%f", &photo.FocalLength)
	}
	if v, ok := meta["Base ISO"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		fmt.Sscanf(v, "%d", &photo.ISO)
	}
	if v, ok := meta["tiff:Orientation"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		fmt.Sscanf(v, "%d", &photo.Orientation)
	}
	if v, ok := meta["exif:DateTimeOriginal"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		photo.TakenDateTime = v
	}
	if v, ok := meta["exif:ExposureTime"]; ok {
		if photo == nil { photo = &takiPhoto{} }
		var et float64
		fmt.Sscanf(v, "%g", &et)
		if et > 0 {
			photo.ExposureNumerator = 1
			photo.ExposureDenominator = 1 / et
		}
	}

	if lat, ok := meta["geo:lat"]; ok {
		if loc == nil { loc = &takiLocation{} }
		fmt.Sscanf(lat, "%f", &loc.Latitude)
	}
	if lng, ok := meta["geo:long"]; ok {
		if loc == nil { loc = &takiLocation{} }
		fmt.Sscanf(lng, "%f", &loc.Longitude)
	}

	return img, photo, loc
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
	text, method := s.transcribeAudioBytes(data, "audio/"+ct, "")
	if method == "whisper" {
		return text, "whisper"
	}
	return text, method // "skipped" | "error"
}

// transcribeAudioBytes leitet rohe Audio-Bytes über Whisper (microllm
// llm-stt). ct = MIME-Typ (Bestimmung der Dateiendung), filename =
// Originalname (Fallback, z.B. "aufnahme.webm"). Liefert Text und Methode:
// "whisper" (Erfolg), "skipped" (nicht konfiguriert), "error" (Fehler —
// text enthält dann einen Hinweis).
func (s *Server) transcribeAudioBytes(data []byte, ct, filename string) (string, string) {
	if s.cfg.Whisper.APIBase == "" {
		return "[audio transcription not configured]", "skipped"
	}

	ext := ".wav"
	for _, pair := range [][2]string{{"mp3", ".mp3"}, {"mpeg", ".mp3"}, {"ogg", ".ogg"}, {"webm", ".webm"}, {"flac", ".flac"}, {"m4a", ".m4a"}, {"mp4", ".m4a"}, {"aac", ".aac"}} {
		if strings.Contains(ct, pair[0]) {
			ext = pair[1]
			break
		}
	}
	// Filename-Match vor MIME (Browser-Aufnahmen: MIME webm, Name .webm)
	for _, pair := range [][2]string{{".mp3", ".mp3"}, {".ogg", ".ogg"}, {".oga", ".ogg"}, {".webm", ".webm"}, {".flac", ".flac"}, {".m4a", ".m4a"}, {".mp4", ".m4a"}, {".wav", ".wav"}, {".aac", ".aac"}} {
		if strings.HasSuffix(strings.ToLower(filename), pair[0]) {
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

	audioPath := tmp.Name()

	// WebM/Opus: vLLM Whisper decodes it not — transcode to WAV first
	if ext == ".webm" || ext == ".ogg" {
		wavPath := tmp.Name() + ".wav"
		cmd := exec.Command("ffmpeg", "-i", tmp.Name(),
			"-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
			"-y", wavPath)
		if err := cmd.Run(); err != nil {
			log.Printf("ffmpeg transcode error: %v", err)
		} else {
			defer os.Remove(wavPath)
			audioPath = wavPath
		}
	}

	text := s.whisperTranscribe(audioPath)
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
		text, method := s.extractPDF(pdfData)
		if text != "" && text != "[extraction failed]" {
			return text, "collabora_" + method
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
	result, _ := s.llmDescribeWithBackend(imagePath, prompt)
	return result
}

// llmDescribeWithBackend returns the LLM response and the backend that served it.
func (s *Server) llmDescribeWithBackend(imagePath, prompt string) (string, string) {
	imgData, err := os.ReadFile(imagePath)
	if err != nil {
		return "", ""
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
	content := []contentBlock{
		{Type: "text", Text: prompt},
		{Type: "image_url", ImageURL: &imageURL{URL: "data:" + mediaType + ";base64," + b64}},
	}
	// OCR stage (grounded bbox OCR, VLM text OCR, CAD reading) — uses ocr.model
	return s.llmCompleteOCRWithBackend([]chatMessage{{Role: "user", Content: content}})
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

func (s *Server) llmTrackStart() time.Time {
	t := time.Now()
	s.llmMu.Lock()
	s.llmStartTimes = append(s.llmStartTimes, t)
	s.llmMu.Unlock()
	return t
}

func (s *Server) llmTrackDone(t time.Time) {
	s.llmMu.Lock()
	for i, st := range s.llmStartTimes {
		if st.Equal(t) {
			s.llmStartTimes = append(s.llmStartTimes[:i], s.llmStartTimes[i+1:]...)
			break
		}
	}
	s.llmMu.Unlock()
}

func (s *Server) llmQueueStats() (int, time.Duration) {
	s.llmMu.Lock()
	defer s.llmMu.Unlock()
	n := len(s.llmStartTimes)
	if n == 0 {
		return 0, 0
	}
	oldest := s.llmStartTimes[0]
	for _, t := range s.llmStartTimes[1:] {
		if t.Before(oldest) {
			oldest = t
		}
	}
	return n, time.Since(oldest)
}

func (s *Server) llmCompleteOpts(messages []chatMessage, rf *responseFormat) string {
	result, _ := s.llmCompleteOptsBackend(messages, rf, nil, "")
	return result
}

// llmCompleteWithBackend returns the result and the x-backend header.
func (s *Server) llmCompleteWithBackend(messages []chatMessage) (string, string) {
	return s.llmCompleteOptsBackend(messages, nil, nil, "")
}

// ocrModel returns the model for the OCR stage (grounded bbox OCR + VLM
// text OCR). An empty ocr.model falls back to llm.model.
func (s *Server) ocrModel() string {
	if s.cfg.OCR.Model != "" {
		return s.cfg.OCR.Model
	}
	return s.cfg.LLM.Model
}

// llmCompleteOCRWithBackend is the OCR-stage entry point. Uses ocr.model
// (fallback llm.model). All llmDescribe* calls (page OCR, grounded bbox
// OCR, CAD vision) go through here.
func (s *Server) llmCompleteOCRWithBackend(messages []chatMessage) (string, string) {
	return s.llmCompleteOptsBackendModel(messages, nil, nil, "", s.ocrModel())
}

func (s *Server) llmCompleteOptsTrace(messages []chatMessage, rf *responseFormat, tc *traceCtx, label string) string {
	result, _ := s.llmCompleteOptsBackend(messages, rf, tc, label)
	return result
}

func (s *Server) llmCompleteOptsBackend(messages []chatMessage, rf *responseFormat, tc *traceCtx, label string) (string, string) {
	return s.llmCompleteOptsBackendModel(messages, rf, tc, label, s.cfg.LLM.Model)
}

func (s *Server) llmCompleteOptsBackendModel(messages []chatMessage, rf *responseFormat, tc *traceCtx, label, model string) (string, string) {
	s.llmSem <- struct{}{}        // acquire slot
	defer func() { <-s.llmSem }() // release slot
	t := s.llmTrackStart()
	defer s.llmTrackDone(t)

	reqBody := chatRequest{
		Model:          model,
		MaxTokens:      s.cfg.LLM.MaxTokens,
		Temp:           0.0,
		Messages:       messages,
		ResponseFormat: rf,
	}

	jsonData, _ := json.Marshal(reqBody)
	url := strings.TrimRight(s.cfg.LLM.APIBase, "/") + "/chat/completions"

	if tc != nil && tc.enabled {
		tc.logLLM(label, reqBody, nil)
	}

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
			return "", ""
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		backend := resp.Header.Get("X-Backend")

		var chatResp chatResponse
		if err := json.Unmarshal(respBody, &chatResp); err != nil {
			log.Printf("LLM response parse error (HTTP %d, attempt %d/%d, backend=%s): %v (raw: %.300s)",
				resp.StatusCode, attempt+1, maxRetries, backend, err, string(respBody))
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return "", backend
		}

		if len(chatResp.Choices) > 0 {
			result := stripThinkTags(chatResp.Choices[0].Message.Content)
			if tc != nil && tc.enabled {
				tc.writeJSON(fmt.Sprintf("llm/%02d_%s_response.json", tc.llmSeq, label),
					map[string]interface{}{"status": resp.StatusCode, "content": result, "backend": backend})
			}
			return result, backend
		}

		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			log.Printf("LLM HTTP %d (attempt %d/%d, backend=%s), retrying in %v",
				resp.StatusCode, attempt+1, maxRetries, backend, backoff[attempt])
			if attempt < maxRetries-1 {
				time.Sleep(backoff[attempt])
				continue
			}
			return "", backend
		}

		if attempt < maxRetries-1 {
			log.Printf("LLM empty response (attempt %d/%d, backend=%s), retrying in %v",
				attempt+1, maxRetries, backend, backoff[attempt])
			time.Sleep(backoff[attempt])
			continue
		}

		return "", backend
	}
	return "", ""
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

	// 1b. OCR stage model (only when it differs from the LLM model)
	if ocrM := s.ocrModel(); ocrM != s.cfg.LLM.Model {
		t0 := time.Now()
		ocrStatus := "ok"
		ocrDetail := fmt.Sprintf("%s (model: %s)", s.cfg.LLM.APIBase, ocrM)
		body := fmt.Sprintf(`{"model":"%s","messages":[{"role":"user","content":"Say OK"}],"max_tokens":5}`, ocrM)
		req, _ := http.NewRequest("POST", s.cfg.LLM.APIBase+"/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := testClient.Do(req)
		if err != nil {
			ocrStatus = "failed"
			ocrDetail = err.Error()
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				ocrStatus = "failed"
				ocrDetail = fmt.Sprintf("HTTP %d from %s", resp.StatusCode, s.cfg.LLM.APIBase)
			}
		}
		results = append(results, subsystem{
			Name: "ocr", Status: ocrStatus, Detail: ocrDetail,
			Latency: fmt.Sprintf("%dms", time.Since(t0).Milliseconds()),
		})
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
		"max":       cap(s.llmSem),
	}
	if inFlight > 0 {
		queue["oldest_seconds"] = int(oldest.Seconds())
	}

	// microllm backend stats (if LLM or Embedding goes through microllm)
	var backends interface{}
	microllmBase := s.cfg.LLM.APIBase
	if microllmBase == "" {
		microllmBase = s.cfg.Embedding.APIBase
	}
	if microllmBase != "" {
		statsURL := strings.TrimRight(microllmBase, "/v1") + "/stats"
		// Try both with and without /v1 suffix
		if strings.HasSuffix(statsURL, "/v1/stats") {
			statsURL = strings.TrimSuffix(statsURL, "/v1/stats") + "/stats"
		}
		statsResp, err := testClient.Get(statsURL)
		if err == nil {
			defer statsResp.Body.Close()
			var statsData struct {
				Models map[string]struct {
					Requests int     `json:"requests"`
					Errors   int     `json:"errors"`
					AvgTokS  float64 `json:"avg_tok_s"`
					Backends []struct {
						APIBase        string                  `json:"api_base"`
						Healthy        bool                    `json:"healthy"`
						FailCount      int                     `json:"fail_count"`
						UnhealthySince *float64                `json:"unhealthy_since"`
						Queue          *struct {
							InFlight int `json:"in_flight"`
							Max      int `json:"max"`
						} `json:"queue,omitempty"`
					} `json:"backends"`
				} `json:"models"`
			}
			if json.NewDecoder(statsResp.Body).Decode(&statsData) == nil {
				// Extract only models relevant to taki (local-ocr, local-embed, llm-stt)
				relevant := map[string]interface{}{}
				for _, name := range []string{"local-ocr", "local-embed", "llm-stt"} {
					if m, ok := statsData.Models[name]; ok {
						backendSummary := []map[string]interface{}{}
						for _, b := range m.Backends {
							entry := map[string]interface{}{
								"api_base": b.APIBase,
								"healthy":  b.Healthy,
							}
							if b.FailCount > 0 {
								entry["fail_count"] = b.FailCount
							}
							if b.Queue != nil {
								entry["queue"] = fmt.Sprintf("%d/%d", b.Queue.InFlight, b.Queue.Max)
							}
							backendSummary = append(backendSummary, entry)
						}
						relevant[name] = map[string]interface{}{
							"requests": m.Requests,
							"errors":   m.Errors,
							"backends": backendSummary,
						}
					}
				}
				if len(relevant) > 0 {
					backends = relevant
				}
			}
		}
	}

	response := map[string]interface{}{
		"status":     overall,
		"version":    version,
		"subsystems": results,
		"queue":      queue,
	}
	if backends != nil {
		response["backends"] = backends
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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

func (s *Server) recordStat(method string, elapsed time.Duration, chars int) {
	ms := elapsed.Milliseconds()
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	st, ok := s.methodStats[method]
	if !ok {
		st = &methodStat{}
		s.methodStats[method] = st
	}
	st.Count++
	st.TotalMs += ms
	st.TotalChars += int64(chars)
	st.AvgMs = st.TotalMs / st.Count
	if ms > st.MaxMs {
		st.MaxMs = ms
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.methodStats)
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
	ocrModel := cfg.LLM.Model
	if cfg.OCR.Model != "" {
		ocrModel = cfg.OCR.Model
	}
	log.Printf("  OCR:       model: %s", ocrModel)
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
	log.Printf("  Chat:      /chat/ask (opencloud=%s, model=%s, max_iter=%d, chat_token=%v)", cfg.OpenCloud.URL, cfg.Chat.DefaultModel, cfg.Chat.MaxIterations, cfg.Chat.ChatToken.Secret != "")

	srv := NewServer(cfg)

	http.HandleFunc("/tika/text", srv.handleTikaText)
	http.HandleFunc("/tika/pdf2chat", srv.handlePdf2Chat)
	http.HandleFunc("/rmeta/text", srv.handleRmetaText)
	http.HandleFunc("/taki/enrich-pdf", srv.handleEnrichPDF)
	http.HandleFunc("/taki/remount-pdf", srv.handleRemountPDF)
	http.HandleFunc("/embed", srv.handleEmbed)
	http.HandleFunc("/schema", srv.handleSchema)
	http.HandleFunc("/chat/ask", srv.handleChatAsk)
	http.HandleFunc("/chat/token", srv.handleChatToken)
	http.HandleFunc("/chat/tools", srv.handleChatTools)
	http.HandleFunc("/chat-direct/ask", srv.handleChatDirectAsk)
	http.HandleFunc("/chat-direct/transcribe", srv.handleChatTranscribe)
	http.HandleFunc("/test", srv.handleTest)
	http.HandleFunc("/stats", srv.handleStats)
	http.HandleFunc("/tika", srv.handleHealth)
	http.HandleFunc("/version", srv.handleVersion)
	http.HandleFunc("/", srv.handleHealth)

	log.Fatal(http.ListenAndServe(cfg.Listen, nil))
}
