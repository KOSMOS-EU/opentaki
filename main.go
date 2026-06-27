// open_taki — lightweight Tika replacement with LLM OCR
//
// Drop-in replacement for Apache Tika's text extraction endpoint.
// Uses pdftotext for text-based PDFs, falls back to LLM Vision OCR
// (via microllm/vLLM) for scanned documents.
//
// Tika-compatible API:
//   PUT /tika/text     → extract text from PDF body
//   GET /tika          → health check
//   GET /version       → version info
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const version = "0.1.0"

type Config struct {
	Listen string `yaml:"listen"`
	LLM    struct {
		APIBase   string `yaml:"api_base"`
		Model     string `yaml:"model"`
		MaxTokens int    `yaml:"max_tokens"`
	} `yaml:"llm"`
	PDF struct {
		DPI      int `yaml:"dpi"`
		MaxPages int `yaml:"max_pages"`
	} `yaml:"pdf"`
	Fallback struct {
		MinChars        int `yaml:"min_chars"`
		MinCharsPerPage int `yaml:"min_chars_per_page"`
	} `yaml:"fallback"`
}

type Server struct {
	cfg    Config
	client *http.Client
}

// OpenAI chat completion request
type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens"`
	Temp      float64       `json:"temperature"`
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

// Tika-compatible JSON response
type tikaResponse struct {
	Content string `json:"X-TIKA:content"`
	Type    string `json:"Content-Type"`
	Method  string `json:"X-TAKI:method"`
}

func loadConfig(path string) Config {
	var cfg Config
	// defaults
	cfg.Listen = ":9998"
	cfg.LLM.APIBase = "http://localhost:8012/v1"
	cfg.LLM.Model = "local"
	cfg.LLM.MaxTokens = 4096
	cfg.PDF.DPI = 150
	cfg.PDF.MaxPages = 20
	cfg.Fallback.MinChars = 200
	cfg.Fallback.MinCharsPerPage = 50

	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("config %s not found, using defaults", path)
		return cfg
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("config parse error: %v", err)
	}
	return cfg
}

func NewServer(cfg Config) *Server {
	return &Server{
		cfg: cfg,
		client: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

func (s *Server) handleTikaText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 100*1024*1024)) // 100MB max
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	ct := r.Header.Get("Content-Type")
	accept := r.Header.Get("Accept")

	var text, method string

	switch {
	case strings.Contains(ct, "pdf"):
		text, method = s.extractPDF(body)
	case strings.Contains(ct, "text/plain"):
		text = string(body)
		method = "passthrough"
	case strings.Contains(ct, "text/html"):
		text, method = s.extractHTML(body)
	default:
		// Try as PDF anyway
		if len(body) > 4 && string(body[:4]) == "%PDF" {
			text, method = s.extractPDF(body)
		} else if utf8.Valid(body) {
			text = string(body)
			method = "passthrough"
		} else {
			http.Error(w, "unsupported content type: "+ct, http.StatusUnsupportedMediaType)
			return
		}
	}

	log.Printf("extracted %d chars via %s (%s)", len(text), method, ct)

	if strings.Contains(accept, "application/json") {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tikaResponse{
			Content: text,
			Type:    ct,
			Method:  method,
		})
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(text))
	}
}

func (s *Server) extractPDF(data []byte) (string, string) {
	// Step 1: Try pdftotext
	text := s.pdftotext(data)
	if s.isGoodText(text, data) {
		return text, "pdftotext"
	}

	// Step 2: LLM Vision OCR
	llmText := s.llmOCR(data)
	if llmText != "" {
		return llmText, "llm_ocr"
	}

	// Step 3: Return whatever pdftotext got (better than nothing)
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
	// Count pages (rough: count "%%Page" or /Type /Page)
	pages := bytes.Count(pdfData, []byte("/Type /Page"))
	if pages < 1 {
		pages = 1
	}
	charsPerPage := len(text) / pages
	return charsPerPage >= s.cfg.Fallback.MinCharsPerPage
}

func (s *Server) llmOCR(data []byte) string {
	// PDF → images via pdftoppm
	tmpDir, err := os.MkdirTemp("", "taki-ocr-*")
	if err != nil {
		log.Printf("tmpdir error: %v", err)
		return ""
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "page")
	cmd := exec.Command("pdftoppm", "-png", "-r",
		fmt.Sprintf("%d", s.cfg.PDF.DPI),
		"-", prefix)
	cmd.Stdin = bytes.NewReader(data)
	if err := cmd.Run(); err != nil {
		log.Printf("pdftoppm error: %v", err)
		return ""
	}

	// Collect page images
	pages, _ := filepath.Glob(prefix + "-*.png")
	if len(pages) == 0 {
		return ""
	}
	if len(pages) > s.cfg.PDF.MaxPages {
		pages = pages[:s.cfg.PDF.MaxPages]
	}

	var allText []string
	for _, pagePath := range pages {
		imgData, err := os.ReadFile(pagePath)
		if err != nil {
			continue
		}

		b64 := base64.StdEncoding.EncodeToString(imgData)
		dataURL := "data:image/png;base64," + b64

		content := []contentBlock{
			{Type: "text", Text: "Extract all text from this scanned document page. Return the complete text content, preserving structure. If it's a form or contract, extract structured fields."},
			{Type: "image_url", ImageURL: &imageURL{URL: dataURL}},
		}

		reqBody := chatRequest{
			Model:     s.cfg.LLM.Model,
			MaxTokens: s.cfg.LLM.MaxTokens,
			Temp:      0.0,
			Messages: []chatMessage{
				{Role: "user", Content: content},
			},
		}

		jsonData, _ := json.Marshal(reqBody)
		url := strings.TrimRight(s.cfg.LLM.APIBase, "/") + "/chat/completions"

		resp, err := s.client.Post(url, "application/json", bytes.NewReader(jsonData))
		if err != nil {
			log.Printf("LLM request error: %v", err)
			continue
		}

		var chatResp chatResponse
		json.NewDecoder(resp.Body).Decode(&chatResp)
		resp.Body.Close()

		if len(chatResp.Choices) > 0 {
			text := chatResp.Choices[0].Message.Content
			// Strip <think>...</think> tags
			text = stripThinkTags(text)
			if text != "" {
				allText = append(allText, text)
			}
		}
	}

	return strings.Join(allText, "\n\n")
}

func stripThinkTags(s string) string {
	for {
		start := strings.Index(s, "<think>")
		if start < 0 {
			break
		}
		end := strings.Index(s, "</think>")
		if end < 0 {
			// unclosed think tag — strip to end
			s = s[:start]
			break
		}
		s = s[:start] + s[end+len("</think>"):]
	}
	return strings.TrimSpace(s)
}

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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"name":    "open_taki",
		"version": version,
		"llm":     s.cfg.LLM.APIBase,
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "open_taki %s\n", version)
}

func main() {
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	// Also accept config path as first positional arg
	if flag.NArg() > 0 {
		*configPath = flag.Arg(0)
	}

	cfg := loadConfig(*configPath)

	log.Printf("open_taki %s starting on %s", version, cfg.Listen)
	log.Printf("  LLM: %s (model: %s)", cfg.LLM.APIBase, cfg.LLM.Model)
	log.Printf("  PDF: %d DPI, max %d pages", cfg.PDF.DPI, cfg.PDF.MaxPages)
	log.Printf("  Fallback: pdftotext if >%d chars, >%d chars/page",
		cfg.Fallback.MinChars, cfg.Fallback.MinCharsPerPage)

	srv := NewServer(cfg)

	http.HandleFunc("/tika/text", srv.handleTikaText)
	http.HandleFunc("/tika", srv.handleHealth)
	http.HandleFunc("/version", srv.handleVersion)
	http.HandleFunc("/", srv.handleHealth)

	log.Fatal(http.ListenAndServe(cfg.Listen, nil))
}
