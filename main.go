// open_taki — lightweight Tika replacement with LLM-powered extraction
//
// Drop-in replacement for Apache Tika's text extraction endpoint.
// Supports PDF, Office, images, audio, HTML, email, and archives.
//
// Extraction methods:
//   - PDF: pdftotext fast-path, LLM Vision OCR fallback
//   - Images: LLM Vision (content description, not just EXIF)
//   - Audio: Whisper transcription (via OpenAI-compatible API)
//   - Office: pandoc/libreoffice conversion
//   - Archives: external tools (unzip, 7z, unrar) + recursive extraction
//   - Email: built-in RFC822 parser
//
// Tika-compatible API:
//   PUT /tika/text     → extract text from document body
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
	"mime"
	"net/http"
	"net/mail"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const version = "0.2.0"

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

// OpenAI chat completion request/response
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

// Whisper transcription response
type whisperResponse struct {
	Text string `json:"text"`
}

// Tika-compatible JSON response
type tikaResponse struct {
	Content string `json:"X-TIKA:content"`
	Type    string `json:"Content-Type"`
	Method  string `json:"X-TAKI:method"`
}

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

// ── Main handler ─────────────────────────────────────────────

func (s *Server) handleTikaText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 200*1024*1024)) // 200MB max
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	ct := r.Header.Get("Content-Type")
	accept := r.Header.Get("Accept")

	text, method := s.extract(body, ct)

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

func (s *Server) extract(body []byte, ct string) (string, string) {
	ct = strings.ToLower(ct)

	switch {
	// PDF
	case strings.Contains(ct, "pdf"):
		return s.extractPDF(body)

	// Images → LLM Vision
	case strings.HasPrefix(ct, "image/"):
		return s.extractImage(body, ct)

	// Audio → Whisper
	case strings.HasPrefix(ct, "audio/"):
		return s.extractAudio(body, ct)

	// Video → extract audio track → Whisper
	case strings.HasPrefix(ct, "video/"):
		return s.extractVideo(body, ct)

	// Office documents
	case strings.Contains(ct, "wordprocessingml"),
		strings.Contains(ct, "msword"),
		strings.Contains(ct, "opendocument.text"),
		strings.Contains(ct, "rtf"):
		return s.extractOfficeDoc(body)

	case strings.Contains(ct, "spreadsheetml"),
		strings.Contains(ct, "ms-excel"),
		strings.Contains(ct, "opendocument.spreadsheet"):
		return s.extractSpreadsheet(body)

	case strings.Contains(ct, "presentationml"),
		strings.Contains(ct, "ms-powerpoint"),
		strings.Contains(ct, "opendocument.presentation"):
		return s.extractPresentation(body)

	// Email
	case strings.Contains(ct, "rfc822"),
		strings.Contains(ct, "message/"):
		return s.extractEmail(body)

	// HTML
	case strings.Contains(ct, "text/html"),
		strings.Contains(ct, "xhtml"):
		return s.extractHTML(body)

	// Archives
	case strings.Contains(ct, "zip"),
		strings.Contains(ct, "x-tar"),
		strings.Contains(ct, "x-7z"),
		strings.Contains(ct, "x-rar"):
		return s.extractArchive(body, ct)

	// Plain text
	case strings.Contains(ct, "text/"):
		return string(body), "passthrough"

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
	if len(pages) > s.cfg.PDF.MaxPages {
		pages = pages[:s.cfg.PDF.MaxPages]
	}

	var allText []string
	for _, pagePath := range pages {
		text := s.llmDescribe(pagePath,
			"Extract all text from this scanned document page. "+
				"Return the complete text content, preserving structure. "+
				"If it's a form or contract, extract structured fields.")
		if text != "" {
			allText = append(allText, text)
		}
	}
	return strings.Join(allText, "\n\n")
}

// ── Image extraction (LLM Vision) ───────────────────────────

func (s *Server) extractImage(data []byte, ct string) (string, string) {
	// Determine image format for data URL
	mediaType := "image/png"
	if strings.Contains(ct, "jpeg") || strings.Contains(ct, "jpg") {
		mediaType = "image/jpeg"
	} else if strings.Contains(ct, "webp") {
		mediaType = "image/webp"
	} else if strings.Contains(ct, "gif") {
		mediaType = "image/gif"
	} else if strings.Contains(ct, "tiff") {
		mediaType = "image/tiff"
	} else if strings.Contains(ct, "bmp") {
		mediaType = "image/bmp"
	}

	b64 := base64.StdEncoding.EncodeToString(data)
	dataURL := "data:" + mediaType + ";base64," + b64

	prompt := "Describe this image in detail. " +
		"If it contains text, extract all text. " +
		"If it's a document, form, or scan, extract all structured content. " +
		"If it's a photo, describe what you see including objects, people, settings, and any visible text or signs."

	text := s.llmVision(dataURL, prompt)
	if text != "" {
		return text, "llm_vision"
	}
	return "[image not describable]", "error"
}

// ── Audio extraction (Whisper) ───────────────────────────────

func (s *Server) extractAudio(data []byte, ct string) (string, string) {
	if s.cfg.Whisper.APIBase == "" {
		return "[audio transcription not configured - set whisper.api_base]", "skipped"
	}

	ext := ".wav"
	if strings.Contains(ct, "mp3") || strings.Contains(ct, "mpeg") {
		ext = ".mp3"
	} else if strings.Contains(ct, "ogg") {
		ext = ".ogg"
	} else if strings.Contains(ct, "flac") {
		ext = ".flac"
	} else if strings.Contains(ct, "m4a") || strings.Contains(ct, "mp4") {
		ext = ".m4a"
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
		return "[video transcription not configured - set whisper.api_base]", "skipped"
	}

	// Extract audio track with ffmpeg
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
	// OpenAI-compatible /v1/audio/transcriptions endpoint
	url := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"

	f, err := os.Open(audioPath)
	if err != nil {
		return ""
	}
	defer f.Close()

	// Build multipart request
	var buf bytes.Buffer
	boundary := "----TakiBoundary" + fmt.Sprintf("%d", time.Now().UnixNano())
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
	return s.convertViaPandoc(data, "docx")
}

func (s *Server) extractSpreadsheet(data []byte) (string, string) {
	// libreoffice → csv
	tmp, err := os.CreateTemp("", "taki-*.xlsx")
	if err != nil {
		return "", "error"
	}
	defer os.Remove(tmp.Name())
	tmp.Write(data)
	tmp.Close()

	tmpDir, _ := os.MkdirTemp("", "taki-lo-*")
	defer os.RemoveAll(tmpDir)

	cmd := exec.Command("libreoffice", "--headless",
		"--convert-to", "csv:Text - txt - csv (StarCalc):44,34,76,1",
		"--outdir", tmpDir, tmp.Name())
	cmd.Run()

	// Read all csv files
	csvs, _ := filepath.Glob(filepath.Join(tmpDir, "*.csv"))
	var parts []string
	for _, csvPath := range csvs {
		content, _ := os.ReadFile(csvPath)
		parts = append(parts, string(content))
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n---\n"), "libreoffice"
	}
	return "[spreadsheet extraction failed]", "error"
}

func (s *Server) extractPresentation(data []byte) (string, string) {
	return s.convertViaPandoc(data, "pptx")
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
		// Fallback: libreoffice → txt
		return s.convertViaLibreOffice(tmp.Name())
	}
	text := strings.TrimSpace(out.String())
	if text != "" {
		return text, "pandoc"
	}
	return s.convertViaLibreOffice(tmp.Name())
}

func (s *Server) convertViaLibreOffice(path string) (string, string) {
	tmpDir, _ := os.MkdirTemp("", "taki-lo-*")
	defer os.RemoveAll(tmpDir)

	cmd := exec.Command("libreoffice", "--headless",
		"--convert-to", "txt:Text", "--outdir", tmpDir, path)
	cmd.Run()

	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	txtPath := filepath.Join(tmpDir, base+".txt")
	content, err := os.ReadFile(txtPath)
	if err != nil {
		return "[office conversion failed]", "error"
	}
	return strings.TrimSpace(string(content)), "libreoffice"
}

// ── Email ────────────────────────────────────────────────────

func (s *Server) extractEmail(data []byte) (string, string) {
	msg, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		// Fallback: raw strings
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

// ── Archives ─────────────────────────────────────────────────

func (s *Server) extractArchive(data []byte, ct string) (string, string) {
	tmpDir, _ := os.MkdirTemp("", "taki-archive-*")
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, "archive")
	os.WriteFile(archivePath, data, 0644)

	var extractCmd *exec.Cmd
	outDir := filepath.Join(tmpDir, "out")
	os.MkdirAll(outDir, 0755)

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

	// Recursively extract text from all files in archive
	var allText []string
	filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Size() == 0 || info.Size() > 50*1024*1024 {
			return nil
		}

		fileData, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		// Detect content type
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

// ── Magic byte detection fallback ────────────────────────────

func (s *Server) extractByMagic(body []byte) (string, string) {
	if len(body) > 4 && string(body[:4]) == "%PDF" {
		return s.extractPDF(body)
	}
	if len(body) > 2 && body[0] == 0x50 && body[1] == 0x4B { // PK = ZIP
		return s.extractArchive(body, "application/zip")
	}
	if utf8.Valid(body) {
		return string(body), "passthrough"
	}
	return "[unknown format]", "error"
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
	case ".gif":
		mediaType = "image/gif"
	}

	b64 := base64.StdEncoding.EncodeToString(imgData)
	return s.llmVision("data:"+mediaType+";base64,"+b64, prompt)
}

func (s *Server) llmVision(dataURL, prompt string) string {
	content := []contentBlock{
		{Type: "text", Text: prompt},
		{Type: "image_url", ImageURL: &imageURL{URL: dataURL}},
	}

	reqBody := chatRequest{
		Model:     s.cfg.LLM.Model,
		MaxTokens: s.cfg.LLM.MaxTokens,
		Temp:      0.0,
		Messages:  []chatMessage{{Role: "user", Content: content}},
	}

	jsonData, _ := json.Marshal(reqBody)
	url := strings.TrimRight(s.cfg.LLM.APIBase, "/") + "/chat/completions"

	resp, err := s.client.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		log.Printf("LLM error: %v", err)
		return ""
	}
	defer resp.Body.Close()

	var chatResp chatResponse
	json.NewDecoder(resp.Body).Decode(&chatResp)

	if len(chatResp.Choices) > 0 {
		return stripThinkTags(chatResp.Choices[0].Message.Content)
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

// ── Multipart writer (minimal, for Whisper API) ──────────────

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
	features := map[string]bool{
		"pdf":     true,
		"office":  true,
		"image":   true,
		"audio":   s.cfg.Whisper.APIBase != "",
		"archive": true,
		"email":   true,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "ok",
		"name":     "open_taki",
		"version":  version,
		"llm":      s.cfg.LLM.APIBase,
		"whisper":  s.cfg.Whisper.APIBase,
		"features": features,
	})
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "open_taki %s\n", version)
}

func main() {
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	if flag.NArg() > 0 {
		*configPath = flag.Arg(0)
	}

	cfg := loadConfig(*configPath)

	log.Printf("open_taki %s starting on %s", version, cfg.Listen)
	log.Printf("  LLM:     %s (model: %s)", cfg.LLM.APIBase, cfg.LLM.Model)
	if cfg.Whisper.APIBase != "" {
		log.Printf("  Whisper: %s (model: %s)", cfg.Whisper.APIBase, cfg.Whisper.Model)
	} else {
		log.Printf("  Whisper: disabled (no whisper.api_base configured)")
	}
	log.Printf("  PDF:     %d DPI, max %d pages", cfg.PDF.DPI, cfg.PDF.MaxPages)

	srv := NewServer(cfg)

	http.HandleFunc("/tika/text", srv.handleTikaText)
	http.HandleFunc("/tika", srv.handleHealth)
	http.HandleFunc("/version", srv.handleVersion)
	http.HandleFunc("/", srv.handleHealth)

	log.Fatal(http.ListenAndServe(cfg.Listen, nil))
}
