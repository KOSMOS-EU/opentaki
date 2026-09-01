// recording.go — Recording-Modul für near-live Transkription.
//
// Taki orchestriert: Whisper (ASR) + openannote (Diarization) + WebDAV-Upload.
// Browser sendet nur WebM-Chunks, Taki liefert SSE-Response mit Live-Text.
//
// Routes:
//   POST /recording/session   — Session erstellen (oder GET für Liste)
//   POST /recording/chunk     — Chunk uploaden → SSE: Whisper + Diarize + WebDAV
//   GET  /recording/sessions  — Session-Liste
//   GET/PUT /recording/speakers — Speaker-Profile verwalten

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Config ───────────────────────────────────────────────────

type RecordingConfig struct {
	DiarizeAPIBase string `yaml:"diarize_api_base"` // e.g. "http://microllm:8012/svc/steno-ml"
	DiarizeModel   string `yaml:"diarize_model"`    // e.g. "pyannote/speaker-diarization-3.1"
	SpeakerStore   string `yaml:"speaker_store"`    // e.g. "/data/speakers.json"
	MaxChunkMB     int    `yaml:"max_chunk_mb"`     // default 50
	SessionDir     string `yaml:"session_dir"`      // default ".recordings"
}

// ── Types ────────────────────────────────────────────────────

type RecordingSession struct {
	ID       string           `json:"id"`
	SpaceID  string           `json:"space_id"`
	UserID   string           `json:"user_id"`
	Created  time.Time        `json:"created"`
	Chunks   []RecordingChunk `json:"chunks"`
	Done     bool             `json:"done"`
}

type RecordingChunk struct {
	Index    int     `json:"index"`
	Text     string  `json:"text"`
	Speaker  string  `json:"speaker"`
	Start    float64 `json:"start"`
	End      float64 `json:"end"`
	Duration float64 `json:"duration"`
	Status   string  `json:"status"` // "processing" | "done" | "failed"
}

type SpeakerProfile struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Embedding []float64 `json:"embedding"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Count     int       `json:"count"`
}

// ── Diarize response (openannote format) ────────────────────

type diarizeResponse struct {
	Segments []diarizeSegment `json:"segments"`
	Speakers []string         `json:"speakers"`
}

type diarizeSegment struct {
	Speaker string  `json:"speaker"`
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
}

// ── SSE helpers ──────────────────────────────────────────────

func sseWrite(w http.ResponseWriter, flusher http.Flusher, event map[string]any) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "data: %s\n\n", data)
	if flusher != nil {
		flusher.Flush()
	}
}

// ── Speaker store ────────────────────────────────────────────

func (s *Server) loadSpeakers() {
	s.speakerMu.Lock()
	defer s.speakerMu.Unlock()
	s.loadSpeakersLocked()
}

func (s *Server) loadSpeakersLocked() {
	path := s.cfg.Recording.SpeakerStore
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			s.speakers = []SpeakerProfile{}
			return
		}
		log.Printf("recording: speaker load error: %v", err)
		return
	}
	if err := json.Unmarshal(data, &s.speakers); err != nil {
		log.Printf("recording: speaker parse error: %v", err)
	}
}

func (s *Server) saveSpeakers() {
	s.speakerMu.Lock()
	defer s.speakerMu.Unlock()
	s.saveSpeakersLocked()
}

func (s *Server) saveSpeakersLocked() {
	path := s.cfg.Recording.SpeakerStore
	if path == "" {
		return
	}
	data, err := json.MarshalIndent(s.speakers, "", "  ")
	if err != nil {
		log.Printf("recording: speaker save error: %v", err)
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("recording: speaker dir error: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("recording: speaker write error: %v", err)
	}
}

// cosineSimilarity berechnet die Cosine-Similarität zweier Vektoren.
func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// matchSpeaker findet das beste Speaker-Profil für ein Embedding.
// Gibt (profil, similarität) zurück; similarität < threshold → kein Match.
func (s *Server) matchSpeaker(embedding []float64) (SpeakerProfile, float64, bool) {
	s.speakerMu.RLock()
	defer s.speakerMu.RUnlock()

	bestProfile := SpeakerProfile{}
	bestScore := 0.0
	threshold := 0.75 // wie opensteno speaker_pool.py

	for _, profile := range s.speakers {
		score := cosineSimilarity(embedding, profile.Embedding)
		if score > bestScore {
			bestScore = score
			bestProfile = profile
		}
	}

	if bestScore >= threshold {
		return bestProfile, bestScore, true
	}
	return SpeakerProfile{}, bestScore, false
}

// ── Routes ───────────────────────────────────────────────────

// handleRecordingSession: POST = Session erstellen, GET = Session-Liste.
func (s *Server) handleRecordingSession(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}

	switch r.Method {
	case http.MethodPost:
		var body struct {
			SpaceID string `json:"space_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
			return
		}

		sessionID := fmt.Sprintf("%s-%s", time.Now().Format("2006-01-02"),
			randHex(4))

		session := &RecordingSession{
			ID:      sessionID,
			SpaceID: body.SpaceID,
			UserID:  r.Header.Get("x-access-token"),
			Created: time.Now(),
			Chunks:  []RecordingChunk{},
		}

		s.recMu.Lock()
		s.sessions[sessionID] = session
		s.recMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(session)

	case http.MethodGet:
		s.handleRecordingSessions(w, r)

	default:
		http.Error(w, "POST or GET only", http.StatusMethodNotAllowed)
	}
}

// handleRecordingSessions: GET — Session-Liste.
func (s *Server) handleRecordingSessions(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}

	s.recMu.Lock()
	sessions := make([]RecordingSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, *session)
	}
	s.recMu.Unlock()

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].Created.After(sessions[j].Created)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sessions)
}

// handleRecordingChunk: POST — Audio-Chunk uploaden, SSE-Response mit
// Live-Transkription (Whisper) + Speaker (Diarization).
func (s *Server) handleRecordingChunk(w http.ResponseWriter, r *http.Request) {
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

	maxBytes := int64(s.cfg.Recording.MaxChunkMB) * 1024 * 1024
	if maxBytes <= 0 {
		maxBytes = 50 * 1024 * 1024
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes+1024*1024)
	mr, err := r.MultipartReader()
	if err != nil {
		writeChatError(w, http.StatusBadRequest, "multipart/form-data erwartet: "+err.Error())
		return
	}

	var audioData []byte
	var sessionID string
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeChatError(w, http.StatusBadRequest, "multipart-Fehler: "+err.Error())
			return
		}
		switch part.FormName() {
		case "file":
			audioData, err = io.ReadAll(io.LimitReader(part, maxBytes+1))
			if err != nil {
				writeChatError(w, http.StatusBadRequest, "Audio-Fehler: "+err.Error())
				return
			}
			if len(audioData) > int(maxBytes) {
				writeChatError(w, http.StatusRequestEntityTooLarge,
					fmt.Sprintf("Audio-Datei zu groß (max. %d MB)", s.cfg.Recording.MaxChunkMB))
				return
			}
		case "session_id":
			buf := new(bytes.Buffer)
			io.CopyN(buf, part, 256)
			sessionID = strings.TrimSpace(buf.String())
		}
	}

	if audioData == nil {
		writeChatError(w, http.StatusBadRequest, "Feld 'file' fehlt")
		return
	}

	// SSE-Header setzen
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeChatError(w, http.StatusInternalServerError, "Streaming nicht unterstützt")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	// 1+2. Whisper + Diarize parallel (unabhängig, gleicher Audio-Buffer)
	sseWrite(w, flusher, map[string]any{"type": "status", "status": "processing"})

	var text string
	var speaker = "unknown"
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		text = s.whisperTranscribeBytes(audioData)
	}()

	diarizeBase := s.cfg.Recording.DiarizeAPIBase
	if diarizeBase != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			diarizeResult := s.diarizeAudioBytes(audioData)
			if diarizeResult != nil && len(diarizeResult.Segments) > 0 {
				speaker = diarizeResult.Segments[0].Speaker
			}
		}()
	}

	wg.Wait()

	if text == "" {
		sseWrite(w, flusher, map[string]any{"type": "error", "message": "Transkription fehlgeschlagen"})
		return
	}
	sseWrite(w, flusher, map[string]any{"type": "final", "text": text, "method": "whisper"})
	if speaker != "unknown" {
		sseWrite(w, flusher, map[string]any{"type": "speaker", "speaker": speaker})
	}

	// 3. Session-Update
	chunkIndex := 0
	if sessionID != "" {
		s.recMu.Lock()
		if session, exists := s.sessions[sessionID]; exists {
			chunkIndex = len(session.Chunks)
			session.Chunks = append(session.Chunks, RecordingChunk{
				Index:   chunkIndex,
				Text:    text,
				Speaker: speaker,
				Status:  "done",
			})
		}
		s.recMu.Unlock()
	}

	// 4. Done
	sseWrite(w, flusher, map[string]any{
		"type":      "done",
		"chunk_id":  fmt.Sprintf("chunk_%04d", chunkIndex),
		"text":      text,
		"speaker":   speaker,
	})
}

// handleRecordingSpeakers: GET = Liste, PUT = Profil anlegen/aktualisieren.
func (s *Server) handleRecordingSpeakers(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.speakerMu.RLock()
		speakers := make([]SpeakerProfile, len(s.speakers))
		copy(speakers, s.speakers)
		s.speakerMu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(speakers)

	case http.MethodPut:
		var profile SpeakerProfile
		if err := json.NewDecoder(r.Body).Decode(&profile); err != nil {
			writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
			return
		}
		if profile.ID == "" {
			sum := sha256.Sum256([]byte(profile.Name + time.Now().Format(time.RFC3339)))
			profile.ID = hex.EncodeToString(sum[:8])
		}
		if profile.FirstSeen.IsZero() {
			profile.FirstSeen = time.Now()
		}
		profile.LastSeen = time.Now()
		profile.Count++

		s.speakerMu.Lock()
		// Bestehendes Profil ersetzen
		found := false
		for i, existing := range s.speakers {
			if existing.ID == profile.ID {
				s.speakers[i] = profile
				found = true
				break
			}
		}
		if !found {
			s.speakers = append(s.speakers, profile)
		}
		s.saveSpeakersLocked()
		s.speakerMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(profile)

	default:
		http.Error(w, "GET or PUT only", http.StatusMethodNotAllowed)
	}
}

// ── Diarization ──────────────────────────────────────────────

func (s *Server) diarizeAudioBytes(audioData []byte) *diarizeResponse {
	url := strings.TrimRight(s.cfg.Recording.DiarizeAPIBase, "/") + "/diarize"

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	if s.cfg.Recording.DiarizeModel != "" {
		w.WriteField("model", s.cfg.Recording.DiarizeModel)
	}
	w.WriteFile("file", "chunk.webm", bytes.NewReader(audioData))
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		log.Printf("recording: diarize request error: %v", err)
		return nil
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("recording: diarize error: %v", err)
		return nil
	}
	defer resp.Body.Close()

	var result diarizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("recording: diarize decode error: %v", err)
		return nil
	}
	return &result
}

// ── Whisper (bytes statt file) ──────────────────────────────

// whisperTranscribeBytes transkribiert Audio-Bytes direkt (ohne Temp-Datei).
// Nutzt dieselbe Whisper-API wie whisperTranscribe, aber mit Bytes-Input.
func (s *Server) whisperTranscribeBytes(audioData []byte) string {
	url := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteField("model", s.cfg.Whisper.Model)
	w.WriteField("language", "de")
	w.WriteFile("file", "chunk.webm", bytes.NewReader(audioData))
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		log.Printf("recording: whisper request error: %v", err)
		return ""
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("recording: whisper error: %v", err)
		return ""
	}
	defer resp.Body.Close()

	var wr whisperResponse
	if err := json.NewDecoder(resp.Body).Decode(&wr); err != nil {
		log.Printf("recording: whisper decode error: %v", err)
		return ""
	}
	return strings.TrimSpace(wr.Text)
}

// ── Helpers ──────────────────────────────────────────────────

func randHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
	}
	return string(b)
}
