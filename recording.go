// recording.go — Recording-Modul für near-live Transkription.
//
// Taki ist der Orchestrator: VAD, Fragmentierung, Whisper (ASR),
// Diarization, Speaker-Profile, LLM-Finalpass, WebDAV-Upload.
// Der Browser ist ein dünner Client: Audio aufzeichnen, an Taki schicken,
// SSE-Events anzeigen.
//
// Routes:
//   POST /recording/session     — Session erstellen
//   POST /recording/session/end — Session beenden → LLM-Finalpass + WebDAV
//   POST /recording/chunk       — Audio-Chunk (WebM) → SSE (partial/final/speaker)
//   GET  /recording/sessions    — Session-Liste
//   GET/PUT /recording/speakers — Speaker-Profile verwalten

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── Config ───────────────────────────────────────────────────

type RecordingConfig struct {
	DiarizeAPIBase  string  `yaml:"diarize_api_base"`   // e.g. "http://microllm:8012/svc/steno-ml"
	DiarizeModel    string  `yaml:"diarize_model"`      // e.g. "pyannote/speaker-diarization-3.1"
	SpeakerStore    string  `yaml:"speaker_store"`      // e.g. "/data/speakers.json"
	SpeakerMatch    float64 `yaml:"speaker_match"`      // cosine threshold (default 0.75)
	MaxChunkMB      int     `yaml:"max_chunk_mb"`       // max size per chunk (default 50)
	SilenceThresh   float64 `yaml:"silence_thresh"`     // RMS below this = silence (default 0.01)
	SilenceTimeout  int     `yaml:"silence_timeout_ms"` // ms of silence → fragment end (default 1500)
	PartialInterval int     `yaml:"partial_interval_s"` // seconds between partial transcriptions (default 3)
	MaxFragmentSec  int     `yaml:"max_fragment_sec"`   // max fragment duration (default 30)
}

// ── Types ────────────────────────────────────────────────────

type RecordingSession struct {
	ID        string            `json:"id"`
	SpaceID   string            `json:"space_id"`
	UserID    string            `json:"user_id"`
	Created   time.Time         `json:"created"`
	Done      bool              `json:"done"`
	Fragments []RecordingFrag   `json:"fragments"`

	// Server-seitiger State (nicht serialisiert)
	mu              sync.Mutex
	fragAudio       []byte    // rohe Audio-Bytes des aktuellen Fragments
	fragStart       time.Time // Zeitpunkt Fragment-Start
	lastSilence     time.Time // letzter Zeitpunkt mit Stille
	speechActive    bool      // aktuell in Sprechphase
	silenceSince    time.Time // seit wann Stille
	lastPartial     time.Time // letzter Partial-Transcribe
	fragIndex       int       // laufende Fragment-Nummer
	prevTranscript  string    // Transkript bis letzte Sprechpause (Kontext)
	totalAudio      []byte    // komplettes Audio der Session
}

type RecordingFrag struct {
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

// ── RMS-VAD (server-seitig) ──────────────────────────────────
//
// Berechnet den RMS-Wert eines PCM16-Audio-Arrays (16kHz mono).
// WebM-Chunks werden vor der Analyse in PCM umgewandelt (ffmpeg).

func rmsFromPCM16(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		v := float64(s)
		sum += v * v
	}
	rms := math.Sqrt(sum / float64(len(samples)))
	// Normalisieren auf 0..1 (int16 range: 32768)
	return rms / 32768.0
}

// vadIsSilent prüft ob ein Audio-Chunk unterhalb der Stille-Schwelle ist.
func (s *Server) vadIsSilent(rms float64) bool {
	threshold := s.cfg.Recording.SilenceThresh
	if threshold <= 0 {
		threshold = 0.01
	}
	return rms < threshold
}

// ── Audio-Transcoding (WebM → PCM16 16kHz) ──────────────────

// decodeAudioToPCM16 konvertiert WebM/OGG/AAC in PCM16 16kHz mono.
// Nutzt ffmpeg als Subprocess.
func decodeAudioToPCM16(audioData []byte) []int16 {
	cmd := exec.Command("ffmpeg",
		"-i", "pipe:0",
		"-f", "s16le",
		"-acodec", "pcm_s16le",
		"-ar", "16000",
		"-ac", "1",
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(audioData)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		log.Printf("recording: ffmpeg decode error: %v", err)
		return nil
	}

	raw := out.Bytes()
	// int16 little-endian → []int16
	numSamples := len(raw) / 2
	samples := make([]int16, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return samples
}

// pcm16ToWAV wrappt PCM16 16kHz mono in einen WAV-Container.
// Nötig weil Whisper-API (vLLM) WAV erwartet.
func pcm16ToWAV(samples []int16) []byte {
	numSamples := uint32(len(samples))
	byteRate := uint32(16000 * 2 * 1) // 16kHz, 16-bit, mono
	dataSize := uint32(numSamples * 2)
	wav := make([]byte, 44+int(dataSize))

	// RIFF header
	copy(wav[0:4], "RIFF")
	binary.LittleEndian.PutUint32(wav[4:8], 36+dataSize)
	copy(wav[8:12], "WAVE")
	copy(wav[12:16], "fmt ")
	binary.LittleEndian.PutUint32(wav[16:20], 16)         // fmt chunk size
	binary.LittleEndian.PutUint16(wav[20:22], 1)          // PCM
	binary.LittleEndian.PutUint16(wav[22:24], 1)          // mono
	binary.LittleEndian.PutUint32(wav[24:28], 16000)      // sample rate
	binary.LittleEndian.PutUint32(wav[28:32], byteRate)   // byte rate
	binary.LittleEndian.PutUint16(wav[32:34], 2)          // block align
	binary.LittleEndian.PutUint16(wav[34:36], 16)         // bits per sample
	copy(wav[36:40], "data")
	binary.LittleEndian.PutUint32(wav[40:44], dataSize)

	// Samples
	for i, s := range samples {
		binary.LittleEndian.PutUint16(wav[44+i*2:], uint16(s))
	}
	return wav
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

// matchSpeaker findet das beste Speaker-Profil für einen Speaker-Label.
// openannote liefert "SPEAKER_00", "SPEAKER_01" etc. Wir mappen das
// über die Embeddings in SpeakerProfile.
func (s *Server) matchSpeaker(embedding []float64) (SpeakerProfile, float64, bool) {
	s.speakerMu.RLock()
	defer s.speakerMu.RUnlock()

	threshold := s.cfg.Recording.SpeakerMatch
	if threshold <= 0 {
		threshold = 0.75
	}

	bestProfile := SpeakerProfile{}
	bestScore := 0.0

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

// handleRecordingSession: POST = Session erstellen.
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

		sessionID := time.Now().Format("2006-01-02-1504") + "-" + randHex(4)

		session := &RecordingSession{
			ID:        sessionID,
			SpaceID:   body.SpaceID,
			UserID:    r.Header.Get("x-access-token"),
			Created:   time.Now(),
			Fragments: []RecordingFrag{},
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

// handleRecordingSessionEnd: POST /recording/session/end
// Beendet die Session: LLM-Finalpass auf komplettem Transkript,
// WebDAV-Upload (Audio + Transkript-JSON).
func (s *Server) handleRecordingSessionEnd(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
		return
	}

	s.recMu.Lock()
	session, exists := s.sessions[body.SessionID]
	if !exists {
		s.recMu.Unlock()
		writeChatError(w, http.StatusNotFound, "Session nicht gefunden")
		return
	}
	session.Done = true
	s.recMu.Unlock()

	// 1. Komplettes Transkript zusammenstellen
	var transcriptBuilder strings.Builder
	for _, frag := range session.Fragments {
		if frag.Speaker != "" && frag.Speaker != "unknown" {
			fmt.Fprintf(&transcriptBuilder, "[%s]: ", frag.Speaker)
		}
		transcriptBuilder.WriteString(frag.Text)
		transcriptBuilder.WriteString("\n")
	}
	fullTranscript := transcriptBuilder.String()

	if fullTranscript == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":      "done",
			"transcript":  "",
			"message":     "Kein Transkript (leere Session)",
		})
		return
	}

	// 2. LLM-Finalpass (Politur)
	finishedTranscript := fullTranscript
	if s.cfg.LLM.APIBase != "" {
		prompt := fmt.Sprintf(
			`Du erhältst ein Roh-Transkript einer Audio-Aufnahme. Korrigiere:
- Tippfehler und Erkennungsfehler
- Fehlende Satzzeichen (Punkte, Kommas, Frage-/Ausrufezeichen)
- Grammatik und Rechtschreibung
- Übermäßige Wiederholungen (Füllwörter)

Behalte die Sprecher-Zuordnungen bei ([Name]: Text).
Gib NUR den korrigierten Text zurück, keine Erklärungen.

Roh-Transkript:
%s`, fullTranscript)

		log.Printf("recording: LLM-Finalpass für Session %s (%d chars)", body.SessionID, len(fullTranscript))
		polished := s.llmChat(prompt)
		if polished != "" {
			finishedTranscript = polished
		}
	}

	// 3. WebDAV-Upload
	uploadPath := ""
	if session.SpaceID != "" {
		uploadPath = s.webdavUploadRecording(session, finishedTranscript)
	}

	// 4. Response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "done",
		"session_id": body.SessionID,
		"transcript": finishedTranscript,
		"upload":     uploadPath,
		"fragments":  len(session.Fragments),
	})
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
		// Kopie ohne internen State
		snap := *session
		snap.fragAudio = nil
		snap.totalAudio = nil
		sessions = append(sessions, snap)
	}
	s.recMu.Unlock()

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].Created.After(sessions[j].Created)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sessions)
}

// handleRecordingChunk: POST — Audio-Chunk (WebM) uploaden.
// Taki bufferst, VAD prüft, bei Sprechpause → Fragment fertig → Whisper+Diarize.
// Während Fragment läuft → Periodic-Partial.
//
// SSE-Events:
//   {type:"partial", fragment: N, text: "..."}
//   {type:"final", fragment: N, text: "...", speaker: "..."}
//   {type:"speaker", fragment: N, speaker: "..."}
//   {type:"done", fragment: N}
//   {type:"error", message: "..."}
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
					fmt.Sprintf("Audio zu groß (max. %d MB)", s.cfg.Recording.MaxChunkMB))
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
	if sessionID == "" {
		writeChatError(w, http.StatusBadRequest, "Feld 'session_id' fehlt")
		return
	}

	// SSE-Header
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

	// Session holen
	s.recMu.Lock()
	session, exists := s.sessions[sessionID]
	s.recMu.Unlock()

	if !exists {
		sseWrite(w, flusher, map[string]any{"type": "error", "message": "Session nicht gefunden"})
		return
	}
	if session.Done {
		sseWrite(w, flusher, map[string]any{"type": "error", "message": "Session bereits beendet"})
		return
	}

	// Audio in Session-Buffer einhängen + VAD-Auswertung
	fragmentIdx, fragmentComplete, partialText := s.processAudioChunk(session, audioData)

	// Partial-Text senden (Fragment noch offen)
	if partialText != "" {
		sseWrite(w, flusher, map[string]any{
			"type":     "partial",
			"fragment": fragmentIdx,
			"text":     partialText,
		})
	}

	// Fragment fertig → Whisper + Diarize parallel
	if fragmentComplete {
		sseWrite(w, flusher, map[string]any{
			"type":     "status",
			"fragment": fragmentIdx,
			"status":   "processing",
		})

		fragAudio := session.takeFragAudio()
		fragDuration := session.fragDuration()

		var text string
		var speaker = "unknown"
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			text = s.whisperTranscribeBytes(fragAudio)
		}()

		if s.cfg.Recording.DiarizeAPIBase != "" {
			wg.Add(1)
			go func() {
				defer wg.Done()
				diarizeResult := s.diarizeAudioBytes(fragAudio)
				if diarizeResult != nil && len(diarizeResult.Segments) > 0 {
					speaker = diarizeResult.Segments[0].Speaker
					// Speaker-Matching + Anreicherung
					s.enrichSpeakerProfile(session, speaker)
				}
			}()
		}

		wg.Wait()

		if text == "" {
			sseWrite(w, flusher, map[string]any{
				"type":     "error",
				"fragment": fragmentIdx,
				"message":  "Transkription fehlgeschlagen",
			})
			session.markFragFailed(fragmentIdx)
			return
		}

		session.addFragment(fragmentIdx, text, speaker, fragDuration)

		sseWrite(w, flusher, map[string]any{
			"type":     "final",
			"fragment": fragmentIdx,
			"text":     text,
			"method":   "whisper",
		})
		if speaker != "unknown" {
			sseWrite(w, flusher, map[string]any{
				"type":     "speaker",
				"fragment": fragmentIdx,
				"speaker":  speaker,
			})
		}

		sseWrite(w, flusher, map[string]any{
			"type":     "done",
			"fragment": fragmentIdx,
			"text":     text,
			"speaker":  speaker,
		})
	}
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

// ── Core: Audio-Chunk Verarbeitung + VAD ─────────────────────

// processAudioChunk hängt ein Audio-Chunk an die Session an,
// wertet VAD aus und entscheidet:
//   - Fragment fertig? (Sprechpause erkannt)
//   - Partial-Transkription fällig? (Intervall überschritten)
//
// Returns (fragmentIdx, fragmentComplete, partialText).
func (s *Server) processAudioChunk(session *RecordingSession, audioData []byte) (int, bool, string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	now := time.Now()
	silenceTimeout := time.Duration(s.cfg.Recording.SilenceTimeout) * time.Millisecond
	if s.cfg.Recording.SilenceTimeout <= 0 {
		silenceTimeout = 1500 * time.Millisecond
	}
	partialInterval := time.Duration(s.cfg.Recording.PartialInterval) * time.Second
	if s.cfg.Recording.PartialInterval <= 0 {
		partialInterval = 3 * time.Second
	}
	maxFragment := time.Duration(s.cfg.Recording.MaxFragmentSec) * time.Second
	if s.cfg.Recording.MaxFragmentSec <= 0 {
		maxFragment = 30 * time.Second
	}

	// Audio dekodieren + RMS
	samples := decodeAudioToPCM16(audioData)
	rms := 0.0
	if samples != nil {
		rms = rmsFromPCM16(samples)
	}

	isSilent := s.vadIsSilent(rms)

	// Audio in Session-Buffer einhängen
	session.totalAudio = append(session.totalAudio, audioData...)

	// VAD-State-Machine
	fragmentComplete := false

	if isSilent {
		if !session.silenceSince.IsZero() {
			// Bereits in Stille → Timer läuft
			silenceDur := now.Sub(session.silenceSince)
			if silenceDur >= silenceTimeout {
				// Sprechpause lang genug → Fragment fertig
				fragmentComplete = true
			}
		} else {
			// Stille beginnt
			session.silenceSince = now
		}
		session.speechActive = false
	} else {
		// Sprache erkannt
		if !session.silenceSince.IsZero() {
			// Stille war kürzer als Timeout → nicht wirklich Pause
			// (z.B. Komma-Pause), weiter sprechen
			session.silenceSince = time.Time{}
		}
		session.speechActive = true

		// Neues Fragment starten (wenn noch keins offen)
		if session.fragAudio == nil {
			session.fragStart = now
			session.fragIndex++
			session.lastPartial = time.Time{}
		}

		// Audio zum aktuellen Fragment hinzufügen
		session.fragAudio = append(session.fragAudio, audioData...)
	}

	// Max-Fragment-Dauer
	fragCompleteByDuration := false
	if session.fragAudio != nil && now.Sub(session.fragStart) >= maxFragment {
		fragCompleteByDuration = true
	}

	// Partial-Transkription prüfen
	var partialText string
	if !isSilent && session.fragAudio != nil &&
		session.lastPartial.IsZero() ||
		(!isSilent && session.fragAudio != nil &&
			now.Sub(session.lastPartial) >= partialInterval) {
		// Partial: Whisper auf bisherigem Fragment-Teil
		partialText = s.whisperTranscribeBytes(session.fragAudio)
		session.lastPartial = now
	}

	// Fragment abschließen (VAD oder Duration)
	if fragmentComplete || fragCompleteByDuration {
		fragmentComplete = fragmentComplete || fragCompleteByDuration
	}

	return session.fragIndex, fragmentComplete, partialText
}

// ── Session-Hilfsfunktionen ──────────────────────────────────

// takeFragAudio gibt das aktuelle Fragment-Audio zurück und resettet den Buffer.
func (session *RecordingSession) takeFragAudio() []byte {
	session.mu.Lock()
	defer session.mu.Unlock()
	audio := session.fragAudio
	session.fragAudio = nil
	session.silenceSince = time.Time{}
	session.speechActive = false
	session.lastPartial = time.Time{}
	session.prevTranscript = ""
	return audio
}

// fragDuration gibt die Dauer des aktuellen Fragments in Sekunden zurück.
func (session *RecordingSession) fragDuration() float64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.fragAudio == nil {
		return 0
	}
	// Näherung: WebM/Opus ~16 KB/s bei 16kHz
	return float64(len(session.fragAudio)) / 16000.0
}

// addFragment fügt ein abgeschlossenes Fragment zur Session hinzu.
func (session *RecordingSession) addFragment(idx int, text, speaker string, duration float64) {
	session.mu.Lock()
	defer session.mu.Unlock()

	start := float64(len(session.totalAudio) - len(session.fragAudio)) / 16000.0
	if start < 0 {
		start = 0
	}

	session.Fragments = append(session.Fragments, RecordingFrag{
		Index:    idx,
		Text:     text,
		Speaker:  speaker,
		Start:    start,
		End:      start + duration,
		Duration: duration,
		Status:   "done",
	})
	session.prevTranscript = text
}

// markFragFailed markiert ein Fragment als fehlgeschlagen.
func (session *RecordingSession) markFragFailed(idx int) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.fragAudio = nil
	session.silenceSince = time.Time{}
	session.speechActive = false
	session.lastPartial = time.Time{}
}

// ── Speaker-Profile-Anreicherung ─────────────────────────────

// enrichSpeakerProfile: Wenn ein Speaker-Label von Diarization kommt,
// prüfen ob es einem bekannten Profil zugeordnet werden kann.
// Aktuell: Label durchreichen (openannote liefert SPEAKER_00 etc.).
// Später: Embedding-basiertes Matching.
func (s *Server) enrichSpeakerProfile(session *RecordingSession, speakerLabel string) {
	// Aktuell nur Log — Embedding-Matching kommt mit wav2vec2-Integration
	log.Printf("recording: speaker %s in Session %s", speakerLabel, session.ID)
}

// ── WebDAV-Upload ────────────────────────────────────────────

// webdavUploadRecording legt die Session unter
// <personal>/recordings/<yyyy-mm-dd-hh-mm>/<session_id>/ ab.
// Liefert den relativen Pfad zurück (leer bei Fehler).
func (s *Server) webdavUploadRecording(session *RecordingSession, transcript string) string {
	if session.SpaceID == "" {
		return ""
	}

	datetime := session.Created.Format("2006-01-02-1504")
	basePath := fmt.Sprintf("/recordings/%s/%s", datetime, session.ID)
	davBase := strings.TrimRight(s.cfg.OpenCloud.URL, "/") + "/dav/spaces"

	// 1. Verzeichnisse anlegen (MKCOL)
	for _, dir := range []string{
		"/recordings",
		fmt.Sprintf("/recordings/%s", datetime),
		basePath,
	} {
		s.webdavMkcol(davBase+"/"+session.SpaceID+dir)
	}

	// 2. Transkript-JSON
	transcriptJSON, _ := json.MarshalIndent(map[string]any{
		"session_id": session.ID,
		"created":    session.Created,
		"transcript": transcript,
		"fragments":  session.Fragments,
	}, "", "  ")
	s.webdavPut(davBase+"/"+session.SpaceID+basePath+"/transkript.json", transcriptJSON)

	// 3. Audio (komplettes Session-Audio)
	if len(session.totalAudio) > 0 {
		s.webdavPut(davBase+"/"+session.SpaceID+basePath+"/aufnahme.webm", session.totalAudio)
	}

	return basePath
}

// webdavMkcol erzeugt ein WebDAV-Verzeichnis (idempotent).
func (s *Server) webdavMkcol(url string) {
	req, err := http.NewRequest("MKCOL", url, nil)
	if err != nil {
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("recording: MKCOL %s: %v", url, err)
		return
	}
	resp.Body.Close()
	// 404/405 = existiert schon → ok
}

// webdavPut lädt eine Datei via WebDAV PUT hoch.
func (s *Server) webdavPut(url string, data []byte) {
	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("recording: PUT %s: %v", url, err)
		return
	}
	resp.Body.Close()
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

// whisperTranscribeBytes transkribiert Audio-Bytes.
// WebM/Opus → WAV (16kHz PCM) → Whisper-API.
func (s *Server) whisperTranscribeBytes(audioData []byte) string {
	url := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"

	// WebM → WAV umwandeln
	samples := decodeAudioToPCM16(audioData)
	if samples == nil {
		log.Printf("recording: Audio-Dekodierung fehlgeschlagen (%d bytes)", len(audioData))
		return ""
	}
	wavData := pcm16ToWAV(samples)

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteField("model", s.cfg.Whisper.Model)
	w.WriteField("language", "de")
	w.WriteFile("file", "audio.wav", bytes.NewReader(wavData))
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
