// recording.go — Recording-Modul für near-live Transkription.
//
// Pipeline (siehe PIPELINE.md):
//   1. Chunk-Empfang → VAD → Fragment
//   2. Transkription (Whisper schnell) → SSE
//   3. Word-Timestamps (Whisper DTW, async)
//   4. Diarization (pyannote pro Fragment)
//   5. Segmentierung (Word-TS + Satzgrenzen-Korrektur)
//   6-11. Session-End: Flush, Utterances, LLM-Finalpass, Upload
//
// Routes:
//   POST /recording/session     — Session erstellen
//   POST /recording/session/end — Session beenden
//   POST /recording/chunk       — Audio-Chunk
//   GET  /recording/sessions    — Session-Liste
//   GET/PUT /recording/speakers — Speaker-Profile verwalten

package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
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

	_ "modernc.org/sqlite"
)

// ── Config ───────────────────────────────────────────────────

type RecordingConfig struct {
	DiarizeAPIBase  string  `yaml:"diarize_api_base"`
	DiarizeModel    string  `yaml:"diarize_model"`
	MinSpeakers     int     `yaml:"min_speakers"`
	MaxSpeakers     int     `yaml:"max_speakers"`
	SpeakerStore    string  `yaml:"speaker_store"`
	ProfileMatch    float64 `yaml:"profile_match"`
	MaxChunkMB      int     `yaml:"max_chunk_mb"`
	SilenceThresh   float64 `yaml:"silence_thresh"`
	SilenceTimeout  int     `yaml:"silence_timeout_ms"`
	SoftLimitSec    int     `yaml:"soft_limit_sec"`
	SoftSilenceMs   int     `yaml:"soft_silence_ms"`
	PartialInterval int     `yaml:"partial_interval_s"`
	MaxFragmentSec  int     `yaml:"max_fragment_sec"`
	LLMFinalpass    *bool   `yaml:"llm_finalpass"`
}

func (c *RecordingConfig) silenceThresh() float64 {
	if c.SilenceThresh > 0 { return c.SilenceThresh }
	return 0.03
}
func (c *RecordingConfig) silenceTimeoutMs() int {
	if c.SilenceTimeout > 0 { return c.SilenceTimeout }
	return 800
}
func (c *RecordingConfig) softLimitSec() int {
	if c.SoftLimitSec > 0 { return c.SoftLimitSec }
	return 15
}
func (c *RecordingConfig) softSilenceMs() int {
	if c.SoftSilenceMs > 0 { return c.SoftSilenceMs }
	return 200
}
func (c *RecordingConfig) maxFragmentSec() int {
	if c.MaxFragmentSec > 0 { return c.MaxFragmentSec }
	return 60
}
func (c *RecordingConfig) minSpeakers() int {
	if c.MinSpeakers > 0 { return c.MinSpeakers }
	return 2
}
func (c *RecordingConfig) profileMatchThreshold() float64 {
	if c.ProfileMatch > 0 { return c.ProfileMatch }
	return 0.65
}
func (c *RecordingConfig) doLLMFinalpass() bool {
	if c.LLMFinalpass != nil { return *c.LLMFinalpass }
	return true
}

// ── Types ────────────────────────────────────────────────────

type WordTimestamp struct {
	Word  string  `json:"word"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

type RecordingFrag struct {
	Index    int              `json:"index"`
	Text     string           `json:"text"`
	Speaker  string           `json:"speaker"`
	Start    float64          `json:"start"`
	End      float64          `json:"end"`
	Duration float64          `json:"duration"`
	Status   string           `json:"status"`
	Segments []FragSpeakerSeg `json:"segments,omitempty"`
	Words    []WordTimestamp  `json:"words,omitempty"`
}

type FragSpeakerSeg struct {
	Speaker string  `json:"speaker"`
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Text    string  `json:"text"`
}

type Utterance struct {
	Speaker string  `json:"speaker"`
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Text    string  `json:"text"`
}

type RecordingSession struct {
	ID        string          `json:"id"`
	SpaceID   string          `json:"space_id"`
	UserID    string          `json:"user_id"`
	Created   time.Time       `json:"created"`
	Done      bool            `json:"done"`
	Fragments []RecordingFrag `json:"fragments"`

	mu                 sync.Mutex
	fragAudio          []byte
	fragStartSamples   int
	speechActive       bool
	silenceSinceSamples int
	maxSilenceSamples  int
	minRMS             float64
	lastPartialSamples int
	fragIndex          int
	prevTranscript     string
	totalAudio         []byte
	totalSamples       int
	shareToken         string
	sharePasswd        string
	wordWg             sync.WaitGroup
}

type SpeakerPerson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type SpeakerProfile struct {
	ID        int       `json:"id"`
	PersonID  int       `json:"person_id"`
	Embedding []float64 `json:"-"`
	Source    string    `json:"source"`
	FirstSeen string    `json:"first_seen"`
	LastSeen  string    `json:"last_seen"`
}

type SpeakerRef struct {
	PersonName string
	PersonID   int
	ProfileID  int
}

func (r SpeakerRef) String() string {
	if r.ProfileID <= 0 {
		return r.PersonName
	}
	return fmt.Sprintf("%s/%d", r.PersonName, r.ProfileID)
}

type diarizeResponse struct {
	Segments          []diarizeSegment           `json:"segments"`
	Speakers          []string                   `json:"speakers"`
	SpeakerEmbeddings map[string][]float64       `json:"speaker_embeddings"`
}

type diarizeSegment struct {
	Speaker string  `json:"speaker"`
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
}

type whisperTranscribeResult struct {
	Text  string
	Words []WordTimestamp
}

// ── Helpers ──────────────────────────────────────────────────

func sseWrite(w http.ResponseWriter, flusher http.Flusher, event map[string]any) {
	data, _ := json.Marshal(event)
	fmt.Fprintf(w, "data: %s\n\n", data)
	if flusher != nil {
		flusher.Flush()
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func endsWithSentence(word string) bool {
	if len(word) == 0 {
		return false
	}
	last := word[len(word)-1]
	return last == '.' || last == '?' || last == '!' || last == ';'
}

func randHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
	}
	return string(b)
}

// ── Audio ────────────────────────────────────────────────────

func rmsFromPCM16(samples []int16) float64 {
	if len(samples) == 0 {
		return 0
	}
	var sum float64
	for _, s := range samples {
		v := float64(s)
		sum += v * v
	}
	return math.Sqrt(sum/float64(len(samples))) / 32768.0
}

func (s *Server) vadIsSilent(rms float64) bool {
	return rms < s.cfg.Recording.silenceThresh()
}

func decodeAudioToPCM16(audioData []byte) []int16 {
	if len(audioData) < 2 {
		return nil
	}
	isWebM := bytes.HasPrefix(audioData, []byte{0x1A, 0x45, 0xDF, 0xA3})
	isWAV := bytes.HasPrefix(audioData, []byte("RIFF"))
	isOGG := bytes.HasPrefix(audioData, []byte("OggS"))
	if !isWebM && !isWAV && !isOGG {
		if len(audioData)%2 != 0 {
			audioData = audioData[:len(audioData)-1]
		}
		numSamples := len(audioData) / 2
		samples := make([]int16, numSamples)
		for i := 0; i < numSamples; i++ {
			samples[i] = int16(binary.LittleEndian.Uint16(audioData[i*2:]))
		}
		return samples
	}
	cmd := exec.Command("ffmpeg", "-i", "pipe:0", "-f", "s16le", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", "pipe:1")
	cmd.Stdin = bytes.NewReader(audioData)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return nil
	}
	raw := out.Bytes()
	numSamples := len(raw) / 2
	samples := make([]int16, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return samples
}

func pcm16ToBytes(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(s))
	}
	return b
}

func pcm16ToWAV(samples []int16) []byte {
	dataSize := uint32(len(samples) * 2)
	wav := make([]byte, 44+int(dataSize))
	copy(wav[0:4], "RIFF")
	binary.LittleEndian.PutUint32(wav[4:8], 36+dataSize)
	copy(wav[8:12], "WAVE")
	copy(wav[12:16], "fmt ")
	binary.LittleEndian.PutUint32(wav[16:20], 16)
	binary.LittleEndian.PutUint16(wav[20:22], 1)
	binary.LittleEndian.PutUint16(wav[22:24], 1)
	binary.LittleEndian.PutUint32(wav[24:28], 16000)
	binary.LittleEndian.PutUint32(wav[28:32], 32000)
	binary.LittleEndian.PutUint16(wav[32:34], 2)
	binary.LittleEndian.PutUint16(wav[34:36], 16)
	copy(wav[36:40], "data")
	binary.LittleEndian.PutUint32(wav[40:44], dataSize)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(wav[44+i*2:], uint16(s))
	}
	return wav
}

func trimSilence(samples []int16) []int16 {
	const threshold = 0.012
	const frameSize = 320
	if len(samples) < frameSize*2 {
		return samples
	}
	start := 0
	for start+frameSize <= len(samples) {
		if rmsFromPCM16(samples[start:start+frameSize]) >= threshold {
			break
		}
		start += frameSize
	}
	end := len(samples)
	for end-start > frameSize {
		if rmsFromPCM16(samples[end-frameSize:end]) >= threshold {
			break
		}
		end -= frameSize
	}
	padding := 800
	if start > padding { start -= padding } else { start = 0 }
	if end < len(samples)-padding { end += padding } else { end = len(samples) }
	if start >= end { return samples }
	return samples[start:end]
}

// ── Speaker-DB (SQLite) ──────────────────────────────────────

func (s *Server) initSpeakerStore() error {
	path := s.cfg.Recording.SpeakerStore
	if path == "" {
		path = "/data/speakers.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("speaker dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("speaker open: %w", err)
	}
	s.speakerDB = db
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA busy_timeout=5000")
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS persons (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE
		);
		CREATE TABLE IF NOT EXISTS profiles (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			person_id  INTEGER NOT NULL REFERENCES persons(id),
			embedding  BLOB NOT NULL,
			source     TEXT,
			first_seen TEXT,
			last_seen  TEXT
		);
		CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT
		);
	`)
	if err != nil {
		db.Close()
		return fmt.Errorf("speaker schema: %w", err)
	}
	return nil
}

func (s *Server) closeSpeakerStore() {
	if s.speakerDB != nil {
		s.speakerDB.Close()
	}
}

func (s *Server) getMeta(key string) string {
	var val string
	s.speakerDB.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&val)
	return val
}

func (s *Server) setMeta(key, value string) {
	s.speakerDB.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)`, key, value)
}

func (s *Server) createSprecher() SpeakerPerson {
	val := s.getMeta("next_speaker_idx")
	idx := 0
	if val != "" {
		fmt.Sscanf(val, "%d", &idx)
	}
	name := fmt.Sprintf("Sprecher_%d", idx)
	s.setMeta("next_speaker_idx", fmt.Sprintf("%d", idx+1))
	res, err := s.speakerDB.Exec(`INSERT INTO persons (name) VALUES (?)`, name)
	if err != nil {
		return SpeakerPerson{Name: name}
	}
	id, _ := res.LastInsertId()
	return SpeakerPerson{ID: int(id), Name: name}
}

func (s *Server) findPersonByName(name string) *SpeakerPerson {
	var p SpeakerPerson
	err := s.speakerDB.QueryRow(`SELECT id, name FROM persons WHERE name = ? COLLATE NOCASE`, name).Scan(&p.ID, &p.Name)
	if err != nil { return nil }
	return &p
}

func (s *Server) findOrCreatePerson(name string) SpeakerPerson {
	if p := s.findPersonByName(name); p != nil { return *p }
	res, err := s.speakerDB.Exec(`INSERT INTO persons (name) VALUES (?)`, name)
	if err != nil { return SpeakerPerson{Name: name} }
	id, _ := res.LastInsertId()
	return SpeakerPerson{ID: int(id), Name: name}
}

func (s *Server) renamePerson(personID int, newName string) error {
	if p := s.findPersonByName(newName); p != nil && p.ID != personID {
		return fmt.Errorf("name %q bereits vergeben", newName)
	}
	_, err := s.speakerDB.Exec(`UPDATE persons SET name = ? WHERE id = ?`, newName, personID)
	return err
}

func (s *Server) addProfileForPerson(personID int, embedding []float64, source string) int {
	embBytes, _ := json.Marshal(embedding)
	now := time.Now().Format(time.RFC3339)
	res, err := s.speakerDB.Exec(
		`INSERT INTO profiles (person_id, embedding, source, first_seen, last_seen) VALUES (?,?,?,?,?)`,
		personID, embBytes, source, now, now)
	if err != nil {
		return 0
	}
	id, _ := res.LastInsertId()
	return int(id)
}

func (s *Server) loadAllProfiles() []SpeakerProfile {
	rows, err := s.speakerDB.Query(`SELECT id, person_id, embedding, source, first_seen, last_seen FROM profiles`)
	if err != nil { return nil }
	defer rows.Close()
	var profiles []SpeakerProfile
	for rows.Next() {
		var p SpeakerProfile
		var embBlob []byte
		if err := rows.Scan(&p.ID, &p.PersonID, &embBlob, &p.Source, &p.FirstSeen, &p.LastSeen); err != nil {
			continue
		}
		json.Unmarshal(embBlob, &p.Embedding)
		profiles = append(profiles, p)
	}
	return profiles
}

func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 { return 0 }
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 { return 0 }
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

type matchSpeakerResult struct {
	Person    SpeakerPerson
	ProfileID int
	Score     float64
	Matched   bool
}

func (s *Server) matchSpeaker(embedding []float64) matchSpeakerResult {
	threshold := s.cfg.Recording.profileMatchThreshold()
	var bestPerson SpeakerPerson
	var bestProfileID int
	bestScore := 0.0
	profiles := s.loadAllProfiles()
	personCache := map[int]SpeakerPerson{}
	for _, profile := range profiles {
		score := cosineSimilarity(embedding, profile.Embedding)
		if score > bestScore {
			bestScore = score
			bestProfileID = profile.ID
			if p, ok := personCache[profile.PersonID]; ok {
				bestPerson = p
			} else {
				var p SpeakerPerson
				s.speakerDB.QueryRow(`SELECT id, name FROM persons WHERE id = ?`, profile.PersonID).Scan(&p.ID, &p.Name)
				bestPerson = p
				personCache[profile.PersonID] = p
			}
		}
	}
	if bestScore >= threshold {
		return matchSpeakerResult{Person: bestPerson, ProfileID: bestProfileID, Score: bestScore, Matched: true}
	}
	return matchSpeakerResult{Score: bestScore, Matched: false}
}

// ── VAD ──────────────────────────────────────────────────────

func (s *Server) processAudioChunk(session *RecordingSession, audioData []byte) (int, bool, string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	cfg := &s.cfg.Recording
	silenceTimeoutSamples := cfg.silenceTimeoutMs() * 16
	maxFragmentSamples := cfg.maxFragmentSec() * 16000
	softLimitSamples := cfg.softLimitSec() * 16000
	partialIntervalSec := cfg.PartialInterval
	if partialIntervalSec <= 0 { partialIntervalSec = 3 }
	partialIntervalSamples := partialIntervalSec * 16000

	samples := decodeAudioToPCM16(audioData)
	rms := 0.0
	if samples != nil {
		rms = rmsFromPCM16(samples)
	}
	isSilent := s.vadIsSilent(rms)

	if samples != nil {
		session.totalAudio = append(session.totalAudio, pcm16ToBytes(samples)...)
		session.totalSamples += len(samples)
	}

	log.Printf("recording: chunk rms=%.4f silent=%v audio=%d bytes", rms, isSilent, len(audioData))

	// Min RMS tracken
	if session.fragAudio != nil && rms > 0 && rms < session.minRMS {
		session.minRMS = rms
	}

	// Fragment-Dauer
	fragDurSamples := 0
	if session.fragAudio != nil {
		fragDurSamples = session.totalSamples - session.fragStartSamples
	}

	fragmentComplete := false

	if isSilent {
		if session.silenceSinceSamples > 0 {
			silDur := session.totalSamples - session.silenceSinceSamples
			// Fragment-Ende nur wenn: Stille >= 800ms UND Fragment >= soft_limit
			if silDur >= silenceTimeoutSamples && fragDurSamples >= softLimitSamples {
				fragmentComplete = true
			}
		} else {
			session.silenceSinceSamples = session.totalSamples
		}
		session.speechActive = false
	} else {
		if session.silenceSinceSamples > 0 {
			silDur := session.totalSamples - session.silenceSinceSamples
			if silDur > session.maxSilenceSamples {
				session.maxSilenceSamples = silDur
			}
			session.silenceSinceSamples = 0
		}
		session.speechActive = true
		if session.fragAudio == nil {
			session.fragStartSamples = session.totalSamples
			session.fragIndex++
			session.lastPartialSamples = 0
			session.maxSilenceSamples = 0
			session.minRMS = 1.0
		}
		if rms < session.minRMS {
			session.minRMS = rms
		}
		session.fragAudio = append(session.fragAudio, audioData...)
	}

	// Hard-Limit
	fragEndSamples := session.totalSamples
	if session.silenceSinceSamples > 0 {
		fragEndSamples = session.silenceSinceSamples
	}
	if session.fragAudio != nil && fragEndSamples-session.fragStartSamples >= maxFragmentSamples {
		fragmentComplete = true
		maxSilMs := session.maxSilenceSamples * 1000 / 16000
		log.Printf("recording: HARD-CUT bei %ds (längste Stille: %dms, min RMS: %.4f, silence_thresh: %.4f)",
			cfg.maxFragmentSec(), maxSilMs, session.minRMS, cfg.silenceThresh())
	}

	// Partial-Transkription
	var partialText string
	if !isSilent && session.fragAudio != nil &&
		(session.lastPartialSamples == 0 || session.totalSamples-session.lastPartialSamples >= partialIntervalSamples) {
		partialText = s.whisperFast(session.fragAudio)
		session.lastPartialSamples = session.totalSamples
	}

	return session.fragIndex, fragmentComplete, partialText
}

// ── Session-Helfer ───────────────────────────────────────────

func (session *RecordingSession) takeFragAudio() []byte {
	session.mu.Lock()
	defer session.mu.Unlock()
	audio := session.fragAudio
	session.fragAudio = nil
	session.silenceSinceSamples = 0
	session.speechActive = false
	session.lastPartialSamples = 0
	return audio
}

func (session *RecordingSession) addFragment(idx int, text string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	start := float64(session.fragStartSamples) / 16000.0
	if start < 0 { start = 0 }
	endSamples := session.totalSamples
	if session.silenceSinceSamples > 0 {
		endSamples = session.silenceSinceSamples
	}
	end := float64(endSamples) / 16000.0
	if end < start { end = start }
	session.Fragments = append(session.Fragments, RecordingFrag{
		Index:    idx,
		Text:     text,
		Speaker:  "unknown",
		Start:    start,
		End:      end,
		Duration: end - start,
		Status:   "done",
	})
}

// ── Whisper ──────────────────────────────────────────────────

// whisperFast — schnelle Transkription, nur Text. Für Live-SSE.
func (s *Server) whisperFast(audioData []byte) string {
	whisperURL := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"
	samples := decodeAudioToPCM16(audioData)
	if samples == nil { return "" }
	samples = trimSilence(samples)
	if len(samples) < 1600 { return "" }
	wavData := pcm16ToWAV(samples)

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteField("model", s.cfg.Whisper.Model)
	w.WriteField("language", "de")
	w.WriteFile("file", "audio.wav", bytes.NewReader(wavData))
	w.Close()

	req, err := http.NewRequest("POST", whisperURL, &buf)
	if err != nil { return "" }
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	resp, err := s.client.Do(req)
	if err != nil { return "" }
	defer resp.Body.Close()
	var wr whisperResponse
	if err := json.NewDecoder(resp.Body).Decode(&wr); err != nil { return "" }
	return strings.TrimSpace(wr.Text)
}

// whisperDTW — Transkription mit Word-Timestamps (DTW). Langsam (~10s RTX).
func (s *Server) whisperDTW(audioData []byte, fragStartSec float64) whisperTranscribeResult {
	whisperURL := strings.TrimRight(s.cfg.Whisper.APIBase, "/") + "/audio/transcriptions"
	samples := decodeAudioToPCM16(audioData)
	if samples == nil { return whisperTranscribeResult{} }
	samples = trimSilence(samples)
	if len(samples) < 1600 { return whisperTranscribeResult{} }
	wavData := pcm16ToWAV(samples)

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	w.WriteField("model", s.cfg.Whisper.Model)
	w.WriteField("language", "de")
	w.WriteField("response_format", "verbose_json")
	w.WriteField("timestamp_granularities", "word")
	w.WriteFile("file", "audio.wav", bytes.NewReader(wavData))
	w.Close()

	req, err := http.NewRequest("POST", whisperURL, &buf)
	if err != nil { return whisperTranscribeResult{} }
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	resp, err := s.client.Do(req)
	if err != nil { return whisperTranscribeResult{} }
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil { return whisperTranscribeResult{} }

	var wr whisperResponse
	if err := json.Unmarshal(body, &wr); err != nil { return whisperTranscribeResult{} }
	text := strings.TrimSpace(wr.Text)
	if text == "" { return whisperTranscribeResult{} }

	var words []WordTimestamp
	for _, ww := range wr.Words {
		words = append(words, WordTimestamp{
			Word:  strings.TrimSpace(ww.Word),
			Start: round2(fragStartSec + ww.Start),
			End:   round2(fragStartSec + ww.End),
		})
	}
	return whisperTranscribeResult{Text: text, Words: words}
}

// ── Diarization ──────────────────────────────────────────────

// diarizeAudio sendet Audio an openannote und gibt rohe Segmente + Embeddings zurück.
func (s *Server) diarizeAudio(audioData []byte) *diarizeResponse {
	url := strings.TrimRight(s.cfg.Recording.DiarizeAPIBase, "/") + "/diarize"
	samples := decodeAudioToPCM16(audioData)
	if samples == nil { return nil }
	wavData := pcm16ToWAV(samples)

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	if s.cfg.Recording.DiarizeModel != "" {
		w.WriteField("model", s.cfg.Recording.DiarizeModel)
	}
	minSp := s.cfg.Recording.minSpeakers()
	w.WriteField("min_speakers", fmt.Sprintf("%d", minSp))
	if s.cfg.Recording.MaxSpeakers > 0 {
		w.WriteField("max_speakers", fmt.Sprintf("%d", s.cfg.Recording.MaxSpeakers))
	}
	w.WriteFile("file", "chunk.wav", bytes.NewReader(wavData))
	w.Close()

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil { return nil }
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	resp, err := s.client.Do(req)
	if err != nil { return nil }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { return nil }

	var result diarizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil { return nil }
	return &result
}

// diarizeFragment — pyannote auf einem Fragment-Audio.
// Jedes Embedding = neue Person + neues Profil.
// Word-Timestamps für Wort-zu-Speaker-Zuordnung.
// correctSegmentBoundaries für Satzgrenzen.
func (s *Server) diarizeFragment(session *RecordingSession, fragmentIdx int, fragAudio []byte) {
	result := s.diarizeAudio(fragAudio)
	if result == nil || len(result.Segments) == 0 {
		return
	}

	log.Printf("recording: diarize-fragment %d: %d Segmente, %d Speaker",
		fragmentIdx, len(result.Segments), len(result.Speakers))

	session.mu.Lock()

	// Fragment-Startzeit
	var frag *RecordingFrag
	for i := range session.Fragments {
		if session.Fragments[i].Index == fragmentIdx {
			frag = &session.Fragments[i]
			break
		}
	}
	if frag == nil {
		session.mu.Unlock()
		return
	}
	fragStart := frag.Start

	// Jedes Embedding = neue Person + neues Profil (innerhalb eines pyannote-Calls)
	labelRefs := make(map[string]SpeakerRef)
	s.speakerMu.Lock()
	for _, label := range result.Speakers {
		emb := result.SpeakerEmbeddings[label]
		if len(emb) == 0 {
			continue
		}
		person := s.createSprecher()
		pid := s.addProfileForPerson(person.ID, emb, session.ID)
		ref := SpeakerRef{PersonName: person.Name, PersonID: person.ID, ProfileID: pid}
		labelRefs[label] = ref
		log.Printf("recording: diarize-fragment %d: %s → %s/%d", fragmentIdx, label, person.Name, pid)
	}
	s.speakerMu.Unlock()

	// pyannote-Segmente auf absolute Session-Zeit + stabile Speaker-Namen
	var segments []FragSpeakerSeg
	for _, seg := range result.Segments {
		speakerName := "unknown"
		if ref, ok := labelRefs[seg.Speaker]; ok {
			speakerName = ref.String()
		}
		segments = append(segments, FragSpeakerSeg{
			Speaker: speakerName,
			Start:   round2(fragStart + seg.Start),
			End:     round2(fragStart + seg.End),
		})
	}

	// Text auf Segmente verteilen per Word-Timestamps
	words := strings.Fields(frag.Text)
	if len(words) > 0 && len(segments) > 0 {
		type wordWithTime struct {
			word    string
			absTime float64
		}
		wordsWT := make([]wordWithTime, len(words))
		if len(frag.Words) >= len(words) {
			for wi := range words {
				wordsWT[wi] = wordWithTime{words[wi], frag.Words[wi].Start}
			}
		} else {
			for wi := range words {
				wordsWT[wi] = wordWithTime{words[wi], frag.Start + float64(wi)/float64(len(words))*frag.Duration}
			}
		}

		// Wort-zu-Speaker per pyannote-Segmente
		wordSpeakers := make([]string, len(words))
		for wi := range wordsWT {
			absTime := wordsWT[wi].absTime
			wordSpeakers[wi] = segments[0].Speaker
			for _, seg := range segments {
				if absTime >= seg.Start && absTime < seg.End {
					wordSpeakers[wi] = seg.Speaker
					break
				}
			}
		}

		// Satzgrenzen-Korrektur
		wordSpeakers = correctSegmentBoundaries(words, wordSpeakers)

		// Kontiguierte Wortgruppen → finale Segmente mit Text
		var finalSegs []FragSpeakerSeg
		segStart := 0
		for wi := 1; wi <= len(words); wi++ {
			if wi < len(words) && wordSpeakers[wi] == wordSpeakers[segStart] {
				continue
			}
			segText := strings.Join(words[segStart:wi], " ")
			absStart := wordsWT[segStart].absTime
			absEnd := frag.End
			if wi < len(words) {
				absEnd = wordsWT[wi].absTime
			}
			finalSegs = append(finalSegs, FragSpeakerSeg{
				Speaker: wordSpeakers[segStart],
				Start:   round2(absStart),
				End:     round2(absEnd),
				Text:    segText,
			})
			segStart = wi
		}
		frag.Segments = finalSegs
	} else {
		frag.Segments = segments
	}

	// Dominanter Speaker = längstes Segment
	maxDur := 0.0
	for _, seg := range frag.Segments {
		d := seg.End - seg.Start
		if d > maxDur {
			maxDur = d
			frag.Speaker = seg.Speaker
		}
	}

	session.mu.Unlock()
	log.Printf("recording: diarize-fragment %d: %d Segmente, dominant=%s",
		fragmentIdx, len(frag.Segments), frag.Speaker)
}

// ── Segment-Korrektur ────────────────────────────────────────

func correctSegmentBoundaries(words []string, wordSpeakers []string) []string {
	if len(words) < 3 {
		return wordSpeakers
	}
	result := make([]string, len(wordSpeakers))
	copy(result, wordSpeakers)

	for wi := 1; wi < len(words); wi++ {
		if result[wi] == result[wi-1] {
			continue
		}
		if endsWithSentence(words[wi-1]) {
			continue
		}
		prevSpeaker := result[wi-1]
		currentSpeaker := result[wi]

		// Vorwärts: Satzende suchen (max 12 Wörter)
		bestSplit := -1
		for j := wi; j < len(words) && j < wi+12; j++ {
			if endsWithSentence(words[j]) {
				bestSplit = j
				break
			}
		}

		if bestSplit >= wi {
			// Nicht über Dritt-Speaker hinweg verschieben
			wouldOverwrite := false
			for k := wi; k <= bestSplit; k++ {
				if result[k] != currentSpeaker && result[k] != prevSpeaker {
					wouldOverwrite = true
					break
				}
			}
			if !wouldOverwrite {
				for k := wi; k <= bestSplit; k++ {
					result[k] = prevSpeaker
				}
			}
		}
	}
	return result
}

// ── Utterances ───────────────────────────────────────────────

func buildUtterances(fragments []RecordingFrag) []Utterance {
	var allSegs []FragSpeakerSeg
	for _, frag := range fragments {
		if len(frag.Segments) > 0 {
			allSegs = append(allSegs, frag.Segments...)
		} else if frag.Text != "" {
			allSegs = append(allSegs, FragSpeakerSeg{
				Speaker: frag.Speaker, Start: frag.Start, End: frag.End, Text: frag.Text,
			})
		}
	}
	if len(allSegs) == 0 {
		return nil
	}
	sort.Slice(allSegs, func(i, j int) bool { return allSegs[i].Start < allSegs[j].Start })

	const maxPause = 2.0
	var utterances []Utterance
	cur := Utterance{Speaker: allSegs[0].Speaker, Start: allSegs[0].Start, End: allSegs[0].End, Text: allSegs[0].Text}
	for i := 1; i < len(allSegs); i++ {
		seg := allSegs[i]
		gap := seg.Start - cur.End
		if seg.Speaker == cur.Speaker && gap < maxPause {
			cur.End = seg.End
			cur.Text += " " + seg.Text
		} else {
			cur.Text = strings.TrimSpace(cur.Text)
			utterances = append(utterances, cur)
			cur = Utterance{Speaker: seg.Speaker, Start: seg.Start, End: seg.End, Text: seg.Text}
		}
	}
	cur.Text = strings.TrimSpace(cur.Text)
	utterances = append(utterances, cur)

	for i := range utterances {
		utterances[i].Start = round2(utterances[i].Start)
		utterances[i].End = round2(utterances[i].End)
	}
	return utterances
}

// ── Flush ────────────────────────────────────────────────────

func (s *Server) flushPendingFragment(session *RecordingSession) {
	fragAudio := session.takeFragAudio()
	if len(fragAudio) < 2 || s.cfg.Whisper.APIBase == "" {
		return
	}
	session.mu.Lock()
	fragmentIdx := session.fragIndex
	if fragmentIdx == 0 { fragmentIdx = 1 }
	fragStartSec := float64(session.fragStartSamples) / 16000.0
	session.mu.Unlock()

	log.Printf("recording: flush fragment %d (%d bytes)", fragmentIdx, len(fragAudio))
	text := s.whisperFast(fragAudio)
	if text == "" { return }
	session.addFragment(fragmentIdx, text)

	// Word-Timestamps synchron (Session-End wartet ohnehin)
	wordResult := s.whisperDTW(fragAudio, fragStartSec)
	if len(wordResult.Words) > 0 {
		session.mu.Lock()
		for i := range session.Fragments {
			if session.Fragments[i].Index == fragmentIdx {
				session.Fragments[i].Words = wordResult.Words
				break
			}
		}
		session.mu.Unlock()
	}

	// Diarization
	if s.cfg.Recording.DiarizeAPIBase != "" {
		s.diarizeFragment(session, fragmentIdx, fragAudio)
	}

	if session.shareToken != "" {
		go s.webdavUploadFragment(session, fragmentIdx, fragAudio)
	}
}

// ── Save Profiles ────────────────────────────────────────────

func (s *Server) saveSessionProfiles(session *RecordingSession) {
	// Profile werden schon in diarizeFragment angelegt.
	// Hier nur loggen.
	session.mu.Lock()
	fragCount := len(session.Fragments)
	segCount := 0
	for _, f := range session.Fragments {
		segCount += len(f.Segments)
	}
	session.mu.Unlock()
	log.Printf("recording: saveSessionProfiles: %d Fragmente, %d Segmente", fragCount, segCount)
}

// ── Routes ───────────────────────────────────────────────────

func (s *Server) handleRecordingSession(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
	switch r.Method {
	case http.MethodPost:
		var body struct {
			SpaceID string `json:"space_id"`
			Share   struct {
				Token    string `json:"token"`
				Password string `json:"password"`
			} `json:"share"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
			return
		}
		sessionID := time.Now().Format("2006-01-02-1504") + "-" + randHex(4)
		session := &RecordingSession{
			ID: sessionID, SpaceID: body.SpaceID,
			UserID: r.Header.Get("x-access-token"),
			Created: time.Now(), Fragments: []RecordingFrag{},
			shareToken: body.Share.Token, sharePasswd: body.Share.Password,
		}
		s.recMu.Lock()
		s.sessions[sessionID] = session
		s.recMu.Unlock()
		if body.Share.Token != "" {
			go s.webdavEnsureDirs(session)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(session)
	case http.MethodGet:
		s.handleRecordingSessions(w, r)
	default:
		http.Error(w, "POST or GET only", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRecordingSessionEnd(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
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

	log.Printf("recording: session/end: %s frags=%d audio=%d bytes",
		body.SessionID, len(session.Fragments), len(session.totalAudio))

	// 6. Flush
	s.flushPendingFragment(session)

	// 6b. Fallback: totalAudio als ein Fragment
	if len(session.Fragments) == 0 && len(session.totalAudio) > 0 && s.cfg.Whisper.APIBase != "" {
		text := s.whisperFast(session.totalAudio)
		if text != "" {
			session.mu.Lock()
			totalDuration := float64(session.totalSamples) / 16000.0
			session.Fragments = append(session.Fragments, RecordingFrag{
				Index: 1, Text: text, Speaker: "unknown",
				Start: 0, End: totalDuration, Duration: totalDuration, Status: "done",
			})
			session.mu.Unlock()
			if s.cfg.Recording.DiarizeAPIBase != "" {
				s.diarizeFragment(session, 1, session.totalAudio)
			}
		}
	}

	// 7. Word-Timestamp Wait (max 30s)
	wordsDone := make(chan struct{})
	go func() { session.wordWg.Wait(); close(wordsDone) }()
	select {
	case <-wordsDone:
	case <-time.After(30 * time.Second):
		log.Printf("recording: session/end: word-timestamp timeout (30s)")
	}

	// 7b. Unzugeordnete Fragmente diarisieren
	if s.cfg.Recording.DiarizeAPIBase != "" {
		session.mu.Lock()
		for i := range session.Fragments {
			frag := &session.Fragments[i]
			if frag.Speaker != "" && frag.Speaker != "unknown" {
				continue
			}
			startSample := int(frag.Start * 16000)
			endSample := int(frag.End * 16000)
			if endSample*2 > len(session.totalAudio) {
				endSample = len(session.totalAudio) / 2
			}
			if startSample*2 >= len(session.totalAudio) || startSample >= endSample {
				continue
			}
			fragAudio := session.totalAudio[startSample*2 : endSample*2]
			session.mu.Unlock()
			s.diarizeFragment(session, frag.Index, fragAudio)
			session.mu.Lock()
		}
		session.mu.Unlock()
	}

	// 8. Utterances
	utterances := buildUtterances(session.Fragments)

	// 9. Transkript aus Utterances
	var transcriptBuilder strings.Builder
	for _, utt := range utterances {
		if utt.Speaker != "" && utt.Speaker != "unknown" {
			fmt.Fprintf(&transcriptBuilder, "[%s]: ", utt.Speaker)
		}
		transcriptBuilder.WriteString(utt.Text)
		transcriptBuilder.WriteString("\n")
	}
	fullTranscript := transcriptBuilder.String()

	// Leere Session
	if fullTranscript == "" {
		if session.shareToken != "" && len(session.totalAudio) > 0 {
			s.webdavUploadRecording(session, "")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status": "done", "transcript": "", "fragments": session.Fragments,
		})
		return
	}

	// 10. LLM-Finalpass (optional)
	finishedTranscript := fullTranscript
	if s.cfg.Recording.doLLMFinalpass() && s.cfg.LLM.APIBase != "" {
		prompt := fmt.Sprintf(
			`Du erhältst ein Roh-Transkript einer Audio-Aufnahme. Korrigiere NUR:
- Tippfehler und Erkennungsfehler
- Fehlende Satzzeichen (Punkte, Kommas, Frage-/Ausrufezeichen)
- Grammatik und Rechtschreibung
- Füllwörter (äh, ähm, also also)
- Whisper-Halluzinationen: "Vielen Dank", "Thank you", "Untertitel von", "Danke fürs Zuschauen" entfernen

WICHTIG:
- Füge KEINE neuen Wörter hinzu.
- Erfinde KEINEN Inhalt.
- Entferne KEINE inhaltlichen Aussagen.
- Behalte die Sprecher-Zuordnungen bei ([Name]: Text).

Gib NUR den korrigierten Text zurück, keine Erklärungen.

Roh-Transkript:
%s`, fullTranscript)
		polished := s.llmChat(prompt)
		if polished != "" {
			finishedTranscript = polished
		}
	}

	// 11. Upload
	uploadPath := ""
	if session.shareToken != "" {
		uploadPath = s.webdavUploadRecording(session, finishedTranscript)
	}

	// Save profiles
	s.saveSessionProfiles(session)

	// Response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "done",
		"session_id": body.SessionID,
		"transcript": finishedTranscript,
		"upload":     uploadPath,
		"fragments":  session.Fragments,
		"utterances": utterances,
	})
}

func (s *Server) handleRecordingSessions(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
	s.recMu.Lock()
	sessions := make([]RecordingSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		snap := *session
		snap.fragAudio = nil
		snap.totalAudio = nil
		sessions = append(sessions, snap)
	}
	s.recMu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].Created.After(sessions[j].Created) })
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sessions)
}

func (s *Server) handleRecordingChunk(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.Whisper.APIBase == "" {
		writeChatError(w, http.StatusServiceUnavailable, "Whisper nicht konfiguriert")
		return
	}

	maxBytes := int64(s.cfg.Recording.MaxChunkMB) * 1024 * 1024
	if maxBytes <= 0 { maxBytes = 50 * 1024 * 1024 }
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes+1024*1024)
	mr, err := r.MultipartReader()
	if err != nil {
		writeChatError(w, http.StatusBadRequest, "multipart erwartet")
		return
	}

	var audioData []byte
	var sessionID string
	for {
		part, err := mr.NextPart()
		if err == io.EOF { break }
		if err != nil { break }
		switch part.FormName() {
		case "file":
			audioData, _ = io.ReadAll(io.LimitReader(part, maxBytes+1))
		case "session_id":
			buf := new(bytes.Buffer)
			io.CopyN(buf, part, 256)
			sessionID = strings.TrimSpace(buf.String())
		}
	}
	if audioData == nil || sessionID == "" {
		writeChatError(w, http.StatusBadRequest, "file und session_id fehlen")
		return
	}

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

	s.recMu.Lock()
	session, exists := s.sessions[sessionID]
	s.recMu.Unlock()
	if !exists || session.Done {
		sseWrite(w, flusher, map[string]any{"type": "error", "message": "Session ungültig"})
		return
	}

	// 1. VAD
	fragmentIdx, fragmentComplete, partialText := s.processAudioChunk(session, audioData)

	if partialText != "" {
		sseWrite(w, flusher, map[string]any{"type": "partial", "fragment": fragmentIdx, "text": partialText})
	}

	// 2-5. Fragment fertig
	if fragmentComplete {
		fragAudio := session.takeFragAudio()
		if len(fragAudio) < 2 {
			return
		}

		session.mu.Lock()
		fragDurSec := float64(session.totalSamples-session.fragStartSamples) / 16000.0
		fragStartSec := float64(session.fragStartSamples) / 16000.0
		session.mu.Unlock()

		log.Printf("recording: fragment %d fertig: %d bytes (%.1fs)", fragmentIdx, len(fragAudio), fragDurSec)

		// 2. Transkription (schnell)
		text := s.whisperFast(fragAudio)
		if text == "" {
			sseWrite(w, flusher, map[string]any{"type": "error", "fragment": fragmentIdx, "message": "Transkription fehlgeschlagen"})
			return
		}
		session.addFragment(fragmentIdx, text)

		sseWrite(w, flusher, map[string]any{"type": "final", "fragment": fragmentIdx, "text": text})

		// 3. Word-Timestamps (async)
		session.wordWg.Add(1)
		go func(fIdx int, audio []byte, startSec float64) {
			defer session.wordWg.Done()
			result := s.whisperDTW(audio, startSec)
			if len(result.Words) > 0 {
				session.mu.Lock()
				for i := range session.Fragments {
					if session.Fragments[i].Index == fIdx {
						session.Fragments[i].Words = result.Words
						break
					}
				}
				session.mu.Unlock()
			}
		}(fragmentIdx, fragAudio, fragStartSec)

		// 4-5. Diarization + Segmentierung
		if s.cfg.Recording.DiarizeAPIBase != "" {
			s.diarizeFragment(session, fragmentIdx, fragAudio)
		}

		// Upload Fragment-Audio async
		if session.shareToken != "" {
			go s.webdavUploadFragment(session, fragmentIdx, fragAudio)
		}

		sseWrite(w, flusher, map[string]any{"type": "done", "fragment": fragmentIdx, "text": text})
	}
}

// ── Speaker-API ──────────────────────────────────────────────

type speakerAPIProfile struct {
	ID        int    `json:"id"`
	FirstSeen string `json:"first_seen"`
	LastSeen  string `json:"last_seen"`
}

type speakerAPIPerson struct {
	ID           int                `json:"id"`
	Name         string             `json:"name"`
	ProfileCount int                `json:"profile_count"`
	Profiles     []speakerAPIProfile `json:"profiles"`
}

func (s *Server) handleRecordingSpeakers(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
	switch r.Method {
	case http.MethodGet:
		s.speakerMu.RLock()
		rows, err := s.speakerDB.Query(`SELECT id, name FROM persons ORDER BY id`)
		if err != nil {
			s.speakerMu.RUnlock()
			writeChatError(w, 500, "db query: "+err.Error())
			return
		}
		var personRows []struct{ ID int; Name string }
		for rows.Next() {
			var pr struct{ ID int; Name string }
			rows.Scan(&pr.ID, &pr.Name)
			personRows = append(personRows, pr)
		}
		rows.Close()
		s.speakerMu.RUnlock()

		var persons []speakerAPIPerson
		for _, pr := range personRows {
			api := speakerAPIPerson{ID: pr.ID, Name: pr.Name}
			pRows, err := s.speakerDB.Query(
				`SELECT id, first_seen, last_seen FROM profiles WHERE person_id = ? ORDER BY id`, pr.ID)
			if err == nil {
				for pRows.Next() {
					var prof speakerAPIProfile
					pRows.Scan(&prof.ID, &prof.FirstSeen, &prof.LastSeen)
					api.Profiles = append(api.Profiles, prof)
				}
				pRows.Close()
				api.ProfileCount = len(api.Profiles)
			}
			persons = append(persons, api)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"persons": persons})

	case http.MethodPut:
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeChatError(w, http.StatusBadRequest, "ungültiges JSON")
			return
		}
		if body.Name == "" {
			writeChatError(w, http.StatusBadRequest, "name fehlt")
			return
		}
		s.speakerMu.Lock()
		person := s.findOrCreatePerson(body.Name)
		s.speakerMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(person)

	default:
		http.Error(w, "GET or PUT only", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleRecordingSpeakerLink(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) { return }
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		SpeakerID  string `json:"speaker_id"`
		PersonName string `json:"person_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeChatError(w, http.StatusBadRequest, "ungültiges JSON")
		return
	}
	if body.PersonName == "" {
		writeChatError(w, http.StatusBadRequest, "person_name fehlt")
		return
	}
	speakerName := body.SpeakerID
	if idx := strings.Index(speakerName, "/"); idx >= 0 {
		speakerName = speakerName[:idx]
	}
	s.speakerMu.Lock()
	person := s.findPersonByName(speakerName)
	if person == nil {
		s.speakerMu.Unlock()
		writeChatError(w, 404, "Person nicht gefunden")
		return
	}
	if err := s.renamePerson(person.ID, body.PersonName); err != nil {
		s.speakerMu.Unlock()
		writeChatError(w, http.StatusConflict, err.Error())
		return
	}
	renamed := SpeakerPerson{ID: person.ID, Name: body.PersonName}
	s.speakerMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(renamed)
}

// ── WebDAV ───────────────────────────────────────────────────

func webdavSessionDir(session *RecordingSession) string {
	return fmt.Sprintf("%s/%s", session.Created.Format("2006-01-02"), session.ID)
}

func (s *Server) webdavEnsureDirs(session *RecordingSession) {
	if session.shareToken == "" || session.sharePasswd == "" { return }
	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)
	dateDir := session.Created.Format("2006-01-02")
	for _, dir := range []string{dateDir, sessionDir} {
		if err := dav.shareMkdir(dir); err != nil {
			log.Printf("recording: webdav: MKCOL %s: %v", dir, err)
			return
		}
	}
}

func (s *Server) webdavUploadRecording(session *RecordingSession, transcript string) string {
	if session.shareToken == "" || session.sharePasswd == "" { return "" }
	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)
	dateDir := session.Created.Format("2006-01-02")
	for _, dir := range []string{dateDir, sessionDir} {
		dav.shareMkdir(dir)
	}

	utterances := buildUtterances(session.Fragments)
	transcriptJSON, _ := json.MarshalIndent(map[string]any{
		"session_id": session.ID, "created": session.Created,
		"transcript": transcript, "fragments": session.Fragments,
		"utterances": utterances, "audio_file": "aufnahme.wav",
	}, "", "  ")
	dav.sharePutFile(sessionDir+"/transkript.trs", string(transcriptJSON))

	if len(session.totalAudio) > 0 {
		dav.sharePutBinary(sessionDir+"/aufnahme.wav", pcm16ToWAV(decodeAudioToPCM16(session.totalAudio)))
	}
	return sessionDir
}

func (s *Server) webdavUploadFragment(session *RecordingSession, fragmentIdx int, fragAudio []byte) {
	samples := decodeAudioToPCM16(fragAudio)
	if samples == nil { return }
	wavData := pcm16ToWAV(samples)
	filename := fmt.Sprintf("fragment_%03d.wav", fragmentIdx)
	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)
	dav.sharePutBinary(sessionDir+"/"+filename, wavData)
}
