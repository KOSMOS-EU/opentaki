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
	DiarizeAPIBase  string  `yaml:"diarize_api_base"`   // e.g. "http://microllm:8012/svc/steno-ml"
	DiarizeModel    string  `yaml:"diarize_model"`      // e.g. "pyannote/speaker-diarization-3.1"
	SpeakerStore    string  `yaml:"speaker_store"`      // SQLite-DB Pfad (leer = /data/speakers.db)
	SpeakerMatch    float64 `yaml:"speaker_match"`      // cosine threshold (default 0.75)
	MaxChunkMB      int     `yaml:"max_chunk_mb"`       // max size per chunk (default 50)
	SilenceThresh   float64 `yaml:"silence_thresh"`     // RMS below this = silence (default 0.01)
	SilenceTimeout  int     `yaml:"silence_timeout_ms"` // ms of silence → fragment end (default 800)
	PartialInterval int     `yaml:"partial_interval_s"` // seconds between partial transcriptions (default 3)
	MaxFragmentSec  int     `yaml:"max_fragment_sec"`   // max fragment duration (default 30)
	LiveDiarize     bool    `yaml:"live_diarize"`       // Rolling-Window-Diarization (default false)
	LiveWindowSec   int     `yaml:"live_window_sec"`    // Rolling-Window-Länge in Sekunden (default 60)
	LiveOverlapSec  int     `yaml:"live_overlap_sec"`   // Overlap für Label-Alignment (default 10)
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
	fragStartSamples int      // totalSamples zum Fragment-Start (Audio-Zeit)
	lastSilence     int       // totalSamples bei letzter Stille (Audio-Zeit)
	speechActive    bool      // aktuell in Sprechphase
	silenceSinceSamples int   // totalSamples seit Stille beginnt (Audio-Zeit)
	lastPartialSamples int    // totalSamples beim letzten Partial-Transcribe (Audio-Zeit)
	fragIndex       int       // laufende Fragment-Nummer
	prevTranscript  string    // Transkript bis letzte Sprechpause (Kontext)
	totalAudio      []byte    // komplettes Audio der Session (PCM16)
	totalSamples    int       // kumulierte PCM-Samples (für korrektes Timing)
	shareToken      string    // WebDAV Share-Token (public-files)
	sharePasswd     string    // WebDAV Share-Password

	// Rolling-Window-Diarization (Live-Speaker)
	windowCursorSamples int                 // höchstes totalSamples mit Live-Speaker
	liveSpeakerByFrag   map[int]string      // Fragment-Index → Speaker-Name
	liveSpeakerMap      map[string]SpeakerRef // pyannote-Label → stabile SpeakerRef (session-lokal)
	liveSpeakerEmb      map[string][]float64  // pyannote-Label → Embedding (für Alignment)
}

type RecordingFrag struct {
	Index    int              `json:"index"`
	Text     string           `json:"text"`
	Speaker  string           `json:"speaker"` // dominanter Speaker (Fallback)
	Start    float64          `json:"start"`
	End      float64          `json:"end"`
	Duration float64          `json:"duration"`
	Status   string           `json:"status"` // "processing" | "done" | "failed"
	Segments []FragSpeakerSeg `json:"segments,omitempty"` // Speaker-Segmente nach Diarization
}

// FragSpeakerSeg ist ein Speaker-Segment innerhalb eines Fragments.
// Wird nach der Session-End-Diarization per Zeit-Overlap berechnet.
type FragSpeakerSeg struct {
	Speaker string  `json:"speaker"`
	Start   float64 `json:"start"` // relative Zeit im Fragment (Sekunden)
	End     float64 `json:"end"`
	Text    string  `json:"text"` // Text-Anteil (proportional zur Dauer)
}

// SpeakerPerson = die Person (stabil, user-definiert oder SPEAKER_XX).
type SpeakerPerson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// SpeakerProfile = eine erkannte Stimme, gehört zu einer Person.
type SpeakerProfile struct {
	ID        int       `json:"id"`
	PersonID  int       `json:"person_id"`
	Embedding []float64 `json:"-"`
	Source    string    `json:"source"`
	FirstSeen string    `json:"first_seen"`
	LastSeen  string    `json:"last_seen"`
}

// SpeakerRef identifiziert einen Sprecher in einem Fragment.
type SpeakerRef struct {
	PersonName string
	PersonID   int // Person-Table-ID (für addProfileForSession)
	ProfileID  int // 0 = nicht gesetzt (erstes Profil)
}

// String liefert das .trs-Format: "Klaus", "Klaus/2", "SPEAKER_00".
func (r SpeakerRef) String() string {
	if r.ProfileID <= 1 {
		return r.PersonName
	}
	return fmt.Sprintf("%s/%d", r.PersonName, r.ProfileID)
}

// ── Diarize response (openannote format) ────────────────────

type diarizeResponse struct {
	Segments         []diarizeSegment  `json:"segments"`
	Speakers         []string          `json:"speakers"`
	SpeakerEmbeddings map[string][]float64 `json:"speaker_embeddings"`
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

func round2(v float64) float64 {
	return math.Round(v*100) / 100
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

// ── Audio-Decoding ──────────────────────────────────────────

// decodeAudioToPCM16 wandelt Audio-Bytes in PCM16 16kHz mono um.
// Unterstützt: Raw PCM (Int16 LE) und WebM/OGG (via ffmpeg).
func decodeAudioToPCM16(audioData []byte) []int16 {
	if len(audioData) < 2 {
		return nil
	}
	// Raw PCM erkennen: keine Container-Magic
	isWebM := bytes.HasPrefix(audioData, []byte{0x1A, 0x45, 0xDF, 0xA3})
	isWAV := bytes.HasPrefix(audioData, []byte("RIFF"))
	isOGG := bytes.HasPrefix(audioData, []byte("OggS"))
	if !isWebM && !isWAV && !isOGG {
		// Raw PCM Int16 LE → direkt konvertieren
		// Ungerade Länge: letztes Byte verwerfen
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

	// Fallback: ffmpeg (WebM/OGG/WAV)
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
	numSamples := len(raw) / 2
	samples := make([]int16, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return samples
}

// pcm16ToWAV wrappt PCM16 16kHz mono in einen WAV-Container.
// Nötig weil Whisper-API (vLLM) WAV erwartet.
// pcm16ToBytes konvertiert []int16 zu raw PCM16-Bytes (little-endian).
func pcm16ToBytes(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(s))
	}
	return b
}

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

// ── Speaker store (SQLite) ───────────────────────────────────

// initSpeakerStore öffnet/erzeugt die SQLite-DB und migriert speakers.json falls vorhanden.
func (s *Server) initSpeakerStore() error {
	path := s.cfg.Recording.SpeakerStore
	if path == "" {
		path = "/data/speakers.db"
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("speaker dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("speaker open: %w", err)
	}
	s.speakerDB = db

	// WAL für bessere Concurrency
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

	// Migration: speakers.json → SQLite
	if strings.HasSuffix(path, ".json") {
		jsonPath := strings.TrimSuffix(path, ".db")
		if jsonPath == "" {
			jsonPath = strings.Replace(path, ".db", ".json", 1)
		}
		s.migrateJSONToSQLite(jsonPath)
	} else {
		// Falls neben der .db eine .json liegt
		jsonPath := strings.TrimSuffix(path, ".db") + ".json"
		s.migrateJSONToSQLite(jsonPath)
	}

	return nil
}

// migrateJSONToSQLite importiert eine bestehende speakers.json in die DB.
func (s *Server) migrateJSONToSQLite(jsonPath string) {
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		return // keine JSON-Datei, nichts zu migrieren
	}

	var old struct {
		Persons []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"persons"`
		Profiles []struct {
			ID        string    `json:"id"`
			PersonID  string    `json:"person_id"`
			Embedding []float64 `json:"embedding"`
			Source    string    `json:"source"`
			FirstSeen string    `json:"first_seen"`
			LastSeen  string    `json:"last_seen"`
		} `json:"profiles"`
	}
	if err := json.Unmarshal(data, &old); err != nil {
		log.Printf("recording: migration parse error: %v", err)
		return
	}
	if len(old.Persons) == 0 {
		return
	}

	log.Printf("recording: migrating %d persons, %d profiles from %s", len(old.Persons), len(old.Profiles), jsonPath)

	tx, err := s.speakerDB.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// Personen eintragen (ID-Mapping: alte String-ID → neue Int-ID)
	idMap := map[string]int{}
	for _, p := range old.Persons {
		res, err := tx.Exec(`INSERT OR IGNORE INTO persons (name) VALUES (?)`, p.Name)
		if err != nil {
			continue
		}
		newID, _ := res.LastInsertId()
		if newID == 0 {
			// Exists already
			var id int
			tx.QueryRow(`SELECT id FROM persons WHERE name = ?`, p.Name).Scan(&id)
			newID = int64(id)
		}
		idMap[p.ID] = int(newID)
	}

	// Profile eintragen
	for _, pr := range old.Profiles {
		personID, ok := idMap[pr.PersonID]
		if !ok {
			continue
		}
		embBytes, _ := json.Marshal(pr.Embedding)
		tx.Exec(`INSERT INTO profiles (person_id, embedding, source, first_seen, last_seen) VALUES (?,?,?,?,?)`,
			personID, embBytes, pr.Source, pr.FirstSeen, pr.LastSeen)
	}

	// next_speaker_idx setzen
	maxIdx := 0
	for _, p := range old.Persons {
		if strings.HasPrefix(p.Name, "SPEAKER_") {
			var idx int
			if _, err := fmt.Sscanf(p.Name, "SPEAKER_%d", &idx); err == nil && idx >= maxIdx {
				maxIdx = idx
			}
		}
	}
	tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('next_speaker_idx', ?)`, fmt.Sprintf("%d", maxIdx+1))

	if err := tx.Commit(); err != nil {
		log.Printf("recording: migration commit error: %v", err)
		return
	}
	log.Printf("recording: migration complete")
}

func (s *Server) closeSpeakerStore() {
	if s.speakerDB != nil {
		s.speakerDB.Close()
	}
}

// getMeta holt einen Meta-Wert.
func (s *Server) getMeta(key string) string {
	var val string
	s.speakerDB.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&val)
	return val
}

// setMeta setzt einen Meta-Wert.
func (s *Server) setMeta(key, value string) {
	s.speakerDB.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)`, key, value)
}

// findPersonByNameID sucht eine Person per Name (case-insensitive).
func (s *Server) findPersonByName(name string) *SpeakerPerson {
	var p SpeakerPerson
	err := s.speakerDB.QueryRow(`SELECT id, name FROM persons WHERE name = ? COLLATE NOCASE`, name).Scan(&p.ID, &p.Name)
	if err != nil {
		return nil
	}
	return &p
}

// findOrCreatePerson holt eine Person per Name oder legt sie an.
func (s *Server) findOrCreatePerson(name string) SpeakerPerson {
	if p := s.findPersonByName(name); p != nil {
		return *p
	}
	res, err := s.speakerDB.Exec(`INSERT INTO persons (name) VALUES (?)`, name)
	if err != nil {
		log.Printf("recording: person insert error: %v", err)
		return SpeakerPerson{Name: name}
	}
	id, _ := res.LastInsertId()
	return SpeakerPerson{ID: int(id), Name: name}
}

// createGlobalSpeaker legt eine neue globale SPEAKER_XX Person an.
// Rufer muss speakerMu halten.
func (s *Server) createGlobalSpeaker() SpeakerPerson {
	val := s.getMeta("next_speaker_idx")
	idx := 0
	if val != "" {
		fmt.Sscanf(val, "%d", &idx)
	}
	name := fmt.Sprintf("SPEAKER_%02d", idx)
	s.setMeta("next_speaker_idx", fmt.Sprintf("%d", idx+1))

	res, err := s.speakerDB.Exec(`INSERT INTO persons (name) VALUES (?)`, name)
	if err != nil {
		log.Printf("recording: speaker insert error: %v", err)
		return SpeakerPerson{Name: name}
	}
	id, _ := res.LastInsertId()
	return SpeakerPerson{ID: int(id), Name: name}
}

// renamePerson ändert den Namen einer Person (z.B. SPEAKER_00 → Klaus).
func (s *Server) renamePerson(personID int, newName string) error {
	// Unique-Check
	if p := s.findPersonByName(newName); p != nil && p.ID != personID {
		return fmt.Errorf("name %q bereits vergeben (person %d)", newName, p.ID)
	}
	_, err := s.speakerDB.Exec(`UPDATE persons SET name = ? WHERE id = ?`, newName, personID)
	return err
}

// loadAllProfiles lädt alle Profile mit Embeddings in den Speicher.
func (s *Server) loadAllProfiles() []SpeakerProfile {
	rows, err := s.speakerDB.Query(`SELECT id, person_id, embedding, source, first_seen, last_seen FROM profiles`)
	if err != nil {
		return nil
	}
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

// matchSpeakerResult ist das Ergebnis eines Speaker-Matches.
type matchSpeakerResult struct {
	Person    SpeakerPerson
	ProfileID int
	Score     float64
	Matched   bool
}

// matchSpeaker findet die beste Person per Embedding-Cosine gegen ALLE Profile.
func (s *Server) matchSpeaker(embedding []float64) matchSpeakerResult {
	s.speakerMu.RLock()
	defer s.speakerMu.RUnlock()

	threshold := s.cfg.Recording.SpeakerMatch
	if threshold <= 0 {
		threshold = 0.75
	}

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

// addProfileForSession legt ein Profil für eine Person an (oder aktualisiert letztes-gesehen).
// Rufer muss speakerMu halten.
func (s *Server) addProfileForSession(personID int, embedding []float64, sourceSessionID string) int {
	threshold := s.cfg.Recording.SpeakerMatch
	if threshold <= 0 {
		threshold = 0.75
	}

	// Bestehendes Profil für diese Person mit ähnlichem Embedding → LastSeen updaten
	profiles := s.loadAllProfiles()
	for i, p := range profiles {
		if p.PersonID != personID {
			continue
		}
		score := cosineSimilarity(embedding, p.Embedding)
		if score >= threshold {
			now := time.Now().Format(time.RFC3339)
			s.speakerDB.Exec(`UPDATE profiles SET last_seen = ? WHERE id = ?`, now, p.ID)
			profiles[i].LastSeen = now
			return p.ID
		}
	}

	// Neues Profil anlegen
	embBytes, _ := json.Marshal(embedding)
	now := time.Now().Format(time.RFC3339)
	res, err := s.speakerDB.Exec(
		`INSERT INTO profiles (person_id, embedding, source, first_seen, last_seen) VALUES (?,?,?,?,?)`,
		personID, embBytes, sourceSessionID, now, now)
	if err != nil {
		log.Printf("recording: profile insert error: %v", err)
		return 0
	}
	id, _ := res.LastInsertId()
	return int(id)
}

// profileCountForPerson zählt Profile einer Person.
func (s *Server) profileCountForPerson(personID int) int {
	var count int
	s.speakerDB.QueryRow(`SELECT COUNT(*) FROM profiles WHERE person_id = ?`, personID).Scan(&count)
	return count
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
			SpaceID string `json:"space_id"` // veraltet, nur noch für Logging
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
			ID:           sessionID,
			SpaceID:      body.SpaceID,
			UserID:       r.Header.Get("x-access-token"),
			Created:      time.Now(),
			Fragments:    []RecordingFrag{},
			shareToken:   body.Share.Token,
			sharePasswd:  body.Share.Password,
			liveSpeakerByFrag: make(map[int]string),
			liveSpeakerMap:    make(map[string]SpeakerRef),
			liveSpeakerEmb:    make(map[string][]float64),
		}

		s.recMu.Lock()
		s.sessions[sessionID] = session
		s.recMu.Unlock()

		// WebDAV-Verzeichnisse sofort anlegen (MKCOL), damit Fragment-Uploads
		// während der Aufnahme funktionieren.
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
	log.Printf("recording: session/end called for %s", body.SessionID)

	s.recMu.Lock()
	session, exists := s.sessions[body.SessionID]
	if !exists {
		s.recMu.Unlock()
		log.Printf("recording: session/end: Session %s nicht gefunden", body.SessionID)
		writeChatError(w, http.StatusNotFound, "Session nicht gefunden")
		return
	}
	session.Done = true
	s.recMu.Unlock()
	log.Printf("recording: session/end: %s space=%s frags=%d audio=%d bytes",
		body.SessionID, session.SpaceID, len(session.Fragments), len(session.totalAudio))

	// 1. Session-End-Diarization: ein pyannote-Call auf komplettes Session-Audio,
	//    Segmente zeitbasiert auf Fragmente mappen, Speaker-Profile per Embedding anlegen.
	if s.cfg.Recording.DiarizeAPIBase != "" && len(session.totalAudio) > 0 && len(session.Fragments) > 0 {
		s.diarizeSessionEnd(session)
	}

	// 2. Komplettes Transkript zusammenstellen
	var transcriptBuilder strings.Builder
	for _, frag := range session.Fragments {
		if frag.Speaker != "" && frag.Speaker != "unknown" {
			fmt.Fprintf(&transcriptBuilder, "[%s]: ", frag.Speaker)
		}
		transcriptBuilder.WriteString(frag.Text)
		transcriptBuilder.WriteString("\n")
	}
	fullTranscript := transcriptBuilder.String()

	// 3a. WebDAV-Upload (auch bei leerem Transkript — Audio-File hochladen)
	if session.shareToken != "" && len(session.totalAudio) > 0 {
		uploadPath := s.webdavUploadRecording(session, fullTranscript)
		if uploadPath != "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"status":     "done",
				"transcript": "",
				"upload":     uploadPath,
				"message":    "Kein Transkript, Audio hochgeladen",
			})
			return
		}
	}
	if fullTranscript == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":      "done",
			"transcript":  "",
			"message":     "Kein Transkript (leere Session)",
		})
		return
	}

	// 3. LLM-Finalpass (Politur)
	finishedTranscript := fullTranscript
	if s.cfg.LLM.APIBase != "" {
		prompt := fmt.Sprintf(
			`Du erhältst ein Roh-Transkript einer Audio-Aufnahme. Korrigiere NUR:
- Tippfehler und Erkennungsfehler
- Fehlende Satzzeichen (Punkte, Kommas, Frage-/Ausrufezeichen)
- Grammatik und Rechtschreibung
- Füllwörter (äh, ähm, also also)

WICHTIG:
- Füge KEINE neuen Wörter, Sätze, Floskeln oder Formulierungen hinzu.
- Erfinde KEINEN Inhalt. NUR was im Roh-Transkript steht.
- Entferne KEINE inhaltlichen Aussagen.
- Behalte die Sprecher-Zuordnungen bei ([Name]: Text).

Gib NUR den korrigierten Text zurück, keine Erklärungen.

Roh-Transkript:
%s`, fullTranscript)

		log.Printf("recording: LLM-Finalpass für Session %s (%d chars)", body.SessionID, len(fullTranscript))
		polished := s.llmChat(prompt)
		if polished != "" {
			finishedTranscript = polished
		}
	}

	// 4. WebDAV-Upload (via Public-Link-Share)
	uploadPath := ""
	if session.shareToken != "" {
		uploadPath = s.webdavUploadRecording(session, finishedTranscript)
	} else {
		log.Printf("recording: session/end: kein Share, kein WebDAV-Upload")
	}

	// 5. Response (inkl. Fragmente mit Speaker-Zuweisung nach Session-End-Diarization)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "done",
		"session_id": body.SessionID,
		"transcript": finishedTranscript,
		"upload":     uploadPath,
		"fragments":  session.Fragments,
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
	log.Printf("recording: chunk session=%s audio=%d bytes", sessionID, len(audioData))

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

	// Fragment fertig → Whisper (SPEAKER wird per Session-End-Diarization nachgeliefert)
	if fragmentComplete {
		fragAudio := session.takeFragAudio()
		fragAudioLen := len(fragAudio)

		// Leeres Fragment (z.B. nur Stille) → überspringen
		if fragAudioLen < 2 {
			log.Printf("recording: fragment %d leer (%d bytes), übersprungen", fragmentIdx, fragAudioLen)
			return
		}

		log.Printf("recording: fragment %d fertig: %d bytes (%.1fs)", fragmentIdx, fragAudioLen, float64(fragAudioLen)/16000.0)

		sseWrite(w, flusher, map[string]any{
			"type":     "status",
			"fragment": fragmentIdx,
			"status":   "processing",
		})

		text := s.whisperTranscribeBytes(fragAudio)

		if text == "" {
			sseWrite(w, flusher, map[string]any{
				"type":     "error",
				"fragment": fragmentIdx,
				"message":  "Transkription fehlgeschlagen",
			})
			session.markFragFailed(fragmentIdx)
			return
		}

		session.addFragment(fragmentIdx, text, "unknown")

		// Fragment-Audio asynchron per WebDAV hochladen (crash-safe)
		if session.shareToken != "" {
			go s.webdavUploadFragment(session, fragmentIdx, fragAudio)
		}

		sseWrite(w, flusher, map[string]any{
			"type":     "final",
			"fragment": fragmentIdx,
			"text":     text,
			"method":   "whisper",
		})

		// Rolling-Window-Diarization: Live-Speaker für das fertige Fragment
		if s.cfg.Recording.LiveDiarize && s.cfg.Recording.DiarizeAPIBase != "" {
			liveSpeaker := s.diarizeWindowLive(session, fragmentIdx)
			if liveSpeaker != "" {
				session.addFragmentSpeaker(fragmentIdx, liveSpeaker)
				sseWrite(w, flusher, map[string]any{
					"type":     "speaker",
					"fragment": fragmentIdx,
					"speaker":  liveSpeaker,
				})

				// LLM Word-Boundary-Correction: prüfen ob die letzten Wörter
				// des VORHERIGEN Fragments zum aktuellen Speaker gehören.
				wordCount := s.llmSplitCheck(session, fragmentIdx)
				if wordCount > 0 {
					session.mu.Lock()
					prevIdx := 0
					if len(session.Fragments) >= 2 {
						prevIdx = session.Fragments[len(session.Fragments)-2].Index
					}
					session.mu.Unlock()
					session.applyLlmSplit(prevIdx, wordCount, liveSpeaker)
					sseWrite(w, flusher, map[string]any{
						"type":     "split",
						"fragment": prevIdx,
						"words":    wordCount,
						"speaker":  liveSpeaker,
					})
				}
			}
		}

		sseWrite(w, flusher, map[string]any{
			"type":     "done",
			"fragment": fragmentIdx,
			"text":     text,
		})
	}
}

// speakerAPIProfile ist ein Profil in der GET-Antwort.
type speakerAPIProfile struct {
	ID        int    `json:"id"`
	FirstSeen string `json:"first_seen"`
	LastSeen  string `json:"last_seen"`
}

// speakerAPIPerson ist die GET-Antwort pro Person inkl. Profile.
type speakerAPIPerson struct {
	ID           int               `json:"id"`
	Name         string            `json:"name"`
	ProfileCount int               `json:"profile_count"`
	Profiles     []speakerAPIProfile `json:"profiles"`
}

// handleRecordingSpeakers: GET = Personen + Profile auflisten, PUT = Person anlegen.
func (s *Server) handleRecordingSpeakers(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.speakerMu.RLock()
		rows, err := s.speakerDB.Query(`SELECT id, name FROM persons ORDER BY id`)
		if err != nil {
			s.speakerMu.RUnlock()
			writeChatError(w, 500, "db query: "+err.Error())
			return
		}
		type personRow struct {
			ID   int
			Name string
		}
		var personRows []personRow
		for rows.Next() {
			var pr personRow
			rows.Scan(&pr.ID, &pr.Name)
			personRows = append(personRows, pr)
		}
		rows.Close()
		s.speakerMu.RUnlock()

		// Pro Person: Profile laden
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
		// Person anlegen: {name: "Anna Brandis"}
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
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

// handleRecordingSpeakerLink: PUT — SPEAKER_XX umbenennen (Benennung).
// {speaker_id: "SPEAKER_00", person_name: "Klaus"}
// speaker_id kann auch "SPEAKER_00/1" sein (Profile-Suffix wird ignoriert).
func (s *Server) handleRecordingSpeakerLink(w http.ResponseWriter, r *http.Request) {
	if !s.chatVerifyToken(w, r) {
		return
	}
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		SpeakerID  string `json:"speaker_id"`  // "SPEAKER_00" oder "SPEAKER_00/1"
		PersonName string `json:"person_name"` // "Klaus"
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeChatError(w, http.StatusBadRequest, "ungültiges JSON: "+err.Error())
		return
	}
	if body.PersonName == "" {
		writeChatError(w, http.StatusBadRequest, "person_name fehlt")
		return
	}

	// Profile-Suffix entfernen: "SPEAKER_00/1" → "SPEAKER_00"
	speakerName := body.SpeakerID
	if idx := strings.Index(speakerName, "/"); idx >= 0 {
		speakerName = speakerName[:idx]
	}
	if speakerName == "" {
		writeChatError(w, http.StatusBadRequest, "speaker_id fehlt")
		return
	}

	s.speakerMu.Lock()
	person := s.findPersonByName(speakerName)
	if person == nil {
		s.speakerMu.Unlock()
		writeChatError(w, 404, "Person '"+speakerName+"' nicht gefunden")
		return
	}
	if err := s.renamePerson(person.ID, body.PersonName); err != nil {
		s.speakerMu.Unlock()
		writeChatError(w, http.StatusConflict, err.Error())
		return
	}
	renamed := SpeakerPerson{ID: person.ID, Name: body.PersonName}
	s.speakerMu.Unlock()

	log.Printf("recording: speaker rename: %s → %s (person %d)", speakerName, body.PersonName, person.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(renamed)
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

	silenceTimeoutMs := s.cfg.Recording.SilenceTimeout
	if silenceTimeoutMs <= 0 {
		silenceTimeoutMs = 800
	}
	partialIntervalSec := s.cfg.Recording.PartialInterval
	if partialIntervalSec <= 0 {
		partialIntervalSec = 3
	}
	maxFragmentSec := s.cfg.Recording.MaxFragmentSec
	if maxFragmentSec <= 0 {
		maxFragmentSec = 30
	}

	// Audio dekodieren + RMS
	samples := decodeAudioToPCM16(audioData)
	rms := 0.0
	if samples != nil {
		rms = rmsFromPCM16(samples)
	}

	isSilent := s.vadIsSilent(rms)
	log.Printf("recording: chunk rms=%.4f silent=%v audio=%d bytes", rms, isSilent, len(audioData))

	// Audio in Session-Buffer einhängen (nur PCM16, nicht rohes WebM)
	if samples != nil {
		session.totalAudio = append(session.totalAudio, pcm16ToBytes(samples)...)
		session.totalSamples += len(samples)
	}

	// VAD-State-Machine (Audio-Zeit basierend auf totalSamples, nicht Wall-Clock)
	fragmentComplete := false
	silenceTimeoutSamples := silenceTimeoutMs * 16 // ms → samples (16kHz: 16 samples/ms)
	maxFragmentSamples := maxFragmentSec * 16000   // sec → samples
	partialIntervalSamples := partialIntervalSec * 16000

	if isSilent {
		if session.silenceSinceSamples > 0 {
			silenceDurSamples := session.totalSamples - session.silenceSinceSamples
			if silenceDurSamples >= silenceTimeoutSamples {
				fragmentComplete = true
			}
		} else {
			session.silenceSinceSamples = session.totalSamples
		}
		session.speechActive = false
	} else {
		if session.silenceSinceSamples > 0 {
			session.silenceSinceSamples = 0
		}
		session.speechActive = true

		if session.fragAudio == nil {
			session.fragStartSamples = session.totalSamples
			session.fragIndex++
			session.lastPartialSamples = 0
		}
		session.fragAudio = append(session.fragAudio, audioData...)
	}

	// Fragment-Ende: letztes Sprach-Sample (silenceSinceSamples), nicht totalSamples.
	// totalSamples enthält die Stille nach dem letzten Wort.
	fragEndSamples := session.totalSamples
	if session.silenceSinceSamples > 0 {
		fragEndSamples = session.silenceSinceSamples
	}

	fragCompleteByDuration := false
	if session.fragAudio != nil {
		fragDurSamples := fragEndSamples - session.fragStartSamples
		if fragDurSamples >= maxFragmentSamples {
			fragCompleteByDuration = true
			log.Printf("recording: fragment %d duration-end: %.1fs >= max %.1fs",
				session.fragIndex, float64(fragDurSamples)/16000.0, float64(maxFragmentSamples)/16000.0)
		} else if fragDurSamples%16000 < 160 { // ~1s logging
			log.Printf("recording: fragment %d duration: %.1fs / max %.1fs",
				session.fragIndex, float64(fragDurSamples)/16000.0, float64(maxFragmentSamples)/16000.0)
		}
	}

	var partialText string
	if !isSilent && session.fragAudio != nil &&
		(session.lastPartialSamples == 0 ||
			session.totalSamples-session.lastPartialSamples >= partialIntervalSamples) {
		partialText = s.whisperTranscribeBytes(session.fragAudio)
		session.lastPartialSamples = session.totalSamples
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
	session.silenceSinceSamples = 0
	session.speechActive = false
	session.lastPartialSamples = 0
	session.prevTranscript = ""
	return audio
}

// addFragment fügt ein abgeschlossenes Fragment zur Session hinzu.
func (session *RecordingSession) addFragment(idx int, text, speaker string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	// fragStartSamples = totalSamples zum Fragment-Start (absolute Session-Zeit).
	// silenceSinceSamples = totalSamples bei letztem Sprach-Sample (Fragment-Ende).
	start := float64(session.fragStartSamples) / 16000.0
	if start < 0 {
		start = 0
	}
	// Fragment-Ende: letztes Sprach-Sample, nicht totalSamples (das enthält Stille)
	endSamples := session.totalSamples
	if session.silenceSinceSamples > 0 {
		endSamples = session.silenceSinceSamples
	}
	end := float64(endSamples) / 16000.0
	if end < start {
		end = start
	}

	session.Fragments = append(session.Fragments, RecordingFrag{
		Index:    idx,
		Text:     text,
		Speaker:  speaker,
		Start:    start,
		End:      end,
		Duration: end - start,
		Status:   "done",
	})
	session.prevTranscript = text
}

// markFragFailed markiert ein Fragment als fehlgeschlagen.
func (session *RecordingSession) markFragFailed(idx int) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.fragAudio = nil
	session.silenceSinceSamples = 0
	session.speechActive = false
	session.lastPartialSamples = 0
}

// ── Session-End-Diarization ──────────────────────────────────

// ── Rolling-Window-Diarization (Live-Speaker) ────────────────────

// diarizeWindowLive führt eine Rolling-Window-Diarization durch.
// WICHTIG: session.mu wird NICHT gehalten (wird in handleRecordingChunk aufgerufen).
// Returns Speaker-Name für das Fragment das soeben fertig wurde, oder "".
func (s *Server) diarizeWindowLive(session *RecordingSession, completedFragIdx int) string {
	windowSec := s.cfg.Recording.LiveWindowSec
	if windowSec <= 0 {
		windowSec = 60
	}
	overlapSec := s.cfg.Recording.LiveOverlapSec
	if overlapSec <= 0 {
		overlapSec = 10
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	// Genug Audio seit letztem Cursor?
	if session.totalSamples-session.windowCursorSamples < windowSec*16000 {
		return ""
	}

	// Window berechnen: [cursor - overlap, cursor + window]
	winEndSamples := session.windowCursorSamples + windowSec*16000
	if winEndSamples > session.totalSamples {
		winEndSamples = session.totalSamples
	}
	winStartSamples := session.windowCursorSamples - overlapSec*16000
	if winStartSamples < 0 {
		winStartSamples = 0
	}
	// Defensive: Window muss im totalAudio-Bereich liegen
	if winEndSamples*2 > len(session.totalAudio) {
		winEndSamples = len(session.totalAudio) / 2
	}
	if winEndSamples-winStartSamples < 3*16000 {
		return ""
	}

	// Sub-Window von totalAudio (raw PCM16)
	windowAudio := session.totalAudio[winStartSamples*2 : winEndSamples*2]
	result := s.diarizeAudioBytes(windowAudio)
	if result == nil || len(result.Segments) == 0 {
		return ""
	}

	// Per Label: Embedding extrahieren + Envelope [start, end] berechnen
	type windowEnv struct {
		start, end float64
		emb        []float64
	}
	envByLabel := make(map[string]*windowEnv)
	var labelOrder []string
	for _, seg := range result.Segments {
		e, ok := envByLabel[seg.Speaker]
		if !ok {
			e = &windowEnv{start: seg.Start, end: seg.End}
			envByLabel[seg.Speaker] = e
			labelOrder = append(labelOrder, seg.Speaker)
		}
		if seg.Start < e.start {
			e.start = seg.Start
		}
		if seg.End > e.end {
			e.end = seg.End
		}
	}
	for _, label := range labelOrder {
		if emb, ok := result.SpeakerEmbeddings[label]; ok {
			envByLabel[label].emb = emb
		}
	}

	// Label-Alignment: pyannote-Label → stabile SpeakerRef
	labelRefs := make(map[string]SpeakerRef)
	for _, label := range labelOrder {
		emb := envByLabel[label].emb
		if len(emb) == 0 {
			continue
		}
		ref, ok := s.alignWindowLabel(session, label, emb)
		if ok {
			labelRefs[label] = ref
		}
	}

	// Segmente auf absolute Session-Zeit umrechnen
	winStartSec := float64(winStartSamples) / 16000.0
	var absSegs []diarizeSegment
	for _, seg := range result.Segments {
		absSegs = append(absSegs, diarizeSegment{
			Speaker: seg.Speaker,
			Start:   winStartSec + seg.Start,
			End:     winStartSec + seg.End,
		})
	}

	// Fragmente im Window-Zeitfenster [cursor, winEnd] zuweisen
	cursorSec := float64(session.windowCursorSamples) / 16000.0
	winEndSec := float64(winEndSamples) / 16000.0
	for i := range session.Fragments {
		frag := &session.Fragments[i]
		mid := (frag.Start + frag.End) / 2
		if mid < cursorSec || mid > winEndSec {
			continue
		}
		ref := dominantSpeakerAt(absSegs, labelRefs, mid)
		if ref.PersonName == "" {
			continue
		}
		name := ref.String()
		session.liveSpeakerByFrag[frag.Index] = name
		if frag.Speaker == "" || frag.Speaker == "unknown" {
			frag.Speaker = name
		}
	}

	// Cursor vorrücken
	session.windowCursorSamples = winEndSamples

	log.Printf("recording: live-diarize: window [%.0f-%.0f]s → %d Speaker, cursor=%.0fs",
		cursorSec, winEndSec, len(labelRefs), winEndSec)

	return session.liveSpeakerByFrag[completedFragIdx]
}

// alignWindowLabel aligniert ein pyannote-Window-Label auf eine stabile SpeakerRef.
// Drei Stufen: Identity, Cosine-Alignment, Global Profile Match.
// Precondition: session.mu wird vom Caller gehalten (KEIN Lock hier).
func (s *Server) alignWindowLabel(session *RecordingSession, label string, emb []float64) (SpeakerRef, bool) {
	// 1. Identity: Label existiert schon in der Session
	if ref, ok := session.liveSpeakerMap[label]; ok {
		return ref, true
	}

	// 2. Cosine-Alignment gegen bekannte stable Refs (nur bei ≥2 Labels)
	bestRef := SpeakerRef{}
	bestScore := 0.0
	for l, r := range session.liveSpeakerMap {
		if e, ok := session.liveSpeakerEmb[l]; ok && len(e) > 0 {
			if sc := cosineSimilarity(emb, e); sc > bestScore {
				bestScore = sc
				bestRef = r
			}
		}
	}
	// 0.80 statt 0.60: verschiedene Sprecher haben typischerweise 0.5-0.7 Cosine.
	// Nur derselbe Sprecher mit leicht variierendem Embedding (unterschiedliche
	// Windows) liegt zuverlässig ≥ 0.80.
	const windowAlignThreshold = 0.80
	if bestScore >= windowAlignThreshold && bestRef.PersonName != "" {
		session.liveSpeakerMap[label] = bestRef
		session.liveSpeakerEmb[label] = emb
		log.Printf("recording: live-diarize: label %s → %q (cosine=%.2f)", label, bestRef.PersonName, bestScore)
		return bestRef, true
	}

	// 3. Global Profile Match (REUSE matchSpeaker + create/addProfile Logik)
	match := s.matchSpeaker(emb)
	s.speakerMu.Lock()
	var ref SpeakerRef
	if match.Matched {
		pid := s.addProfileForSession(match.Person.ID, emb, session.ID)
		ref = SpeakerRef{PersonName: match.Person.Name, PersonID: match.Person.ID, ProfileID: pid}
		log.Printf("recording: live-diarize: label %s → %q (match=%.2f)", label, match.Person.Name, match.Score)
	} else if match.Score >= 0.70 && strings.HasPrefix(match.Person.Name, "SPEAKER_") {
		pid := s.addProfileForSession(match.Person.ID, emb, session.ID)
		ref = SpeakerRef{PersonName: match.Person.Name, PersonID: match.Person.ID, ProfileID: pid}
		log.Printf("recording: live-diarize: label %s → %q (unbekannt-zone=%.2f)", label, match.Person.Name, match.Score)
	} else {
		person := s.createGlobalSpeaker()
		pid := s.addProfileForSession(person.ID, emb, session.ID)
		ref = SpeakerRef{PersonName: person.Name, PersonID: person.ID, ProfileID: pid}
		log.Printf("recording: live-diarize: label %s → neue Person %q (best=%.2f)", label, person.Name, match.Score)
	}
	s.speakerMu.Unlock()

	session.liveSpeakerMap[label] = ref
	session.liveSpeakerEmb[label] = emb
	return ref, true
}

// dominantSpeakerAt findet den Speaker mit der längsten Abdeckung an absMid.
// Bei Overlap: längeres Segment gewinnt.
func dominantSpeakerAt(segs []diarizeSegment, labelRefs map[string]SpeakerRef, absMid float64) SpeakerRef {
	bestDur := 0.0
	var best SpeakerRef
	for _, seg := range segs {
		if absMid >= seg.Start && absMid < seg.End {
			dur := seg.End - seg.Start
			if dur > bestDur {
				bestDur = dur
				best = labelRefs[seg.Speaker]
			}
		}
	}
	return best
}

// addFragmentSpeaker setzt den Speaker für ein Fragment (wenn noch unknown).
func (session *RecordingSession) addFragmentSpeaker(idx int, speaker string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	for i := range session.Fragments {
		if session.Fragments[i].Index == idx {
			if session.Fragments[i].Speaker == "" || session.Fragments[i].Speaker == "unknown" {
				session.Fragments[i].Speaker = speaker
			}
			break
		}
	}
}

// ── LLM Word-Boundary-Correction (Live) ─────────────────────────────

// llmSplitPrompt ist der Prompt für die Wort-Grenzen-Korrektur.
// Das LLM prüft ob am Ende des vorherigen Fragments Wörter stehen die
// zum aktuellen (neuen) Sprecher gehören.
const llmSplitPrompt = `Du bist ein Transkript-Korrekter. Mehrere Sprecher wechseln sich ab.
Die Tonerkennung schlägt manchmal Wörter des neuen Sprechers dem
vorherigen zu (Sprecherwechsel zu spät erkannt).

Kontext:
--- [%s] %s ---
%s

--- [%s] %s ---
%s

--- [%s] %s ---
%s

Prüfe: Hängen am ENDE des 2. Abschnitts Wörter, die inhaltlich/syntaktisch
zum 3. Abschnitt gehören? Achte auf Satzanfänge ("Wenn", "Und", "Dass",
"60 Prozent" etc.), Subjekt-Prädikat-Strukturen, Konjunktionen.

Wenn JA: Gib die ANZAHL der Wörter am Ende des 2. Abschnitts an die zum
3. Abschnitt gehören.
SPLIT: <Anzahl>
BEGRÜNDUNG: <eine Zeile>

Wenn NEIN:
SPLIT: NONE
BEGRÜNDUNG: <eine Zeile>`

// llmSplitCheck ruft das LLM auf um zu prüfen ob die letzten Wörter des
// vorherigen Fragments zum aktuellen (neuen) Sprecher gehören.
// Liefert die Wort-Anzahl (0 = kein Split).
func (s *Server) llmSplitCheck(session *RecordingSession, newFragIdx int) int {
	session.mu.Lock()
	frags := make([]RecordingFrag, len(session.Fragments))
	copy(frags, session.Fragments)
	session.mu.Unlock()

	if len(frags) < 2 {
		return 0
	}

	// prev = das Fragment vor dem neuen, cur = das neue, next = ggf. das nach dem neuen
	// In der Live-Pipeline: das "neue" Fragment ist das letzte in der Liste.
	prev := frags[len(frags)-2]
	cur := frags[len(frags)-1]

	// Für besseren Kontext: das Fragment VOR dem prev (falls vorhanden)
	var contextFrag RecordingFrag
	if len(frags) >= 3 {
		contextFrag = frags[len(frags)-3]
	} else {
		contextFrag = RecordingFrag{Speaker: "(none)", Text: "(kein vorheriges Fragment)"}
	}

	fmtCtx := fmt.Sprintf("%.1f-%.1fs", contextFrag.Start, contextFrag.End)
	fmtPrev := fmt.Sprintf("%.1f-%.1fs", prev.Start, prev.End)
	fmtCur := fmt.Sprintf("%.1f-%.1fs", cur.Start, cur.End)

	prompt := fmt.Sprintf(llmSplitPrompt,
		fmtCtx, contextFrag.Speaker, contextFrag.Text,
		fmtPrev, prev.Speaker, prev.Text,
		fmtCur, cur.Speaker, cur.Text,
	)

	messages := []chatMessage{
		{Role: "user", Content: prompt},
	}
	result, _ := s.llmCompleteOptsBackend(messages, nil, nil, "split")

	// Parse: "SPLIT: N" oder "SPLIT: NONE"
	for _, line := range strings.Split(result, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "SPLIT:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "SPLIT:"))
			val = strings.TrimPrefix(strings.TrimPrefix(val, "SPLIT:"), ":")
			if strings.EqualFold(val, "NONE") || val == "" {
				return 0
			}
			var n int
			if _, err := fmt.Sscanf(val, "%d", &n); err == nil && n > 0 && n < len(strings.Fields(prev.Text)) {
				log.Printf("recording: llm-split: fragment %d, letzte %d Wörter → Speaker %s",
					prev.Index, n, cur.Speaker)
				return n
			}
		}
	}
	return 0
}

// applyLlmSplit spaltet ein Fragment: die letzten <wordCount> Wörter
// bekommen den Speaker des nächsten Fragments.
func (session *RecordingSession) applyLlmSplit(fragIdx int, wordCount int, nextSpeaker string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	for i := range session.Fragments {
		if session.Fragments[i].Index != fragIdx {
			continue
		}
		frag := &session.Fragments[i]
		words := strings.Fields(frag.Text)
		if wordCount >= len(words) {
			return
		}

		splitAt := len(words) - wordCount
		partA := strings.Join(words[:splitAt], " ")
		partB := strings.Join(words[splitAt:], " ")

		// Zeit-Split proportional zu Wortanzahl
		duration := frag.End - frag.Start
		splitTime := frag.Start + duration*float64(splitAt)/float64(len(words))

		// Fragment A bleibt, Fragment B wird neu angehängt
		frag.Text = partA
		frag.End = splitTime
		frag.Duration = splitTime - frag.Start

		newFrag := RecordingFrag{
			Index:    fragIdx + 1000, // eindeutiger Sub-Index
			Text:     partB,
			Speaker:  nextSpeaker,
			Start:    splitTime,
			End:      frag.End,
			Duration: frag.End - splitTime,
			Status:   "split",
		}
		// Nach dem alten Fragment einfügen
		session.Fragments = append(session.Fragments[:i+1], append([]RecordingFrag{newFrag}, session.Fragments[i+1:]...)...)
		break
	}
}

// diarizeSessionEnd führt die Diarization einmal auf dem kompletten
// Session-Audio aus und mappt die Segmente zeitbasiert auf Fragmente.
// Pro einzigem Speaker wird ein Envelope-Mean-Embedding berechnet und
// mit matchSpeaker gegen die Speaker-Profile geprüft.
func (s *Server) diarizeSessionEnd(session *RecordingSession) {
	log.Printf("recording: session/end-diarize: start für %s (audio=%d bytes)",
		session.ID, len(session.totalAudio))
	start := time.Now()

	result := s.diarizeAudioBytes(session.totalAudio)
	if result == nil || len(result.Segments) == 0 {
		log.Printf("recording: session/end-diarize: keine Segmente für %s", session.ID)
		return
	}
	log.Printf("recording: session/end-diarize: %d Segmente, %d Speaker, embeddings=%v",
		len(result.Segments), len(result.Speakers), len(result.SpeakerEmbeddings) > 0)

	// 1. Pro Speaker: Envelope-Mean aus allen Segments
	type speakerEnvelope struct {
		label    string
		start    float64
		end      float64
		envelope []float64
	}
	envByLabel := map[string]*speakerEnvelope{}
	var envelopeOrder []string
	for _, seg := range result.Segments {
		env, ok := envByLabel[seg.Speaker]
		if !ok {
			env = &speakerEnvelope{label: seg.Speaker, start: seg.Start, end: seg.End, envelope: []float64{}}
			envByLabel[seg.Speaker] = env
			envelopeOrder = append(envelopeOrder, seg.Speaker)
		}
		// Envelope: frühester Start, spätestes Ende
		if seg.Start < env.start {
			env.start = seg.Start
		}
		if seg.End > env.end {
			env.end = seg.End
		}
		// Embedding-Vector sammeln
		if emb, ok := result.SpeakerEmbeddings[seg.Speaker]; ok {
			env.envelope = append(env.envelope, emb...)
		}
	}

	// 2. Globale Speaker-IDs + Person-Matching per Embedding
	//    speakerDisplay: rawLabel → SpeakerRef (Person-Name + ProfileID)
	speakerDisplay := map[string]SpeakerRef{}
	for _, label := range envelopeOrder {
		env := envByLabel[label]
		if len(env.envelope) == 0 {
			log.Printf("recording: session/end-diarize: %s kein Embedding", label)
			continue
		}

		// Envelope-Mean
		numSegments := len(env.envelope) / len(result.SpeakerEmbeddings[label])
		embDim := len(result.SpeakerEmbeddings[label])
		meanEmb := make([]float64, embDim)
		for si := 0; si < numSegments; si++ {
			for d := 0; d < embDim; d++ {
				meanEmb[d] += env.envelope[si*embDim+d] / float64(numSegments)
			}
		}

		// 1a. Session-lokales Live-Match: wenn der Rolling-Window-Pfad
		//     diesen Sprecher schon zugeordnet hat, wiederverwenden.
		//     Verhindert dass Session-End eine neue Person anlegt, wenn
		//     die Envelope-Mean nicht gegen globale Profile matcht.
		session.mu.Lock()
		liveRef := SpeakerRef{}
		liveMatched := false
		if len(session.liveSpeakerEmb) > 0 {
			var bestLiveScore float64
			for _, emb := range session.liveSpeakerEmb {
				if sc := cosineSimilarity(meanEmb, emb); sc > bestLiveScore {
					bestLiveScore = sc
				}
			}
			// 0.75: Envelope-Mean aus 30+ Segmenten vs. Einzel-Window-Embedding
			// kann abweichen. 0.80 wäre zu streng, 0.60 zu locker.
			if bestLiveScore >= 0.75 {
				// Besten Embedding finden und zugehörigen Ref
				for l, emb := range session.liveSpeakerEmb {
					if sc := cosineSimilarity(meanEmb, emb); sc >= 0.75 {
						if ref, ok := session.liveSpeakerMap[l]; ok {
							liveRef = ref
							liveMatched = true
							log.Printf("recording: session/end-diarize: %s → Live-Speaker %q (cosine=%.2f)",
								label, ref.PersonName, sc)
							break
						}
					}
				}
			}
		}
		session.mu.Unlock()

		if liveMatched {
			s.speakerMu.Lock()
			profileID := s.addProfileForSession(liveRef.PersonID, meanEmb, session.ID)
			speakerDisplay[label] = SpeakerRef{PersonName: liveRef.PersonName, PersonID: liveRef.PersonID, ProfileID: profileID}
			s.speakerMu.Unlock()
			continue
		}

		// Cosine-Matching gegen ALLE Profile → Person
		match := s.matchSpeaker(meanEmb)
		s.speakerMu.Lock()
		if match.Matched {
			// Bekannte Person: Profil updaten oder anlegen
			profileID := s.addProfileForSession(match.Person.ID, meanEmb, session.ID)
			speakerDisplay[label] = SpeakerRef{PersonName: match.Person.Name, PersonID: match.Person.ID, ProfileID: profileID}
			log.Printf("recording: session/end-diarize: %s → Person %q (profile %d, %.2f)",
				label, match.Person.Name, profileID, match.Score)
		} else {
			// Unbekannte Stimme.
			// Falls der Score in der "Unbekannt-Zone" liegt (0.70–0.75) und die
			// beste Person noch ein SPEAKER_XX ist (nicht manuell benannt),
			// wiederverwenden statt neu anlegen — verhindert dass derselbe
			// unbekannte Sprecher in zwei Sessions zwei verschiedene SPEAKER_XX
			// bekommt.
			const unknownToUnknownThreshold = 0.70
			if match.Score >= unknownToUnknownThreshold &&
				strings.HasPrefix(match.Person.Name, "SPEAKER_") {
				profileID := s.addProfileForSession(match.Person.ID, meanEmb, session.ID)
				speakerDisplay[label] = SpeakerRef{PersonName: match.Person.Name, PersonID: match.Person.ID, ProfileID: profileID}
				log.Printf("recording: session/end-diarize: %s → wiederverwende %q (profile %d, score=%.2f, unbekannt-zone)",
					label, match.Person.Name, profileID, match.Score)
			} else {
				person := s.createGlobalSpeaker()
				profileID := s.addProfileForSession(person.ID, meanEmb, session.ID)
				speakerDisplay[label] = SpeakerRef{PersonName: person.Name, PersonID: person.ID, ProfileID: profileID}
				log.Printf("recording: session/end-diarize: %s → neue Person %q (profile %d, best=%.2f)",
					label, person.Name, profileID, match.Score)
			}
		}
		s.speakerMu.Unlock()
	}

	// 3. Pro Fragment: Speaker-Wechsel-Erkennung + Fragment-Splitting
	//    Große VAD-Fragmente (164s in Sitzungen) enthalten mehrere Sprecher.
	//    Nach der Diarization an Speaker-Wechsel-Grenzen aufteilen,
	//    Text pro Zeitspanne proportional verteilen.
	session.mu.Lock()
	newFrags := make([]RecordingFrag, 0, len(session.Fragments))
	splitCount := 0

	for _, frag := range session.Fragments {
		if frag.Duration <= 0 || frag.Text == "" {
			newFrags = append(newFrags, frag)
			continue
		}

		// Diarization-Segmente die mit diesem Fragment überlappen, extrahieren.
		// Segmente < 1s ignorieren (pyannote-Mikro-Artefakte bei Overlap/Noise).
		var overlapping []diarizeSegment
		for _, seg := range result.Segments {
			ovStart := math.Max(frag.Start, seg.Start)
			ovEnd := math.Min(frag.End, seg.End)
			if ovEnd-ovStart < 1.0 {
				continue
			}
			if ovEnd > ovStart {
				overlapping = append(overlapping, diarizeSegment{
					Speaker: seg.Speaker,
					Start:   ovStart,
					End:     ovEnd,
				})
			}
		}
		if len(overlapping) == 0 {
			newFrags = append(newFrags, frag)
			continue
		}

		// Sortieren nach Start
		sort.Slice(overlapping, func(i, j int) bool {
			return overlapping[i].Start < overlapping[j].Start
		})

		// Überlappende Segmente zu Zeitspannen mit dominanter Stimme zusammenfassen
		// (pyannote-Overlap: bei 2 Speakern gleichzeitig gewinnt der mit mehr Dauer)
		type span struct {
			start, end float64
			speaker    string
		}
		var spans []span
		for _, seg := range overlapping {
			// Segment auf Fragment-Start clampen (Fragment beginnt erst hier)
			clampedStart := seg.Start
			if clampedStart < frag.Start {
				clampedStart = frag.Start
			}
			if len(spans) > 0 && clampedStart <= spans[len(spans)-1].end {
				// Overlap: dominante Stimme der Kombination bestimmen
				last := &spans[len(spans)-1]
				ovWith := clampedStart
				if ovWith < last.start {
					ovWith = last.start
				}
				lastDur := last.end - ovWith
				segDur := seg.End - ovWith
				if segDur > lastDur {
					last.speaker = seg.Speaker
				}
				if seg.End > last.end {
					last.end = seg.End
				}
			} else {
				spans = append(spans, span{clampedStart, seg.End, seg.Speaker})
			}
		}

		// Fallback: nur eine Zeitspanne → dominanter Speaker, kein Split
		if len(spans) <= 1 {
			sp := spans[0]
			speakerStr := "unknown"
			if ref, ok := speakerDisplay[sp.speaker]; ok {
				speakerStr = ref.String()
			}
			frag.Speaker = speakerStr
			frag.Segments = []FragSpeakerSeg{{
				Speaker: speakerStr,
				Start:   0,
				End:     round2(frag.Duration),
				Text:    frag.Text,
			}}
			newFrags = append(newFrags, frag)
			continue
		}

		// Fragment an Speaker-Wechsel-Grenzen splitten
		words := strings.Fields(frag.Text)
		if len(words) == 0 {
			newFrags = append(newFrags, frag)
			continue
		}

		// Pro Wort: Zeitspanne bestimmen → Speaker zuordnen
		// Wort i liegt bei relativer Zeit i/N * Duration im Fragment
		wordSpk := make([]string, len(words))
		for wi, _ := range words {
			relTime := float64(wi) / float64(len(words)) * frag.Duration
			absTime := frag.Start + relTime
			wordSpk[wi] = spans[0].speaker // Fallback: erste Span
			for _, sp := range spans {
				if absTime >= sp.start && absTime < sp.end {
					wordSpk[wi] = sp.speaker
					break
				}
			}
		}

		// Kontiguierte Wortgruppen mit gleichem Speaker zusammenfassen
		type subFrag struct {
			speaker  string
			startIdx int
			endIdx   int // exklusiv
		}
		const minWords = 2 // Sub-Fragmente mit < 2 Wörtern werden zusammengeführt
		var subs []subFrag
		for wi := 0; wi < len(words); {
			spk := wordSpk[wi]
			end := wi + 1
			for end < len(words) && wordSpk[end] == spk {
				end++
			}
			subs = append(subs, subFrag{spk, wi, end})
			wi = end
		}

		// Mikro-Fragmente (< minWords) mit Nachbar zusammenführen
		if len(subs) > 1 {
			merged := []subFrag{subs[0]}
			for i := 1; i < len(subs); i++ {
				cur := subs[i]
				prev := &merged[len(merged)-1]
				curWords := cur.endIdx - cur.startIdx
				if curWords < minWords {
					// Zu kurz → in vorheriges aufnehmen
					prev.endIdx = cur.endIdx
					continue
				}
				// Prüfen ob vorheriges jetzt zu kurz ist
				prevWords := prev.endIdx - prev.startIdx
				if prevWords < minWords && len(merged) > 1 {
					pprev := &merged[len(merged)-2]
					pprev.endIdx = prev.endIdx
					merged = merged[:len(merged)-1]
					continue
				}
				merged = append(merged, cur)
			}
			// Letztes kann auch zu kurz sein
			if len(merged) > 1 {
				last := &merged[len(merged)-1]
				lastWords := last.endIdx - last.startIdx
				if lastWords < minWords {
					merged[len(merged)-2].endIdx = last.endIdx
					merged = merged[:len(merged)-1]
				}
			}
			subs = merged
		}

		// Sub-Fragmente als neue Fragments schreiben
		for _, sf := range subs {
			stableID := "unknown"
			if ref, ok := speakerDisplay[sf.speaker]; ok {
				stableID = ref.String()
			}
			subText := strings.Join(words[sf.startIdx:sf.endIdx], " ")
			relStart := float64(sf.startIdx) / float64(len(words)) * frag.Duration
			relEnd := float64(sf.endIdx) / float64(len(words)) * frag.Duration
			newFrags = append(newFrags, RecordingFrag{
				Index:    frag.Index,
				Text:     subText,
				Speaker:  stableID,
				Start:    round2(frag.Start + relStart),
				End:      round2(frag.Start + relEnd),
				Duration: round2(relEnd - relStart),
				Status:   "done",
				Segments: []FragSpeakerSeg{{
					Speaker: stableID,
					Start:   0,
					End:     round2(relEnd - relStart),
					Text:    subText,
				}},
			})
		}
		splitCount++
	}

	// Neue Fragment-Liste ersetzen (Index neu vergeben)
	for i := range newFrags {
		newFrags[i].Index = i + 1
	}
	session.Fragments = newFrags
	session.mu.Unlock()

	log.Printf("recording: session/end-diarize: fertig in %v (%d Speaker, %d Fragmente, %d gesplittet)",
		time.Since(start), len(envelopeOrder), len(newFrags), splitCount)
}

// ── WebDAV-Upload ────────────────────────────────────────────

// webdavUploadRecording lädt das finale Transkript + komplette Aufnahme hoch.
// Fragment-Dateien wurden bereits während der Aufnahme hochgeladen.
// Liefert den relativen Pfad zurück (leer bei Fehler).
func (s *Server) webdavUploadRecording(session *RecordingSession, transcript string) string {
	log.Printf("recording: webdav: finale Upload (token=%d chars, passwd=%d chars, audio=%d bytes)",
		len(session.shareToken), len(session.sharePasswd), len(session.totalAudio))
	if session.shareToken == "" || session.sharePasswd == "" {
		log.Printf("recording: webdav: kein Share vorhanden (token=%v passwd=%v)",
			session.shareToken != "", session.sharePasswd != "")
		return ""
	}

	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)

	// Verzeichnisse sicherstellen (falls Session-Create-MKCOL fehlgeschlagen ist)
	dateDir := session.Created.Format("2006-01-02")
	for _, dir := range []string{dateDir, sessionDir} {
		if err := dav.shareMkdir(dir); err != nil {
			log.Printf("recording: webdav: MKCOL %s: %v", dir, err)
			return ""
		}
	}

	// Transkript-JSON (inkl. speaker_hints)
	speakerHints := buildSpeakerHints(session.Fragments)
	transcriptJSON, _ := json.MarshalIndent(map[string]any{
		"session_id":    session.ID,
		"created":       session.Created,
		"transcript":    transcript,
		"fragments":     session.Fragments,
		"speaker_hints": speakerHints,
		"audio_file":    "aufnahme.wav",
	}, "", "  ")
	if err := dav.sharePutFile(sessionDir+"/transkript.trs", string(transcriptJSON)); err != nil {
		log.Printf("recording: webdav: PUT transkript.trs: %v", err)
	}

	// Audio (komplettes Session-Audio)
	if len(session.totalAudio) > 0 {
		// sharePutFile akzeptiert string — für Binärdaten sharePutBinary nutzen
		if err := dav.sharePutBinary(sessionDir+"/aufnahme.wav", pcm16ToWAV(decodeAudioToPCM16(session.totalAudio))); err != nil {
			log.Printf("recording: webdav: PUT aufnahme.wav: %v", err)
		}
	} else {
		log.Printf("recording: webdav: kein totalAudio")
	}

	log.Printf("recording: webdav: Upload abgeschlossen nach %s", sessionDir)
	return sessionDir
}

// webdavSessionDir liefert den relativen Session-Pfad (ohne leading /).
func webdavSessionDir(session *RecordingSession) string {
	dateDir := session.Created.Format("2006-01-02")
	return fmt.Sprintf("%s/%s", dateDir, session.ID)
}

// webdavEnsureDirs legt die WebDAV-Verzeichnisse für eine Session an (MKCOL).
// Wird beim Session-Start aufgerufen, damit Fragment-Uploads sofort funktionieren.
func (s *Server) webdavEnsureDirs(session *RecordingSession) {
	if session.shareToken == "" || session.sharePasswd == "" {
		return
	}
	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)
	dateDir := session.Created.Format("2006-01-02")

	for _, dir := range []string{dateDir, sessionDir} {
		if err := dav.shareMkdir(dir); err != nil {
			log.Printf("recording: webdav: MKCOL %s: %v", dir, err)
			return
		}
	}
	log.Printf("recording: webdav: Verzeichnisse angelegt: %s", sessionDir)
}

// webdavUploadFragment lädt ein Fragment-Audio asynchron per WebDAV-PUT hoch.
// wird in einer Goroutine aufgerufen (fire-and-forget).
func (s *Server) webdavUploadFragment(session *RecordingSession, fragmentIdx int, fragAudio []byte) {
	samples := decodeAudioToPCM16(fragAudio)
	if samples == nil {
		log.Printf("recording: webdav: fragment %d: Audio-Dekodierung fehlgeschlagen", fragmentIdx)
		return
	}
	wavData := pcm16ToWAV(samples)
	filename := fmt.Sprintf("fragment_%03d.wav", fragmentIdx)

	dav := newShareWebDav(s, session.shareToken, session.sharePasswd)
	sessionDir := webdavSessionDir(session)
	if err := dav.sharePutBinary(sessionDir+"/"+filename, wavData); err != nil {
		log.Printf("recording: webdav: PUT %s: %v", filename, err)
	} else {
		log.Printf("recording: webdav: fragment %d hochgeladen (%d bytes)", fragmentIdx, len(wavData))
	}
}

// speakerHint beschreibt einen Sprecherwechsel im Transkript.
type speakerHint struct {
	Time   float64 `json:"time"`
	Event  string  `json:"event"`
	From   string  `json:"from"`
	To     string  `json:"to"`
}

// buildSpeakerHints erzeugt eine Liste von Speaker-Changes zwischen
// aufeinanderfolgenden Fragmenten. Zeitstempel = Startzeit des neuen Fragments.
func buildSpeakerHints(frags []RecordingFrag) []speakerHint {
	var hints []speakerHint
	for i := 1; i < len(frags); i++ {
		prev := frags[i-1]
		cur := frags[i]
		if prev.Speaker != "" && cur.Speaker != "" && prev.Speaker != cur.Speaker {
			hints = append(hints, speakerHint{
				Time:  cur.Start,
				Event: "speaker_change",
				From:  prev.Speaker,
				To:    cur.Speaker,
			})
		}
	}
	return hints
}

// ── Diarization ──────────────────────────────────────────────

func (s *Server) diarizeAudioBytes(audioData []byte) *diarizeResponse {
	url := strings.TrimRight(s.cfg.Recording.DiarizeAPIBase, "/") + "/diarize"

	// Audio → WAV (openannote erwartet Container, kein Raw-PCM)
	samples := decodeAudioToPCM16(audioData)
	if samples == nil {
		log.Printf("recording: diarize: Audio-Dekodierung fehlgeschlagen (%d bytes)", len(audioData))
		return nil
	}
	wavData := pcm16ToWAV(samples)

	var buf bytes.Buffer
	boundary := fmt.Sprintf("----TakiBoundary%d", time.Now().UnixNano())
	w := NewMultipartWriter(&buf, boundary)
	if s.cfg.Recording.DiarizeModel != "" {
		w.WriteField("model", s.cfg.Recording.DiarizeModel)
	}
	w.WriteFile("file", "chunk.wav", bytes.NewReader(wavData))
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

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		log.Printf("recording: diarize: HTTP %d: %s", resp.StatusCode, string(bodyBytes))
		return nil
	}

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

	// Stille am Anfang/Ende trimmen (Whisper erfindet bei Stille Floskeln wie "Vielen Dank")
	samples = trimSilence(samples)
	if len(samples) < 1600 { // < 100ms → zu kurz, überspringen
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

// trimSilence entfernt Stille (RMS < 0.008) von Anfang und Ende des Audio.
// Whisper erfindet bei Stille/Rauschen Floskeln ("Vielen Dank", "Thank you").
func trimSilence(samples []int16) []int16 {
	const threshold = 0.008 // etwas unter der VAD-Schwelle (0.01)
	const frameSize = 320   // 20ms bei 16kHz

	if len(samples) < frameSize*2 {
		return samples
	}

	// Erstes nicht-stilles Frame finden
	start := 0
	for start+frameSize <= len(samples) {
		frame := samples[start : start+frameSize]
		if rmsFromPCM16(frame) >= threshold {
			break
		}
		start += frameSize
	}

	// Letztes nicht-stilles Frame finden
	end := len(samples)
	for end-start > frameSize {
		fStart := end - frameSize
		if rmsFromPCM16(samples[fStart:end]) >= threshold {
			break
		}
		end -= frameSize
	}

	// 50ms Padding behalten (damit Konsonanten nicht abgeschnitten werden)
	padding := 800 // 50ms bei 16kHz
	if start > padding {
		start -= padding
	} else {
		start = 0
	}
	if end < len(samples)-padding {
		end += padding
	} else {
		end = len(samples)
	}

	if start >= end {
		return samples
	}
	return samples[start:end]
}

// ── Helpers ──────────────────────────────────────────────────

func randHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
	}
	return string(b)
}
