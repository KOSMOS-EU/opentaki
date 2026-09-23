# Recording Pipeline (Ist-Zustand 2026-09-23)

## Modul-Übersicht

| Modul | Zweck | Status | Begründung |
|-------|-------|--------|------------|
| **VAD** | Fragment-Grenzen erkennen (Stille/Max-Dauer) | AKTIV | Grundlage: wann wird Whisper aufgerufen |
| **Whisper (schnell)** | Text pro Fragment (Live-SSE) | AKTIV | Kern: Transkription |
| **Whisper (DTW)** | Word-Timestamps pro Fragment | AKTIV | Präzise Speaker-Grenzen (10.6s besser als i/N) |
| **Rolling-Window-Diarize** | Live-Speaker während Aufnahme | **DEAKTIVIEREN** | Label-Instabilität über Windows: pyannote clustert in jedem Window neu, Labels sind nicht stabil. `diarizeWindowFinal` auf Gesamtaudio ist zuverlässiger. Sliding Window war gedacht um früher Speaker zu erkennen, aber die Inkonsistenz über Windows macht es unbrauchbar. |
| **diarizeWindowFinal** | Ein pyannote-Call auf Gesamtaudio bei Session-End | AKTIV | Stabil: ein Call, ein Clustering, konsistente Labels. Ersetzt Rolling-Window. |
| **alignWindowLabel** | pyannote-Labels auf stabile Speaker-Profile mappen | AKTIV | Braucht Überarbeitung: session-lokaler Check funktioniert, aber Cross-Window-Alignment ist fragil |
| **Word-Timestamp-Zuordnung** | Wörter per echten Timestamps Speakern zuordnen | AKTIV | Funktioniert gut: 10.6s präziser als i/N-Schätzung |
| **correctSegmentBoundaries** | Speaker-Grenzen an Satzgrenzen verschieben | AKTIV | Funktioniert: "an?" wird zum richtigen Speaker geschoben, Satzstruktur erkannt |
| **Utterances** | Aufeinanderfolgende Segmente desselben Speakers gruppieren | AKTIV | Lesesicht für UI/Transkript |
| **LLM-Finalpass** | Tippfehler, Halluzinationen, Satzzeichen | AKTIV | "Vielen Dank"-Filter funktioniert |
| **Fragment-Flush** | Offenes Audio bei Session-End transkribieren | AKTIV | Löst das frags=0-Problem bei kurzen Aufnahmen |
| **Mikro-Segment-Smoothing** | Kurze Spans zusammenfassen | ENTFERNT | Eliminierte Speaker-Segmente bevor der Corrector laufen konnte |
| **Session-End-Diarize** | Zweiter pyannote-Call auf Gesamtaudio | ENTFERNT | War redundant zu Rolling-Window + schlechter (weniger Speaker, überschrieb bessere Live-Ergebnisse) |
| **Session-interne Cosine-Fusion** | Labels innerhalb Session per Embedding mergen | ENTFERNT | Bug: fusionierte verschiedene Sprecher die pyannote korrekt getrennt hatte |

### Empfehlung: Rolling-Window deaktivieren

Das Rolling-Window (Phase 3) war gedacht um **live** Speaker zu erkennen.
Aber es verursacht das Inkonsistenz-Problem:
- pyannote clustert in jedem 60s-Window **neu**
- SPEAKER_01 in Window 1 ≠ SPEAKER_01 in Window 2
- `alignWindowLabel` versucht Alignment, aber Embeddings variieren
- Ergebnis: unterschiedliche Speaker-Anzahl je nach Testlänge

Stattdessen: **`diarizeWindowFinal` auf Gesamtaudio** (ein Call, stabil).
Word-Timestamps + Corrector liefern die präzisen Grenzen.

Live-Speaker-Events (SSE `type:"speaker"`) gehen damit verloren — die
kamen erst nach 60s ohnehin. Akzeptabler Trade-off: Speaker werden bei
Session-End zugewiesen statt live, dafür konsistent.

## Überblick

```
Browser/CLI → 2s-Chunks → Taki → VAD → Whisper → Diarization → Utterances → .trs
```

## Phase 1: Audio-Empfang + VAD

Für jeden 2s-Chunk vom Browser:

1. `handleRecordingChunk` empfängt multipart Audio (WebM/PCM)
2. `decodeAudioToPCM16` → PCM16 16kHz mono
3. `rmsFromPCM16` → RMS-Wert berechnen
4. `processAudioChunk` → VAD State-Machine:
   - Audio in `session.totalAudio` anhängen (kumuliert)
   - `session.totalSamples` hochzählen
   - Wenn RMS < 0.01 → `silent=true`
   - Wenn Stille >= 800ms (`silence_timeout_ms`) → Fragment fertig
   - Wenn Fragment-Dauer >= 30s (`max_fragment_sec`) → Fragment fertig
   - Wenn Sprache aktiv + Intervall erreicht → Partial-Transkription

**Ergebnis**: Fragment-Audio (`session.fragAudio`) wird gesammelt bis
Stille oder Max-Dauer erreicht.

## Phase 2: Fragment-Transkription

Wenn Fragment fertig:

1. `whisperTranscribeBytes(fragAudio)` — schneller Whisper-Call, nur Text
   - Stille trimmen (`trimSilence`, threshold 0.012)
   - PCM → WAV → multipart POST an microllm (`llm-stt`)
   - microllm routet zu einem der 3 RTX-Backends (8021/8022/8023)
   - Response: `{"text": "..."}`
2. SSE-Events an Browser: `partial`, `final`, `done`
3. `session.addFragment(idx, text, "unknown")` — Fragment gespeichert
4. **Async**: `whisperTranscribeWithWords(fragAudio, fragStartSec)` in Goroutine
   - `session.wordWg.Add(1)` → WaitGroup
   - Selber Whisper-Call aber mit `response_format=verbose_json` + `timestamp_granularities=word`
   - HF transformers Pipeline mit DTW (Dynamic Time Warping)
   - ~10-15s auf RTX Blackwell pro Fragment
   - Response: `{"text": "...", "words": [{"word": "...", "start": 0.52, "end": 0.88}, ...]}`
   - Word-Timestamps auf absolute Session-Zeit umgerechnet (`fragStartSec + word.Start`)
   - In `session.Fragments[i].Words` gespeichert
   - `session.wordWg.Done()`

**Ergebnis**: Fragment mit Text + (async) Word-Timestamps.

## Phase 3: Live-Window-Diarization

Nach jedem fertigen Fragment, wenn genug Audio seit letztem Window:

1. `diarizeWindowLive` prüft: `totalSamples - windowCursorSamples >= 60s`
2. Wenn ja: Sub-Window aus `session.totalAudio` extrahieren
   - Window: `[cursor - 10s overlap, cursor + 60s]`
   - PCM → WAV
3. `diarizeAudioBytes(windowAudio)`:
   - POST an openannote `/diarize` via microllm Service-Proxy (`/svc/steno-ml`)
   - Form-Fields: `file`, `model=pyannote/speaker-diarization-3.1`, `min_speakers=5`
   - Response: `{segments: [{speaker, start, end}], speakers: [...], speaker_embeddings: {label: [256-dim]}}`
4. Pro pyannote-Label: `alignWindowLabel(session, label, embedding)`
   - Stufe 1: Identity (Label schon in `session.liveSpeakerMap`?)
   - Stufe 2: Global Profile Match (`matchSpeaker` gegen Speaker-DB)
     - Prüfe ob gematchte Person schon session-lokal vergeben → wenn ja: neue Person
   - Sonst: `createGlobalSpeaker` (neues SPEAKER_XX Profil in DB)
   - Ergebnis: SpeakerRef (PersonName + PersonID + ProfileID)
   - Gespeichert in `session.liveSpeakerMap[label]` + `session.liveSpeakerEmb[label]`
5. Segmente auf absolute Session-Zeit umrechnen (`winStartSec + seg.Start`)
6. Rohe Segmente mit stabilen Speaker-Namen in `session.liveSegments` speichern
7. Fragment-Zuordnung per Midpoint:
   - Pro Fragment: `dominantSpeakerAt(absSegs, labelRefs, mid)`
   - Fallback: nächstes Segment wenn kein exakter Hit
   - Ergebnis in `session.liveSpeakerByFrag[fragIndex]`
8. Cursor vorrücken: `windowCursorSamples = winEndSamples`
9. SSE-Event `type:"speaker"` an Browser

**Ergebnis**: `session.liveSegments` (rohe pyannote-Segmente mit stabilen Namen),
`session.liveSpeakerByFrag` (Fragment → Speaker-Name).

### Bekanntes Problem: Label-Instabilität über Windows

pyannote clustert in **jedem Window neu**. SPEAKER_01 im Window [0-60]s ist
nicht derselbe wie SPEAKER_01 im Window [60-120]s. `alignWindowLabel` versucht
Alignment per Embedding-Cosine, aber:
- Stufe 2 (Global Match) kann verschiedene pyannote-Labels auf dieselbe
  DB-Person matchen wenn Embeddings ähnlich genug sind
- Stufe "session-lokal-Check" verhindert das innerhalb eines Windows,
  aber NICHT über Windows hinweg
- Ergebnis: inkonsistente Speaker-Zuordnung je nach Anzahl Windows

## Phase 4: Session-End

`handleRecordingSessionEnd` wird aufgerufen:

### 4a. Fragment-Flush

`flushPendingFragment`: Offenes `fragAudio` transkribieren (für kurze
Aufnahmen die nie silence_timeout/maxFragmentSec erreichen).
- Schnelle Transkription + synchrone Word-Timestamps

### 4b. Fallback: totalAudio als ein Fragment

Wenn nach Flush immer noch `frags=0` und Audio vorhanden:
- Gesamtes `session.totalAudio` als ein Fragment durch Whisper

### 4c. Word-Timestamp Wait

```go
select {
case <-wordsDone: // alle async DTW-Goroutines fertig
case <-time.After(30 * time.Second): // Timeout
}
```

### 4d. Finales Diarize-Window

`diarizeWindowFinal`: Wenn es unzugeordnete Fragmente gibt
(kein Live-Window gelaufen oder Fragmente jenseits des letzten Windows):
- pyannote auf **gesamtem** `session.totalAudio`
- `alignWindowLabel` pro Label
- Segmente + Fragment-Zuordnung wie in Phase 3

### 4e. Speaker-Finalisierung

`finalizeSessionSpeakers`:

1. Speaker-Profile in DB speichern (aus `liveSpeakerEmb`)
2. Fragmente: Speaker aus `liveSpeakerByFrag` setzen (falls noch "unknown")
3. **Multi-Segment-Zuordnung** pro Fragment:
   a. Rohe `session.liveSegments` die mit Fragment überlappen sammeln
   b. Overlap-Auflösung: Midpoint-Schnitt (beide Speaker behalten)
   c. Aufeinanderfolgende gleiche Speaker zusammenfassen
   d. **Wort-zu-Speaker-Zuordnung**:
      - Wenn Word-Timestamps vorhanden: echte Zeiten
      - Sonst: proportional i/N (Fallback)
   e. **`correctSegmentBoundaries`** (Intelligent Segment Corrector):
      - Speaker-Grenzen an Satzgrenzen verschieben
      - Vorwärts-Suche bis 12 Wörter für Satzende-Interpunktion
      - Kein Overwrite von Dritt-Speaker-Abschnitten
   f. Kontiguierte Wortgruppen mit gleichem Speaker → Segmente
4. Dominanter Speaker pro Fragment = längstes Segment

### 4f. Utterances

`buildUtterances`: Alle Segmente aller Fragmente flach sammeln,
aufeinanderfolgende gleichen Speakers zusammenfassen (Pause > 2s → neue Äußerung).

### 4g. Transkript

Text aus Utterances bauen (nicht aus Fragments).

### 4h. LLM-Finalpass

Prompt an LLM (microllm, `local-ocr`):
- Tippfehler, Satzzeichen, Grammatik korrigieren
- Füllwörter entfernen
- Whisper-Halluzinationen filtern ("Vielen Dank", "Thank you", etc.)

### 4i. WebDAV-Upload

`webdavUploadRecording`:
- `transkript.trs` (JSON mit fragments, utterances, speaker_hints)
- `aufnahme.wav` (komplettes Session-Audio als WAV)
- Fragment-WAVs wurden schon während der Aufnahme hochgeladen

### 4j. Response

JSON an Client mit: status, session_id, transcript, upload, fragments, utterances.

## Datenfluss

```
totalAudio (PCM16, kumuliert)
  ↓
fragAudio (VAD-Chunks, 2-30s)
  ↓
whisperTranscribeBytes → Fragment.Text
whisperTranscribeWithWords → Fragment.Words (async)
  ↓
diarizeWindowLive (60s-Window auf totalAudio)
  → session.liveSegments (rohe pyannote-Segmente, stabile Namen)
  → session.liveSpeakerByFrag (Fragment → Speaker)
  → session.liveSpeakerMap (Label → SpeakerRef)
  → session.liveSpeakerEmb (Label → Embedding)
  ↓
finalizeSessionSpeakers
  → Fragment.Segments (Multi-Speaker per Fragment)
  → correctSegmentBoundaries (Satzgrenzen)
  ↓
buildUtterances
  → Utterances (Speaker-zusammenhängend über Fragment-Grenzen)
  ↓
LLM-Finalpass → Transcript (poliert)
  ↓
.trs JSON (fragments + utterances + speaker_hints + transcript)
```

## Konfiguration (brandis.eu)

```yaml
recording:
  diarize_api_base: "http://microllm:8012/svc/steno-ml"
  diarize_model: "pyannote/speaker-diarization-3.1"
  min_speakers: 5
  speaker_store: "/data/taki-speakers/speakers.db"
  max_chunk_mb: 50
  live_diarize: true
  live_window_sec: 60
  live_overlap_sec: 10
whisper:
  api_base: "http://microllm:8012/v1"
  model: "llm-stt"
```

microllm `llm-stt` → nur RTX-Backends:
- RTX:8021 (GPU1, whisper-large-v3)
- RTX:8022 (GPU2, whisper-large-v3)
- RTX:8023 (GPU0, whisper-large-v3-turbo)

## Speaker-DB (SQLite)

Tabellen: `persons` (id, name), `profiles` (id, person_id, embedding, source, first_seen, last_seen), `meta`.
Jedes pyannote-Label bekommt ein eigenes Profil. Personen-Zuordnung nur durch
Cross-Session Embedding-Match oder manuelle UI-Zuordnung.
