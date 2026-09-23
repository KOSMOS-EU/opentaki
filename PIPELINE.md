# Recording Pipeline — Begriffe, Funktionen, Befunde

Stand: 2026-09-23

## Begriffe

| Begriff | Definition | Code |
|---------|-----------|------|
| **Chunk** | 2s Audio-Paket vom Browser | `handleRecordingChunk` |
| **Fragment** | VAD-Einheit: Audio gesammelt bis Stille (800ms) oder Max-Dauer (30s). Ein Whisper-Call pro Fragment. | `session.fragAudio`, `addFragment` |
| **Fragment-Final** | Fragment fertig transkribiert + Word-Timestamps vorhanden. Bereit für Diarization. | nach `whisperTranscribeBytes` + `whisperTranscribeWithWords` |
| **Segment** | Speaker-homogener Abschnitt innerhalb eines Fragments. Bestimmt durch pyannote + Word-Timestamps. | `FragSpeakerSeg` |
| **Utterance** | Zusammenhängende Rede einer Person über Fragment-Grenzen. Lesesicht für UI. | `Utterance`, `buildUtterances` |
| **Speaker-Profil** | Ein Embedding (256-dim) in der DB, gehört zu einer Person. Mehrere Profile pro Person möglich. | `profiles` Tabelle |
| **Person** | Benannter oder automatischer Sprecher (SPEAKER_XX). Hat 1..N Profile. | `persons` Tabelle |
| **Session** | Eine Aufnahme-Sitzung. Start bis Stop. | `RecordingSession` |

## Funktionen (was sie tun, nicht was sie heißen sollten)

### Im Code

| Funktion | Was sie tut | Wann | Status |
|----------|------------|------|--------|
| `processAudioChunk` | VAD: sammelt Audio, erkennt Stille/Max-Dauer, entscheidet Fragment-Ende | Pro 2s-Chunk | AKTIV |
| `whisperTranscribeBytes` | Schnelle Transkription: Audio → Text (kein DTW) | Fragment-Final | AKTIV |
| `whisperTranscribeWithWords` | Langsame Transkription: Audio → Text + Word-Timestamps (DTW) | Fragment-Final, async | AKTIV |
| `diarizeWindowLive` | Rolling-Window-Diarization: pyannote auf 60s-Fenster von totalAudio | Nach Fragment-Final wenn >=60s seit letztem Window | **DEAKTIVIEREN** |
| `diarizeWindowFinal` | pyannote auf GESAMTEM totalAudio bei Session-End | Session-End, wenn unzugeordnete Fragmente | AKTIV, aber **problematisch** |
| `diarizeAudioBytes` | Low-Level: sendet Audio an openannote, gibt Segmente+Embeddings zurück | Von diarizeWindowLive/Final aufgerufen | AKTIV |
| `alignWindowLabel` | Mappt pyannote-Label auf stabile Person (DB-Match oder neu anlegen) | Pro Label in jedem diarize-Call | AKTIV |
| `finalizeSessionSpeakers` | Baut Multi-Segmente pro Fragment aus liveSegments + Word-Timestamps | Session-End | AKTIV |
| `correctSegmentBoundaries` | Verschiebt Speaker-Grenzen an Satzgrenzen (Interpunktion) | In finalizeSessionSpeakers | AKTIV |
| `buildUtterances` | Gruppiert aufeinanderfolgende Segmente desselben Speakers | Session-End | AKTIV |
| `flushPendingFragment` | Transkribiert offenes fragAudio bei Session-End | Session-End | AKTIV |

### Namens-Probleme

| Aktueller Name | Problem | Besserer Name |
|---------------|---------|---------------|
| `diarizeWindowLive` | Ist kein "Live" — feuert erst nach 60s, Labels instabil über Windows | `diarizeRollingWindow` (und deaktivieren) |
| `diarizeWindowFinal` | Heißt "Final" aber ist eigentlich "auf Gesamtaudio". Nicht besser als Rolling-Window, nur einmalig. | `diarizeFullAudio` |
| `finalizeSessionSpeakers` | Macht viel mehr als "finalize": Multi-Segment-Zuordnung, Word-TS-Matching, Corrector | `buildFragmentSegments` |
| `alignWindowLabel` | Ist nicht Window-spezifisch, allgemeines Label-zu-Person-Mapping | `mapLabelToPerson` |
| `liveSegments` | Nicht "live" — werden bei Session-End aus diarizeWindowFinal gefüllt | `diarizeSegments` |
| `liveSpeakerByFrag` | Nicht "live" — Midpoint-Zuordnung, oft falsch | ggf. entfernen |
| `liveSpeakerMap` | Nicht "live" — Label→Person Mapping | `labelToPersonMap` |

## Befunde aus Tests (21.-22.09.)

### Was funktioniert

1. **pyannote auf einzelnem Audio-Stück** erkennt Speaker korrekt
   - Direkter Test: 60s Audio, min_speakers=5 → 4 Speaker, korrekte Segmente
   - Die Segment-Grenzen sind brauchbar (±200ms)

2. **Word-Timestamps (DTW)** verbessern Speaker-Grenzen massiv
   - 10.6s präziser als i/N-Schätzung
   - RTX Blackwell: ~10-15s pro Fragment (akzeptabel)

3. **correctSegmentBoundaries** korrigiert Satzgrenzen
   - "Kommt das Test schon an?" wird korrekt dem richtigen Speaker zugeordnet
   - Vorwärts-Suche bis 12 Wörter für Satzende

4. **Fragment-Flush** löst das frags=0-Problem bei kurzen Aufnahmen

5. **Utterances** gruppieren korrekt über Fragment-Grenzen

### Was NICHT funktioniert

1. **Rolling-Window (diarizeWindowLive)**
   - pyannote clustert in jedem Window neu → Labels instabil
   - SPEAKER_01 in Window 1 ≠ SPEAKER_01 in Window 2
   - alignWindowLabel-Alignment über Windows ist fragil
   - **Ursache der Inkonsistenz**: 65s-Test → 3 Speaker, 256s-Test → 4+ Speaker im selben Bereich

2. **diarizeWindowFinal auf Gesamtaudio**
   - Erkennt WENIGER Speaker als pyannote auf Einzel-Fragmenten
   - 60s Audio: 4 Speaker. 256s Audio: 3 Speaker (pyannote konsolidiert)
   - Überschreibt die besseren Einzel-Ergebnisse

3. **Session-End überschreibt Fragment-Ergebnisse**
   - Fragmente die einzeln korrekt diarisiert waren, werden bei Session-End
     durch schlechtere Gesamtaudio-Ergebnisse ersetzt

### Schlussfolgerung

**Fragment-Level-Diarization** ist der richtige Ansatz:
- pyannote pro **fertigem Fragment** (30s Audio) aufrufen
- Word-Timestamps für präzise Grenzen innerhalb des Fragments
- Corrector für Satzgrenzen
- Speaker-Profile per Embedding in DB → Cross-Fragment-Zuordnung
- Kein Rolling-Window, kein Gesamtaudio-Call

Das entspricht dem Prinzip: **jedes Fragment ist eine eigenständige Einheit**.
Speaker-Zuordnung über Fragmente hinweg passiert über die Profile-DB,
nicht über einen zweiten pyannote-Call.

## Empfohlene Pipeline (Soll)

```
1. Chunk empfangen → VAD → Fragment-Buffer
2. Fragment-Final:
   a) whisperTranscribeBytes → Text (schnell, SSE)
   b) async whisperTranscribeWithWords → Word-Timestamps (DTW)
   c) diarizeFragment: pyannote auf DIESEM Fragment (30s)
      → Segmente + Embeddings
      → mapLabelToPerson: Embedding gegen DB matchen
      → Word-Timestamps für Wort-zu-Speaker-Zuordnung
      → correctSegmentBoundaries
      → Segmente + Speaker sofort verfügbar
      → SSE-Events für UI-Update
3. Session-End:
   a) flushPendingFragment
   b) wordWg.Wait (max 30s)
   c) buildUtterances (aus Fragment-Segmenten)
   d) LLM-Finalpass
   e) WebDAV-Upload
```

Kein Rolling-Window. Kein Gesamtaudio-Diarize.
Jedes Fragment wird einzeln diarisiert. Speaker-Konsistenz über die DB.
