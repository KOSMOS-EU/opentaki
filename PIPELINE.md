# Recording Pipeline

## Begriffe

| Begriff | Definition |
|---------|-----------|
| **Chunk** | 2s Audio-Paket vom Browser |
| **Fragment** | Audio bis Sprechpause (≥800ms) oder Soft-/Hard-Limit. Ein Whisper-Call. |
| **Segment** | Speaker-homogener Abschnitt innerhalb eines Fragments. Von pyannote. |
| **Utterance** | Zusammenhängende Rede einer Person über Fragment-Grenzen. Lesesicht. |
| **Profil** | Ein Embedding (256-dim) in der DB. Eine Beobachtung einer Stimme. |
| **Person** | Gruppierung von Profilen. Auto-Name "Sprecher_N", manuell änderbar. |

## Fragmentierung (VAD)

```
0 - soft_limit (15s):  normale Stille-Erkennung (800ms, RMS < silence_thresh)
soft_limit - hard_limit:  kürzere Pausen reichen (soft_silence_ms, RMS < silence_thresh)
hard_limit (60s):  harter Schnitt (Log: längste Stille + min RMS im Fragment)
```

Alle Werte per Config einstellbar.

## Pipeline pro Fragment

```
1. CHUNK-EMPFANG
   Browser sendet 2s-Chunks
   → PCM-Decode → RMS → VAD → Fragment-Buffer
   → wenn Fragment fertig: weiter zu 2

2. TRANSKRIPTION
   a) whisperFast(fragAudio) → Text
      Schnell, nur Text, für sofortige SSE-Antwort an Browser
   b) SSE: partial/final/done Events an Browser

3. WORD-TIMESTAMPS (async)
   whisperDTW(fragAudio) → Word-Timestamps
   Langsam (~10s auf RTX), läuft parallel im Hintergrund
   Ergebnis: [{word, start, end}, ...] pro Fragment

4. DIARIZATION
   pyannote(fragAudio, min_speakers) → Segmente + Embeddings
   Pro Embedding:
     → neues Profil anlegen (immer)
     → neue Person anlegen (immer, innerhalb eines pyannote-Calls)
   pyannote-Labels (SPEAKER_XX) werden verworfen. Nur Embeddings zählen.

5. SEGMENTIERUNG
   Wenn Word-Timestamps vorhanden (3 fertig):
     → Wort-zu-Speaker-Zuordnung per echte Zeitstempel
   Sonst:
     → Fallback: proportional i/N
   → correctSegmentBoundaries (Satzgrenzen-Korrektur)
   → Segmente ins Fragment schreiben
   → SSE: speaker Event an Browser
```

Schritte 2-5 passieren pro fertigem Fragment. Der Browser bekommt sofort
nach Schritt 2 den Text, nach Schritt 5 die Speaker-Zuordnung.

## Pipeline Session-End

```
6. FLUSH
   Offenes fragAudio transkribieren (für Aufnahmen ohne finale Sprechpause)
   → gleiche Schritte 2-5 wie oben

7. UNZUGEORDNETE FRAGMENTE
   Fragmente ohne Speaker (z.B. Flush-Fragment):
   → Schritte 4-5 nachholen

8. UTTERANCES
   Alle Segmente aller Fragmente flach sammeln
   → aufeinanderfolgende gleichen Speakers gruppieren (Pause > 2s = neue Utterance)

9. TRANSKRIPT
   Text aus Utterances bauen (Speaker-zusammenhängend)

10. LLM-FINALPASS (optional, per Config)
    Textpolitur: Tippfehler, Satzzeichen, Füllwörter, Whisper-Halluzinationen

11. UPLOAD
    .trs JSON (fragments + utterances + transcript) + aufnahme.wav per WebDAV
```

## Profil-Zuordnung

**Innerhalb eines pyannote-Calls**:
- Jedes Embedding = neue Person + neues Profil
- Kein DB-Matching
- pyannote hat getrennt → bleibt getrennt

**Über Fragmente/Sessions hinweg** (TODO):
- Matching gegen bestehende Profile (Cosine > speaker_match)
- Passiert NICHT innerhalb desselben pyannote-Calls
- Passiert beim nächsten Fragment oder bei einer späteren Session

**Manuell**:
- Personen umbenennen ("Sprecher_0" → "Klaus Witt")
- Personen zusammenführen (zwei Profile → eine Person)

**Format**: `Sprecher_<PersonID>/<ProfilID>`
Wenn `SPEAKER_` in der Ausgabe → Bug.

## Config

```yaml
recording:
  # Fragmentierung
  silence_thresh: 0.03        # RMS-Schwelle für Stille
  silence_timeout_ms: 800     # Stille-Dauer für Fragment-Ende
  soft_limit_sec: 15          # Ab hier kürzere Pausen akzeptieren
  soft_silence_ms: 200        # Pausen-Threshold ab Soft-Limit
  max_fragment_sec: 60        # Hard-Cut (Notfall)

  # Diarization
  diarize_api_base: "..."     # openannote URL (via microllm)
  diarize_model: "pyannote/speaker-diarization-3.1"
  min_speakers: 5             # Hint an pyannote

  # Speaker-DB
  speaker_store: "/data/..."  # SQLite-Pfad
  speaker_match: 0.65         # Cosine-Threshold für Wiedererkennung

  # Optional
  llm_finalpass: true         # LLM-Textpolitur ein/aus
```
