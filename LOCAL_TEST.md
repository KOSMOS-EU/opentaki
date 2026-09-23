# Lokales Test-Environment

## VAD-Tuning (kein Server nötig)

```bash
cd /data/source/gitapps/noetron-transkript

# Threshold-Sweep: welcher Wert erzeugt die besten Fragmente?
python3 vad_tune.py testdata/aufnahme_1322_multi.wav --sweep

# Einzelner Lauf mit bestimmten Parametern
python3 vad_tune.py testdata/aufnahme_1322_multi.wav --thresh 0.04 --soft-limit 15 --soft-silence 200 --hard-limit 60

# RMS-Profil: wo sind die leisen Stellen?
python3 vad_tune.py testdata/aufnahme_1322_multi.wav --profile
```

## open_taki lokal

```bash
cd /data/source/gitapps/open_taki

# Bauen
go build -o open_taki_local .

# Starten (nutzt ai.brandis.eu für Whisper + pyannote)
./open_taki_local -config config.local.yaml

# Testen (von anderem Terminal)
cd /data/source/gitapps/noetron-transkript
python3 test_record.py testdata/aufnahme_1322_multi.wav \
  --base http://localhost:9998 \
  --secret 3c3197484ecf1b4f18210c2a59be8a2034883818a3800412a9b439025433ec46
```

Config ändern → Taki neu starten → sofort testen. Kein Build, kein Deploy.

## Config-Werte in config.local.yaml

```yaml
recording:
  silence_thresh: 0.03    # RMS-Schwelle für Stille
  soft_limit_sec: 15      # Ab hier kürzere Pausen akzeptieren
  soft_silence_ms: 200    # Pausen-Threshold ab Soft-Limit
  max_fragment_sec: 60    # Hard-Cut (Notfall)
  min_speakers: 5         # pyannote Hint
```
