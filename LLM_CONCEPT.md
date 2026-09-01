# open_taki: LLM-Modell-Konzept

## Ist-Zustand

Alle LLM-Calls gehen ueber einen microllm-Alias (`local-ocr`) an dasselbe
Modell: Qwen3.5-122B-A10B-int4 auf dgx/pgx. Das ist fuer Metadaten-Analyse
(DocMeta, Store-Detect) richtig, fuer OCR massiv overkill.

## Pipeline-Schritte und Modellanforderungen

| Schritt | Typ | Anforderung | Aktuell | Empfehlung |
|---------|-----|-------------|---------|------------|
| OCR (Seiten→Text) | Vision | Schnell, Text erkennen | Qwen3.5-122B (2s/Page bei 500tok/s, real: 40s/Page bei 12tok/s) | Qwen3-VL-8B oder Qwen2-VL-7B (10-20x schneller) |
| DocMeta (Briefkopf) | Vision + guided_json | Qualitaet, strukturierte Extraktion | Qwen3.5-122B | Qwen3.5-122B (richtig so) |
| DocMeta Rescue | Text + guided_json | Qualitaet, Fallback | Qwen3.5-122B | Qwen3.5-122B |
| Store-Detect (Aktenplan) | Text | Qualitaet, Klassifikation | Qwen3.5-122B | Qwen3.5-122B |
| Summary/Entities | Text | Mittel | Qwen3.5-122B | Kleineres Modell reicht (8-14B) |
| Embedding | Embed | Schnell | Qwen3-Embed-0.6B | Passt |

## Ziel: Modell-Routing pro Schritt

Verschiedene microllm-Aliase fuer verschiedene Aufgaben:

```yaml
# taki config.yaml
llm:
  api_base: http://microllm:8012/v1
  model: local-ocr            # DocMeta, Store-Detect (Qualitaet)

ocr:
  model: local-ocr-fast       # OCR Pages (Geschwindigkeit)

summary:
  model: local-summary         # Summary/Entities (optional, kleiner)
```

microllm-Aliase:

```
local-ocr       → dgx:8011 / pgx:8011 (Qwen3.5-122B, 4 Slots)
local-ocr-fast  → dgx:8012 / pgx:8012 (Qwen3-VL-8B, 16 Slots)
local-summary   → dgx:8011 / pgx:8011 (oder kleineres Modell)
local-embed     → dgx:8019 / pgx:8019 (Qwen3-Embed-0.6B)
```

## Performance-Erwartung

### OCR mit Qwen3-VL-8B

- ~100 tok/s pro Slot (statt 12 tok/s bei 122B)
- 500 Tokens pro Seite / 100 tok/s = **5s pro Seite**
- 16 Slots parallel: 16 Seiten alle 5s
- 20-seitiges Scan-PDF: 20/16 * 5s = **~7 Sekunden** (statt 43 Minuten)

### DocMeta bleibt bei 122B

- 1 Seite Vision (Briefkopf) + guided_json: ~30s
- Rescue-Pass (Text): ~15s
- Store-Detect: ~10s
- Gesamt pro Dokument: ~55s (akzeptabel, Qualitaet wichtiger)

### Gesamt pro Dokument (Ziel)

| Dokumenttyp | Aktuell | Ziel |
|-------------|---------|------|
| PDF (Text, 1 Seite) | 59s | 55s (kaum Aenderung, OCR nicht noetig) |
| PDF (Scan, 1 Seite) | 3min | 60s (OCR schneller, DocMeta gleich) |
| PDF (Scan, 20 Seiten) | 43min | 65s (OCR parallelisiert auf schnellem Modell) |
| Office (DOCX/XLSX) | 66s | 60s (kein OCR, Collabora + LLM) |
| E-Mail (MSG) | 75s | 65s (kein OCR) |

## Umsetzung

### 1. Neuen vLLM-Worker starten (dgx + pgx)

```bash
# ~/vllm-ocr-fast/start.sh
vllm serve Qwen/Qwen2-VL-7B-Instruct \
  --host 0.0.0.0 --port 8012 \
  --max-num-seqs 16 \
  --max-model-len 8192 \
  --trust-remote-code
```

### 2. microllm-Alias anlegen

```yaml
local-ocr-fast:
  backends:
    - api_base: http://10.30.254.81:8012
    - api_base: http://10.30.254.82:8012
```

### 3. Taki Config erweitern

```yaml
ocr:
  model: local-ocr-fast   # nur fuer llmOCR (Seiten-Extraktion)
```

### 4. Taki Code: llmOCR separat routen

In `main.go` bei `llmOCR()`: separaten API-Base / Model nutzen
wenn `cfg.OCR.Model` gesetzt ist. Fallback auf `cfg.LLM` wenn nicht.

## Kein MTP

Die vLLM-Worker laufen aktuell ohne Multi-Token-Prediction
(kein --num-speculative-tokens). MTP koennte die Geschwindigkeit
nochmal verdoppeln, ist aber nicht fuer alle Modelle verfuegbar
und erfordert ein passendes Draft-Modell.
