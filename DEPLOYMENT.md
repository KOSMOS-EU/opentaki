# open_taki — Deployment Guide

## Drop-in Replacement for Apache Tika

open_taki replaces Apache Tika in OpenCloud deployments. Same port (9998), same API (`/rmeta/text`), same service name (`tika`) — just swap the container image.

### What changes

```
BEFORE:  OpenCloud → Apache Tika (Java, Tesseract OCR, ~1 GB)
AFTER:   OpenCloud → open_taki (Go, LLM Vision OCR, 268 MB)
                       ├→ Collabora (office conversion, already in stack)
                       └→ vLLM/microllm (LLM for OCR + analysis)
```

### Why

| | Apache Tika | open_taki + Collabora |
|---|---|---|
| Container | ~1 GB Java | 268 MB Go |
| RAM | ~1 GB RSS | ~20 MB |
| PDF OCR | Tesseract (CPU) | LLM Vision (qwen3.5) |
| Images | EXIF metadata only | Content description |
| Audio | ID3 tags only | Whisper transcription |
| Office | Built-in Java parsers | Collabora (same LibreOffice) |
| Enrichment | None | Entities, summary, embedding |

---

## Quick Start

### 1. Replace Tika image in your compose

Edit `tika.yml` in your OpenCloud compose directory:

```yaml
# tika.yml — BEFORE
services:
  tika:
    image: apache/tika:latest-full
    networks:
      opencloud-net:
    restart: always

  opencloud:
    environment:
      SEARCH_EXTRACTOR_TYPE: tika
      SEARCH_EXTRACTOR_TIKA_TIKA_URL: http://tika:9998
      FRONTEND_FULL_TEXT_SEARCH_ENABLED: "true"
```

```yaml
# tika.yml — AFTER
services:
  tika:
    image: codeberg.org/kosmos-eu/open_taki:latest
    networks:
      opencloud-net:
    restart: always
    volumes:
      - ./taki-config.yaml:/etc/open_taki/config.yaml:ro
    logging:
      driver: ${LOG_DRIVER:-local}

  opencloud:
    environment:
      SEARCH_EXTRACTOR_TYPE: tika
      SEARCH_EXTRACTOR_TIKA_TIKA_URL: http://tika:9998
      FRONTEND_FULL_TEXT_SEARCH_ENABLED: "true"
```

Note: The service name stays `tika`, the URL stays `http://tika:9998`. OpenCloud doesn't know the difference.

### 2. Create taki-config.yaml

```yaml
# taki-config.yaml
listen: ":9998"

# LLM Backend — for PDF OCR, image description, meta extraction
# Point to any OpenAI-compatible API (vLLM, microllm, ollama, etc.)
llm:
  api_base: "http://your-llm:8012/v1"
  model: "local"
  max_tokens: 4096

# Collabora — office document conversion
# Uses the Collabora instance already in your OpenCloud stack.
# Handles: DOC, XLS, PPT, ODT, ODS, ODP and all LibreOffice formats.
# If not set, only pandoc-supported formats work (DOCX, ODT, EPUB, RTF).
collabora:
  url: "http://collabora:9980"

# Whisper — audio/video transcription (optional)
# Point to any OpenAI-compatible Whisper API.
whisper:
  api_base: ""  # e.g. "http://your-whisper:8019/v1"
  model: "whisper-large-v3"

# Embedding — for vector search (optional, used with v2 protocol)
embedding:
  api_base: ""  # e.g. "http://your-embed:8019/v1"
  model: "nomic-ai/nomic-embed-text-v1.5"

# PDF rendering settings
pdf:
  dpi: 150
  max_pages: 20

# pdftotext fast-path: skip LLM if pdftotext extracts enough text
fallback:
  min_chars: 200
  min_chars_per_page: 50

# Content routing (active with v2 protocol)
# Controls what goes into bleve fulltext index vs. vector-only.
# Everything always gets embedded for vector search.
routing:
  vector: always
  bleve_rules:
    # Documents → bleve (keyword searchable)
    - match:
        mime: ["application/pdf", "application/msword", "application/vnd.openxmlformats-*"]
      features: [meta, entities, summary]
    # Spreadsheets → bleve
    - match:
        mime: ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml*"]
      features: [meta]
    # Email → bleve
    - match:
        mime: ["message/*"]
      features: [meta, entities]
    # Text → bleve (cheap)
    - match:
        mime: ["text/*"]
      features: []
    # Images, audio, video → vector only (no bleve bloat)
    # Project spaces → override: everything to bleve
    - match:
        space_type: ["project"]
        mime: ["*"]
      features: [meta, entities, summary]
```

### 3. Restart

```bash
cd /your/opencloud/compose
podman compose down tika opencloud
podman compose up -d tika opencloud
```

### 4. Verify

Check OpenCloud logs for:
```
open_taki detected (version: 0.7.0), using v2 protocol
```

Check taki health:
```bash
curl http://localhost:9998/
# → {"name":"open_taki","version":"0.7.0","features":{"office":true,"pdf":true,...}}
```

### 5. Reindex (optional)

```bash
podman exec opencloud-1 opencloud search index --all-spaces --insecure
```

---

## Architecture

### Minimal setup (PDF + text only)

```yaml
# Just LLM, no Collabora needed
llm:
  api_base: "http://your-llm:8012/v1"
```

Handles: PDF (pdftotext + LLM OCR), DOCX/ODT/EPUB/RTF (pandoc), HTML, email, archives, plain text.

### Recommended setup (full office support)

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
collabora:
  url: "http://collabora:9980"
```

Adds: DOC, XLS, PPT, and all other LibreOffice-supported formats via Collabora's `/cool/convert-to` API. Collabora is already part of most OpenCloud deployments.

### Full setup (with audio + vector)

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
collabora:
  url: "http://collabora:9980"
whisper:
  api_base: "http://your-whisper:8019/v1"
embedding:
  api_base: "http://your-embed:8019/v1"
```

Adds: Audio/video transcription via Whisper, vector embeddings for semantic search.

---

## Extraction Methods

| Content Type | Method | Requires |
|---|---|---|
| PDF (text-based) | pdftotext | poppler-utils (built-in) |
| PDF (scans) | LLM Vision OCR | llm.api_base |
| DOCX, ODT, EPUB, RTF | pandoc | pandoc (built-in) |
| DOC, XLS, PPT, ODS, ODP | Collabora convert-to | collabora.url |
| Images (JPEG, PNG, ...) | LLM Vision | llm.api_base |
| Audio (MP3, WAV, ...) | Whisper transcription | whisper.api_base |
| Video (MP4, ...) | ffmpeg + Whisper | whisper.api_base + ffmpeg |
| Email (EML) | Go net/mail | built-in |
| MSG (Outlook) | String extraction | built-in |
| HTML, XML | pandoc | pandoc (built-in) |
| Archives (ZIP, 7z, TAR) | Recursive extraction | built-in tools |
| Plain text, CSV, code | Passthrough | nothing |

---

## LLM Backend Options

open_taki works with any OpenAI-compatible chat/completions API:

| Backend | Setup | Notes |
|---|---|---|
| **microllm** | `api_base: http://microllm:8012/v1` | Routing proxy, recommended |
| **vLLM** | `api_base: http://vllm:8011/v1` | Direct GPU inference |
| **ollama** | `api_base: http://ollama:11434/v1` | Easy local setup |
| **OpenAI** | `api_base: https://api.openai.com/v1` | Cloud, needs API key |

For OCR, a vision-capable model is required (qwen3.5, GPT-4V, etc.).

---

## Protocol Versions

### v1 (default, Tika-compatible)

No special headers needed. Response is standard Tika JSON:
```json
[{"X-TIKA:content": "extracted text...", "Content-Type": "application/pdf"}]
```

Works with any Tika client. No code changes needed in OpenCloud.

### v2 (extended, auto-detected)

OpenCloud with kosmos edition auto-detects open_taki and sends:
```
X-Taki-Protocol: v2
X-Taki-Features: meta,entities,summary,embedding
X-Taki-Source-Ref: <space-id>/<file-path>
```

Response includes:
```json
[{
  "X-TIKA:content": "extracted text...",
  "X-TAKI:method": "pdftotext",
  "X-TAKI:meta": {"title": "...", "author": "...", "language": "de", "doc_type": "contract"},
  "X-TAKI:entities": [{"type": "person", "value": "Max Mustermann"}, ...],
  "X-TAKI:summary": "Mietvertrag zwischen...",
  "X-TAKI:embedding": [0.023, -0.118, ...],
  "X-TAKI:routing": {"content_target": "both", "meta_target": "bleve", "vector_target": "vector"}
}]
```
