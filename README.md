# open_taki

**Lightweight, LLM-powered drop-in replacement for Apache Tika.**

Part of the [kosmos](https://codeberg.org/kosmos-eu) open source initiative — building vendor-free, sovereign digital infrastructure for European municipalities and public institutions.

---

## Why open_taki?

European public institutions rely on document management systems for contracts, permits, citizen correspondence, and archival records. Fulltext search is essential — but the standard solution, Apache Tika, is a 1 GB Java monolith from 2007 that uses CPU-based OCR (Tesseract) and can't understand what's actually in a document.

open_taki replaces Tika with a modern, modular architecture:

- **6 MB Go binary** instead of 1 GB Java — runs on any hardware
- **LLM-powered OCR** instead of Tesseract — understands context, tables, handwriting
- **Document intelligence** — extracts entities (names, dates, amounts), generates summaries, classifies document types
- **Image understanding** — describes photo content, not just EXIF metadata
- **Audio transcription** — real speech-to-text via Whisper, not just ID3 tags
- **Office conversion via Collabora** — uses the open source LibreOffice instance already in your stack
- **No vendor lock-in** — works with any OpenAI-compatible LLM (local or cloud)
- **Fully open source** — AGPL, developed in the EU, for the EU

### Sovereignty matters

Every component in the open_taki stack can run on-premise:

| Component | Purpose | Open Source |
|---|---|---|
| **open_taki** | Extraction & enrichment | [codeberg.org/kosmos-eu/open_taki](https://codeberg.org/kosmos-eu/open_taki) |
| **Collabora** | Office document conversion | [collaboraonline.com](https://www.collaboraonline.com/) |
| **vLLM** | LLM inference (OCR, analysis) | [github.com/vllm-project/vllm](https://github.com/vllm-project/vllm) |
| **Whisper** | Speech-to-text | [github.com/openai/whisper](https://github.com/openai/whisper) |
| **Qdrant** | Vector search | [qdrant.tech](https://qdrant.tech/) |
| **OpenCloud** | File sync & share | [opencloud.eu](https://opencloud.eu/) |

No data leaves your infrastructure. No API keys to US cloud providers required.

---

## What it replaces

```
BEFORE                              AFTER

┌─────────────────────┐             ┌─────────────────────┐
│ Apache Tika  (1 GB) │             │ open_taki  (268 MB) │
│                     │             │                     │
│  Java 21 + JVM      │             │  Go binary (6 MB)   │
│  Tesseract OCR      │             │  pdftotext + pandoc  │
│  70 unused parsers  │             │                     │
│  1 GB RAM at idle   │             │  20 MB RAM at idle   │
│                     │             │     │     │     │    │
│  PDF → text (basic) │             │     ▼     ▼     ▼   │
│  Images → EXIF only │             │   LLM  Colla- Whis- │
│  Audio → tags only  │             │   OCR  bora   per   │
│  Office → Java POI  │             │                     │
└─────────────────────┘             └─────────────────────┘
```

| Format | Apache Tika | open_taki |
|---|---|---|
| **PDF (text)** | pdfbox | pdftotext (fast-path) |
| **PDF (scans)** | Tesseract CPU OCR | **LLM Vision OCR** — understands layout, tables, context |
| **Images** | EXIF metadata only | **LLM Vision** — describes content, reads text in photos |
| **Audio** | ID3 tags only | **Whisper** — full speech-to-text transcription |
| **Video** | Container metadata | **ffmpeg + Whisper** — audio extraction + transcription |
| **Office (DOC/XLS/PPT)** | Built-in Java parsers | **Collabora** — same LibreOffice, already in stack |
| **Office (DOCX/ODT)** | Built-in Java parsers | **pandoc** — built into container |
| **Email** | Java MIME parser | Go net/mail (built-in) |
| **Archives** | Java ZIP library | System tools + recursive extraction |
| **Enrichment** | None | **Entities, summaries, embeddings, routing** |

---

## How it works

```
File uploaded to OpenCloud
        │
        ▼
   open_taki receives document
        │
        ├── PDF ────── pdftotext ── good text? ── yes ── content
        │                              no
        │                              └── LLM Vision OCR ── content
        │
        ├── DOC/XLS/PPT ── Collabora /cool/convert-to ── content
        │
        ├── DOCX/ODT ───── pandoc ── content
        │
        ├── Image ──────── LLM Vision (describe) ── content
        │
        ├── Audio ──────── Whisper (transcribe) ── content
        │
        └── Archive ────── unzip → recursive extract ── content
                                    │
                                    ▼
                           LLM enrichment (v2 protocol)
                                    │
                                    ├── meta: title, author, language, type
                                    ├── entities: persons, orgs, locations
                                    ├── summary: 1-2 sentences
                                    └── embedding: 768-dim vector
                                    │
                                    ▼
                           Routing decision
                                    │
                                    ├── meta → always bleve (keyword search)
                                    ├── content → bleve or vector (configurable)
                                    └── embedding → vector DB (semantic search)
```

---

## Quick Start

### 1. Swap the container image

In your OpenCloud compose, change `tika.yml`:

```yaml
services:
  tika:
    image: codeberg.org/kosmos-eu/open_taki:latest
    volumes:
      - ./taki-config.yaml:/etc/open_taki/config.yaml:ro
    networks:
      opencloud-net:
    restart: always

  opencloud:
    environment:
      SEARCH_EXTRACTOR_TYPE: tika
      SEARCH_EXTRACTOR_TIKA_TIKA_URL: http://tika:9998
      FRONTEND_FULL_TEXT_SEARCH_ENABLED: "true"
```

Same service name, same port, same URL. OpenCloud doesn't know the difference.

### 2. Configure

```yaml
# taki-config.yaml
listen: ":9998"

llm:
  api_base: "http://your-llm:8012/v1"    # any OpenAI-compatible API
  model: "local"

collabora:
  url: "http://collabora:9980"            # your existing Collabora

whisper:
  api_base: ""                            # optional: Whisper API

embedding:
  api_base: ""                            # optional: for vector search
```

### 3. Restart and reindex

```bash
podman compose down tika opencloud
podman compose up -d tika opencloud

# Optional: rebuild search index
podman exec opencloud-1 opencloud search index --all-spaces --insecure
```

### 4. Verify

```bash
curl http://localhost:9998/
# → {"name":"open_taki","version":"0.7.0","features":{"office":true,"pdf":true,...}}
```

OpenCloud logs will show:
```
open_taki detected (version: 0.7.0), using v2 protocol
```

---

## Deployment Configurations

### Minimal — PDF + text only

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
```

No Collabora, no Whisper. Handles PDF, DOCX, ODT, HTML, email, text, archives.

### Recommended — with office support

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
collabora:
  url: "http://collabora:9980"
```

Adds DOC, XLS, PPT and all LibreOffice formats. Collabora is already in most OpenCloud stacks.

### Full — with audio + vector search

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

Adds audio/video transcription and vector embeddings for semantic search.

---

## LLM Backend

open_taki works with any OpenAI-compatible chat/completions API. For OCR, a vision-capable model is needed.

| Backend | Command | Notes |
|---|---|---|
| **vLLM + qwen3.5** | `vllm serve qwen3.5-122b` | Best quality, needs GPU |
| **ollama** | `ollama serve` | Easy setup, runs on CPU |
| **microllm** | routing proxy | Model aliasing, recommended |
| **OpenAI API** | cloud | Works but data leaves premises |

---

## Build

```bash
# Binary
go build -o open_taki .

# Container
podman build -t open_taki .

# Cross-compile
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o open_taki .
```

---

## Container Registry

```
codeberg.org/kosmos-eu/open_taki:latest    # 268 MB
codeberg.org/kosmos-eu/open_taki:0.7.0
```

---

## Part of kosmos

open_taki is developed as part of the **kosmos** initiative — open source tools for European digital sovereignty, built around the **OpenCloud kosmos edition**.

The [kosmos edition](https://codeberg.org/kosmos-eu/opencloud) extends OpenCloud with capabilities that European municipalities and public institutions need but upstream doesn't yet provide: intelligent document extraction, metadata search, immutable records, and AI-powered content analysis — all self-hosted, all open source.

### kosmos components

| Component | What it does | Repository |
|---|---|---|
| **OpenCloud kosmos** | File sync & share + metadata search, immutable records | [codeberg.org/kosmos-eu/opencloud](https://codeberg.org/kosmos-eu/opencloud) |
| **open_taki** | Intelligent document extraction (replaces Apache Tika) | [codeberg.org/kosmos-eu/open_taki](https://codeberg.org/kosmos-eu/open_taki) |
| **microllm** | LLM routing proxy (model aliasing, OCR integration) | [codeberg.org/kosmos-eu/microllm](https://codeberg.org/kosmos-eu/microllm) |

These integrate with established open source projects:

| Project | Role in stack |
|---|---|
| [OpenCloud](https://opencloud.eu) | Upstream file platform |
| [Collabora](https://collaboraonline.com) | Document editing + conversion (LibreOffice Online) |
| [vLLM](https://github.com/vllm-project/vllm) | GPU inference for LLMs |
| [Qdrant](https://qdrant.tech) | Vector search engine |
| [Whisper](https://github.com/openai/whisper) | Speech-to-text |

All components run on-premise. No data leaves your infrastructure.

---

## License

AGPL-3.0 — see [LICENSE](LICENSE)

## Links

- **Repository**: [codeberg.org/kosmos-eu/open_taki](https://codeberg.org/kosmos-eu/open_taki)
- **Deployment Guide**: [DEPLOYMENT.md](DEPLOYMENT.md)
- **Issues**: [codeberg.org/kosmos-eu/open_taki/issues](https://codeberg.org/kosmos-eu/open_taki/issues)
