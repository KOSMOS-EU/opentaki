# open_taki

**Intelligent, LLM-powered drop-in replacement for Apache Tika.**

Part of the [kosmos](https://codeberg.org/kosmos-eu) open source initiative — building vendor-free, sovereign digital infrastructure for European municipalities and public institutions.

---

## Why open_taki?

European public institutions rely on document management systems for contracts, permits, citizen correspondence, and archival records. Fulltext search is essential — but the standard solution, Apache Tika, is a 1 GB Java monolith that uses CPU-based OCR (Tesseract) and can't understand what's actually in a document.

open_taki replaces Tika with a modern, modular architecture:

- **6 MB Go binary** instead of 1 GB Java — runs on any hardware
- **LLM-powered OCR** instead of Tesseract — understands context, tables, handwriting
- **Parallel processing** — all PDF pages OCR'd concurrently, 8 documents indexed simultaneously
- **Document intelligence** — extracts entities (names, dates, amounts), generates summaries, classifies document types
- **Image understanding** — describes photo content, not just EXIF metadata
- **Audio transcription** — real speech-to-text via Whisper, not just ID3 tags
- **Office conversion via Collabora** — uses the open source LibreOffice already in your stack, not a bundled copy
- **Configurable content routing** — meta always to keyword index, content selectively, embeddings to vector DB
- **No vendor lock-in** — works with any OpenAI-compatible LLM (local or cloud)
- **Fully open source** — developed in the EU, for the EU

### Sovereignty matters

Every component in the open_taki stack can run on-premise. No data leaves your infrastructure. No API keys to US cloud providers required.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ OpenCloud Compose Stack                                     │
│                                                             │
│  ┌──────────┐  PUT /rmeta/text   ┌───────────────────────┐  │
│  │OpenCloud │ ─────────────────│ open_taki  (268 MB)   │  │
│  │ (search) │  X-Taki-Protocol  │                       │  │
│  └──────────┘                    │  Go binary (6 MB)     │  │
│       │                          │  pdftotext + pandoc    │  │
│       ▼                          └──┬─────┬─────┬───────┘  │
│  ┌──────────┐                       │     │     │           │
│  │  Bleve   │ ◄── meta + content ───┘     │     │           │
│  │  Index   │  (selective routing)        │     │           │
│  └──────────┘                             │     │           │
│  ┌──────────┐ ◄── embedding + ref ────────┘     │           │
│  │  Qdrant  │  (semantic search)                │           │
│  └──────────┘                                   │           │
│                      ┌──────────────────────┐   │           │
│                      │ Collabora            │ ◄─┘           │
│                      │ DOC XLS PPT ODT ODS  │  office conv  │
│                      └──────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
        │ LLM + Embedding + Whisper (OpenAI-compatible APIs)
        ▼
┌──────────────────────────────────────┐
│ GPU Server                           │
│  microllm :8012 → vLLM qwen3.5      │
│  nomic-embed :8019                   │
│  whisper :8017 (optional)            │
└──────────────────────────────────────┘
```

---

## What it replaces

| Format | Apache Tika | open_taki |
|---|---|---|
| **PDF (text)** | pdfbox (Java) | pdftotext (fast-path) |
| **PDF (scans)** | Tesseract CPU OCR | **LLM Vision OCR** — parallel, all pages at once |
| **Images** | EXIF metadata only | **LLM Vision** — describes content, reads text |
| **Audio** | ID3 tags only | **Whisper** — full speech-to-text |
| **Video** | Container metadata | **ffmpeg + Whisper** — transcription |
| **Office** | Built-in Java (500 MB) | **Collabora** — already in your stack |
| **DOCX/ODT** | Built-in Java | **pandoc** — built into container |
| **Email** | Java MIME parser | Go net/mail (built-in) |
| **Archives** | Java ZIP library | System tools + recursive extraction |
| **Enrichment** | None | **Entities, summaries, embeddings, routing** |

### Performance

| | Apache Tika | open_taki |
|---|---|---|
| Container size | ~1 GB | 268 MB |
| RAM at idle | ~1 GB | ~20 MB |
| PDF OCR (10 pages) | ~100s (sequential Tesseract) | ~15s (parallel LLM Vision) |
| Reindex throughput | sequential | 8 parallel workers (configurable) |
| Scan quality | Often empty (Tesseract fails) | Context-aware (LLM understands layout) |

---

## Quick Start

### 1. Swap the container image

In your OpenCloud compose, edit `tika.yml`:

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

Same service name (`tika`), same port (`9998`), same URL. OpenCloud doesn't know the difference.

### 2. Configure

```yaml
# taki-config.yaml
listen: ":9998"

# Required: LLM backend (any OpenAI-compatible vision model)
llm:
  api_base: "http://your-llm:8012/v1"
  model: "local"
  max_tokens: 4096

# Recommended: Collabora for office formats (already in your stack)
collabora:
  url: "http://collabora:9980"

# Optional: audio/video transcription
whisper:
  api_base: ""

# Optional: vector embeddings for semantic search
embedding:
  api_base: ""
  model: "nomic-ai/nomic-embed-text-v1.5"

# PDF settings
pdf:
  dpi: 150
  max_pages: 20

# pdftotext fast-path: skip LLM if text extraction works
fallback:
  min_chars: 200
  min_chars_per_page: 50

# Content routing (active with v2 protocol)
routing:
  vector: always
  bleve_rules:
    - match:
        mime: ["application/pdf", "application/msword", "application/vnd.openxmlformats-*"]
      features: [meta, entities, summary]
    - match:
        mime: ["text/*"]
      features: []
```

### 3. Restart and reindex

```bash
podman compose down tika opencloud
podman compose up -d tika opencloud

# Rebuild search index
podman exec opencloud-1 opencloud search index --all-spaces --insecure
```

### 4. Verify

OpenCloud logs:
```
open_taki detected (version: 0.8.0), using v2 protocol
taki v2 detected: parallel indexing enabled (workers: 8)
```

---

## Deployment Configurations

### Minimal — PDF + text only

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
```

Handles PDF (with LLM OCR), DOCX, ODT, EPUB, RTF, HTML, email, text, archives.

### Recommended — with office + Collabora

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
collabora:
  url: "http://collabora:9980"
```

Adds DOC, XLS, PPT and all LibreOffice formats. Collabora is open source and already part of most OpenCloud stacks — no additional infrastructure needed.

### Full — with audio + vector search

```yaml
llm:
  api_base: "http://your-llm:8012/v1"
collabora:
  url: "http://collabora:9980"
whisper:
  api_base: "http://your-whisper:8017/v1"
embedding:
  api_base: "http://your-embed:8019/v1"
```

Adds audio/video transcription via Whisper and vector embeddings for semantic search.

---

## Protocol

### v1 — Tika compatible (default)

No special headers needed. Standard Tika JSON response:
```json
[{"X-TIKA:content": "extracted text...", "Content-Type": "application/pdf"}]
```

Works with any Tika client. No code changes needed in OpenCloud.

### v2 — Extended (auto-detected by OpenCloud kosmos edition)

OpenCloud sends `X-Taki-Protocol: v2` and receives:
```json
[{
  "X-TIKA:content": "extracted text...",
  "X-TAKI:method": "llm_ocr",
  "X-TAKI:meta": {"title": "...", "author": "...", "language": "de", "doc_type": "contract"},
  "X-TAKI:entities": [{"type": "person", "value": "Max Mustermann"}],
  "X-TAKI:summary": "Pachtvertrag zwischen Stadt Brandis und...",
  "X-TAKI:embedding": [0.023, -0.118, ...],
  "X-TAKI:routing": {"content_target": "both", "meta_target": "bleve", "vector_target": "vector"}
}]
```

OpenCloud uses routing to decide what goes to keyword index vs. vector DB. Configurable via `SEARCH_EXTRACTOR_TIKA_MAX_WORKERS` (default: 8 parallel workers).

---

## LLM Backend

Any OpenAI-compatible chat/completions API. For OCR, a vision-capable model is required.

| Backend | Setup | Notes |
|---|---|---|
| **microllm** | `api_base: http://microllm:8012/v1` | Routing proxy with model aliases, recommended |
| **vLLM + qwen3.5** | `api_base: http://vllm:8011/v1` | Best quality, needs GPU |
| **ollama** | `api_base: http://ollama:11434/v1` | Easy local setup |
| **OpenAI API** | `api_base: https://api.openai.com/v1` | Works, but data leaves premises |

---

## Build & Deploy

```bash
./build.sh          # Build Go binary + container image (date-tagged + latest)
./push.sh           # Push to codeberg.org/kosmos-eu/open_taki
./deploy.sh         # Pull + restart on target host (configured in DIST)
```

Container registry:
```
codeberg.org/kosmos-eu/open_taki:latest
codeberg.org/kosmos-eu/open_taki:20260628
```

---

## Part of kosmos

open_taki is developed as part of the **kosmos** initiative — open source tools for European digital sovereignty, built around the **OpenCloud kosmos edition**.

The [kosmos edition](https://codeberg.org/kosmos-eu/opencloud) extends OpenCloud with capabilities that European municipalities and public institutions need: intelligent document extraction, metadata search, immutable records, and AI-powered content analysis — all self-hosted, all open source.

| Component | What it does | Repository |
|---|---|---|
| **OpenCloud kosmos** | File sync & share + metadata search, immutable records | [codeberg.org/kosmos-eu/opencloud](https://codeberg.org/kosmos-eu/opencloud) |
| **open_taki** | Intelligent document extraction (replaces Apache Tika) | [codeberg.org/kosmos-eu/open_taki](https://codeberg.org/kosmos-eu/open_taki) |
| **microllm** | LLM routing proxy (model aliasing, OCR integration) | [codeberg.org/kosmos-eu/microllm](https://codeberg.org/kosmos-eu/microllm) |

Integrates with established open source projects:

| Project | Role in stack |
|---|---|
| [OpenCloud](https://opencloud.eu) | Upstream file platform |
| [Collabora](https://collaboraonline.com) | Document editing + office conversion |
| [vLLM](https://github.com/vllm-project/vllm) | GPU inference for LLMs |
| [Qdrant](https://qdrant.tech) | Vector search engine |
| [Whisper](https://github.com/openai/whisper) | Speech-to-text |

All components run on-premise. No data leaves your infrastructure.

---

## License

AGPL-3.0

## Links

- **Repository**: [codeberg.org/kosmos-eu/open_taki](https://codeberg.org/kosmos-eu/open_taki)
- **Deployment Guide**: [DEPLOYMENT.md](DEPLOYMENT.md)
- **Container Images**: [codeberg.org/kosmos-eu/-/packages](https://codeberg.org/kosmos-eu/-/packages)
- **Issues**: [codeberg.org/kosmos-eu/open_taki/issues](https://codeberg.org/kosmos-eu/open_taki/issues)
