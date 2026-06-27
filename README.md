# open_taki

Lightweight drop-in replacement for Apache Tika's text extraction.

Uses **pdftotext** for text-based PDFs, falls back to **LLM Vision OCR** (via microllm/vLLM) for scanned documents.

## Why?

Apache Tika is a ~500MB Java application that internally uses Tesseract (CPU OCR) for scanned PDFs. open_taki replaces this with:

- **~10MB Go binary** + poppler-utils (~20MB)
- **LLM-based OCR** via any OpenAI-compatible Vision API (qwen3.5, GPT-4V, etc.)
- **pdftotext fast-path** for text-based PDFs (no LLM needed)

## API

Tika-compatible endpoint:

```
PUT /tika/text
Content-Type: application/pdf

→ extracted text (plain or JSON)
```

## Config

```yaml
listen: ":9998"

llm:
  api_base: "http://localhost:8012/v1"  # microllm
  model: "local"                         # or "ocr" alias
  max_tokens: 4096

fallback:
  min_chars: 200
  min_chars_per_page: 50
```

## Build

```bash
go build -o open_taki .
# or
podman build -t open_taki .
```

## Usage with OpenCloud

Replace Tika in your compose:

```yaml
services:
  tika:
    image: open_taki:latest
    # same port, same API
```
