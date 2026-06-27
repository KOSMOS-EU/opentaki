# open_taki

Lightweight drop-in replacement for Apache Tika's text extraction — with LLM superpowers.

## What it does

| Format | Tika (Java/Tesseract) | open_taki |
|---|---|---|
| **PDF (text)** | pdfbox (Java) | pdftotext (fast-path) |
| **PDF (scans)** | Tesseract OCR (CPU) | **LLM Vision OCR** — understands context, tables, layout |
| **Images** | EXIF metadata only | **LLM Vision** — describes content, extracts text from photos |
| **Audio** | ID3 tags only | **Whisper transcription** — full speech-to-text |
| **Video** | Container metadata | **ffmpeg + Whisper** — extracts and transcribes audio |
| **Office** | Java POI/OOXML | pandoc + libreoffice |
| **Email** | Java MIME parser | Go net/mail |
| **Archives** | Java ZIP/7z/RAR | unzip, 7z, unrar + recursive extraction |
| **HTML** | JSoup (Java) | pandoc |

## Why?

Apache Tika is a ~1GB Java application with 70+ parsers for 1400+ MIME types. Most deployments (e.g. OpenCloud fulltext search) use exactly one feature: PDF→text.

open_taki:
- **9 MB Go binary** + system tools (~50MB container vs ~1GB Tika)
- **LLM-powered OCR** instead of Tesseract — better results on scans
- **Image content recognition** — not just EXIF metadata
- **Audio transcription** — not just ID3 tags
- **1 GB less RAM** at runtime (Tika JVM uses ~1GB RSS)

## API

Tika-compatible:

```
PUT /tika/text
Content-Type: application/pdf   → extracted text
Content-Type: image/jpeg        → image description
Content-Type: audio/mpeg        → transcription
Content-Type: application/zip   → recursive extraction

Accept: application/json        → {"X-TIKA:content": "...", "X-TAKI:method": "llm_ocr"}
Accept: text/plain              → plain text (default)
```

## Config

```yaml
listen: ":9998"

llm:
  api_base: "http://localhost:8012/v1"  # microllm → qwen3.5
  model: "local"

whisper:
  api_base: "http://localhost:8019/v1"  # Whisper API
  model: "whisper-large-v3"

fallback:
  min_chars: 200          # pdftotext threshold
  min_chars_per_page: 50
```

## Build & Run

```bash
# Build
go build -o open_taki .

# Run
./open_taki config.yaml

# Test
curl -X PUT -H "Content-Type: application/pdf" --data-binary @document.pdf http://localhost:9998/tika/text
curl -X PUT -H "Content-Type: image/jpeg" --data-binary @photo.jpg http://localhost:9998/tika/text
curl -X PUT -H "Content-Type: audio/mpeg" --data-binary @recording.mp3 http://localhost:9998/tika/text

# Container
podman build -t open_taki .
podman run -p 9998:9998 -v ./config.yaml:/etc/open_taki/config.yaml open_taki
```

## OpenCloud Integration

Replace Tika in your compose:

```yaml
# tika.yml
services:
  tika:
    image: open_taki:latest
    volumes:
      - ./taki-config.yaml:/etc/open_taki/config.yaml
    # same port 9998, same API — drop-in replacement
```

## Architecture

```
Client → PUT /tika/text
           ↓
       open_taki (Go, 9MB)
           ↓
    ┌──────┼──────────┐
    ↓      ↓          ↓
pdftotext  microllm   Whisper API
(fast)     (LLM OCR)  (transcription)
           ↓
        vLLM qwen3.5
        (GPU Vision)
```
