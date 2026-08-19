#!/usr/bin/env python3
"""Embed invisible OCR text layer into existing PDF pages.

Usage:
    taki-embed-ocr.py input.pdf output.pdf layer.json

Canonical layer format (taki-ocr-layer/1):
{
  "format": "taki-ocr-layer/1",
  "engine": "opentaki/grounded-vlm",
  "created": "2026-08-19T12:00:00Z",
  "source_sha256": "<sha256 of the original PDF>",
  "page_count": 12,
  "pages": [
    {
      "index": 0,                    # 0-based
      "size": [595.28, 841.89],      # visible page, points
      "regions": [
        {"bbox": [x0, y0, x1, y1], "text": "...", "conf": 92}
      ]
    }
  ]
}

bbox coordinates are 0-1 floats relative to the visible (rotation-
applied) page. Legacy layers (dict pages, 1-based keys, bbox in the
VLM's 0..1000 convention) are still accepted: any coordinate above
1.5 must be a scaled value and is divided by 1000.

Also writes an additive XMP marker (taki namespace, previous taki
block replaced) so the layered document carries its origin:
engine, layer version, source sha256, page count, created.

Text is embedded as invisible (render_mode=3). Original page content
is preserved without re-rasterization.
"""

import json
import re
import shutil
import sys
from xml.sax.saxutils import escape

import pymupdf as fitz

TAKI_NS = "https://github.com/kosmos-eu/opentaki/ocr-layer#"

TAKI_BLOCK_RE = re.compile(
    r'<rdf:Description[^>]*xmlns:taki="[^"]*"[^>]*>.*?</rdf:Description>',
    re.DOTALL,
)


def _draw_invisible_text(page, nx0, ny0, nx1, ny1, text, page_width, page_height):
    """Embed invisible text spanning the bbox. Coordinates are normalized (0-1)."""
    text = (text or "").strip()
    if not text:
        return

    pdf_rect = fitz.Rect(
        nx0 * page_width,
        ny0 * page_height,
        nx1 * page_width,
        ny1 * page_height,
    )

    box_width = pdf_rect.width
    box_height = pdf_rect.height
    if box_width <= 0 or box_height <= 0:
        return

    font = fitz.Font("helv")
    ascender = getattr(font, "ascender", 1.075)
    descender = getattr(font, "descender", -0.299)
    extent_em = max(0.01, ascender - descender)

    # Primary: size font so natural text width matches box width.
    # This gives correct selection width — the key constraint.
    natural_at_1pt = font.text_length(text, fontsize=1.0)
    if natural_at_1pt <= 0:
        return
    fontsize_by_width = box_width / natural_at_1pt

    # Secondary: cap fontsize by box height so text doesn't bleed vertically.
    fontsize_by_height = box_height / extent_em
    fontsize = max(3.0, min(fontsize_by_width, fontsize_by_height))

    # With width-fitted fontsize, scale_x should be ~1.0 (no stretching needed).
    natural_width = font.text_length(text, fontsize=fontsize)
    if natural_width <= 0:
        return
    scale_x = (box_width * 0.98) / natural_width
    # Clamp: limit stretch to avoid selection overflow on short text
    scale_x = min(scale_x, 1.8)

    # Top-align: place glyph tops at bbox top (y0), not bottoms at bbox bottom
    baseline = fitz.Point(pdf_rect.x0, pdf_rect.y0 + ascender * fontsize)
    morph = (baseline, fitz.Matrix(scale_x, 1.0))

    page.insert_text(
        baseline,
        text,
        fontsize=fontsize,
        fontname="helv",
        render_mode=3,
        color=(0, 0, 0),
        morph=morph,
    )


def _normalize_regions(regions):
    """Yield (nx0, ny0, nx1, ny1, text) clamped to 0-1 coordinates."""
    out = []
    for region in regions:
        bbox = region.get("bbox") or region.get("bbox_2d") or []
        text = region.get("text", "")
        if len(bbox) != 4 or not text:
            continue
        # Legacy heuristic: 0-1 floats never exceed 1.5; anything larger
        # is the VLM's 0..1000 convention (or pixel values of a <=1500px
        # render) and is scaled to 0-1 the same way.
        if max(abs(float(v)) for v in bbox) > 1.5:
            bbox = [float(v) / 1000.0 for v in bbox]
        vals = [max(0.0, min(1.0, float(v))) for v in bbox]
        out.append((vals[0], vals[1], vals[2], vals[3], text))
    return out


def _iter_pages(layer):
    """Yield (page_index_0based, regions) for canonical and legacy layers."""
    pages = layer.get("pages")
    if isinstance(pages, list):
        for entry in pages:
            yield int(entry.get("index", 0)), entry.get("regions", [])
    elif isinstance(pages, dict):
        for key, entry in pages.items():
            try:
                idx = int(key) - 1
            except ValueError:
                continue
            if isinstance(entry, dict):
                yield idx, entry.get("regions", [])
            else:
                yield idx, entry


def _write_xmp_marker(doc, layer):
    """Additive XMP marker: replace any previous taki block, keep the rest."""
    xmp = (doc.get_xml_metadata() or "").strip()

    block = (
        '<rdf:Description xmlns:taki="%s">'
        "<taki:engine>%s</taki:engine>"
        "<taki:layerVersion>%s</taki:layerVersion>"
        "<taki:srcSha256>%s</taki:srcSha256>"
        "<taki:pageCount>%d</taki:pageCount>"
        "<taki:created>%s</taki:created>"
        "</rdf:Description>"
    ) % (
        TAKI_NS,
        escape(str(layer.get("engine", "unknown"))),
        escape(str(layer.get("format", "unknown"))),
        escape(str(layer.get("source_sha256", ""))),
        int(layer.get("page_count") or 0),
        escape(str(layer.get("created", ""))),
    )

    if xmp:
        xmp = TAKI_BLOCK_RE.sub("", xmp).strip()
        if "</rdf:RDF>" not in xmp:
            # Malformed XMP we cannot reason about — leave it untouched.
            print("warning: existing XMP has no rdf:RDF, marker not written",
                  file=sys.stderr)
            return
        xmp = re.sub(r"\s*</rdf:RDF>", "  %s\n</rdf:RDF>" % block, xmp, count=1)
    else:
        xmp = (
            '<?xpacket begin="\xef\xbb\xbf" id="W5M0MpCehiHzreSzNTczkc9d"?>'
            '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            "  " + block + "\n"
            "</rdf:RDF>"
            "</x:xmpmeta>"
            '<?xpacket end="w"?>'
        )

    doc.set_xml_metadata(xmp)


def embed_ocr(input_path: str, output_path: str, layer_path: str) -> None:
    with open(layer_path) as f:
        layer = json.load(f)

    if not list(_iter_pages(layer)):
        shutil.copy2(input_path, output_path)
        return

    doc = fitz.open(input_path)

    for page_idx, regions in _iter_pages(layer):
        if page_idx < 0 or page_idx >= len(doc):
            continue
        page = doc[page_idx]
        page_width = page.rect.width
        page_height = page.rect.height
        for nx0, ny0, nx1, ny1, text in _normalize_regions(regions):
            _draw_invisible_text(page, nx0, ny0, nx1, ny1, text,
                                 page_width, page_height)

    _write_xmp_marker(doc, layer)

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} input.pdf output.pdf layer.json",
              file=sys.stderr)
        sys.exit(1)

    embed_ocr(sys.argv[1], sys.argv[2], sys.argv[3])
