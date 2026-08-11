#!/usr/bin/env python3
"""Embed invisible OCR text layer into existing PDF pages.

Usage:
    taki-embed-ocr.py input.pdf output.pdf regions.json

regions.json format:
{
  "dpi": 150,
  "pages": {
    "3": {
      "image_width": 1237,
      "image_height": 1746,
      "regions": [{"bbox_2d": [10, 20, 300, 50], "text": "Hello World"}, ...]
    }
  }
}

Page numbers are 1-based. bbox_2d coordinates are pixels relative to the
rendered image dimensions. Text is embedded as invisible (render_mode=3).
Original page content is preserved without re-rasterization.
"""

import json
import sys

import pymupdf as fitz


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


def embed_ocr(input_path: str, output_path: str, regions_path: str) -> None:
    with open(regions_path) as f:
        data = json.load(f)

    dpi = data.get("dpi", 150)
    pages_data = data.get("pages", {})

    if not pages_data:
        import shutil
        shutil.copy2(input_path, output_path)
        return

    doc = fitz.open(input_path)

    for page_str, page_data in pages_data.items():
        page_num = int(page_str) - 1
        if page_num < 0 or page_num >= len(doc):
            continue

        page = doc[page_num]
        page_width = page.rect.width
        page_height = page.rect.height

        # Get regions from page data
        if isinstance(page_data, dict):
            regions = page_data.get("regions", [])
        else:
            regions = page_data

        for region in regions:
            bbox = region.get("bbox_2d", region.get("bbox"))
            text = region.get("text", "")
            if not bbox or not text:
                continue

            # Qwen-VL returns bbox_2d in [0, 999] range (normalized to 1000)
            nx0 = bbox[0] / 1000.0
            ny0 = bbox[1] / 1000.0
            nx1 = bbox[2] / 1000.0
            ny1 = bbox[3] / 1000.0

            # Clamp
            nx0 = max(0.0, min(1.0, nx0))
            ny0 = max(0.0, min(1.0, ny0))
            nx1 = max(0.0, min(1.0, nx1))
            ny1 = max(0.0, min(1.0, ny1))

            _draw_invisible_text(page, nx0, ny0, nx1, ny1, text,
                                 page_width, page_height)

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} input.pdf output.pdf regions.json",
              file=sys.stderr)
        sys.exit(1)

    embed_ocr(sys.argv[1], sys.argv[2], sys.argv[3])
