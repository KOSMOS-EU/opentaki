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

    # Multi-line splitting: if bbox is tall relative to page (>7%)
    # and has multi-line aspect ratio, split into sub-lines
    norm_height = ny1 - ny0
    aspect = box_height / max(0.01, box_width)
    words = text.split()
    if norm_height > 0.07 and aspect > 0.20 and len(words) >= 2:
        n_lines = 3 if norm_height > 0.13 else 2
        n_lines = min(n_lines, len(words))
        slice_h = (ny1 - ny0) / n_lines
        for i in range(n_lines):
            start = round(i * len(words) / n_lines)
            end = round((i + 1) * len(words) / n_lines)
            line_text = " ".join(words[start:end])
            if line_text:
                _draw_invisible_text(
                    page,
                    nx0, ny0 + i * slice_h,
                    nx1, ny0 + (i + 1) * slice_h,
                    line_text, page_width, page_height,
                )
        return

    fontsize = max(3.0, min(72.0, box_height / extent_em))

    natural_width = font.text_length(text, fontsize=fontsize)
    if natural_width <= 0:
        return

    target_width = max(1.0, box_width * 0.98)
    scale_x = target_width / natural_width

    baseline = fitz.Point(pdf_rect.x0, pdf_rect.y1 + descender * fontsize)
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

        # Get actual image dimensions for precise coordinate conversion
        if isinstance(page_data, dict):
            regions = page_data.get("regions", [])
            img_w = page_data.get("image_width", page_width * dpi / 72.0)
            img_h = page_data.get("image_height", page_height * dpi / 72.0)
        else:
            # Legacy format: page_data is a list of regions
            regions = page_data
            img_w = page_width * dpi / 72.0
            img_h = page_height * dpi / 72.0

        for region in regions:
            bbox = region.get("bbox_2d", region.get("bbox"))
            text = region.get("text", "")
            if not bbox or not text:
                continue

            # Normalize to 0-1 using actual image dimensions
            nx0 = bbox[0] / img_w
            ny0 = bbox[1] / img_h
            nx1 = bbox[2] / img_w
            ny1 = bbox[3] / img_h

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
