#!/usr/bin/env python3
"""Embed invisible OCR text layer into existing PDF pages.

Usage:
    taki-embed-ocr.py input.pdf output.pdf regions.json

regions.json format:
{
  "dpi": 150,
  "pages": {
    "3": [{"bbox_2d": [10, 20, 300, 50], "text": "Hello World"}, ...],
    "5": [{"bbox_2d": [10, 20, 300, 50], "text": "Other text"}, ...]
  }
}

Page numbers are 1-based. bbox_2d coordinates are pixels at the given DPI.
Text is embedded as invisible (render_mode=3) — searchable and selectable
but not visible. Original page content is preserved without re-rasterization.
"""

import json
import sys

import pymupdf as fitz


def embed_ocr(input_path: str, output_path: str, regions_path: str) -> None:
    with open(regions_path) as f:
        data = json.load(f)

    dpi = data.get("dpi", 150)
    pages_data = data.get("pages", {})

    if not pages_data:
        # Nothing to do — copy input to output
        import shutil
        shutil.copy2(input_path, output_path)
        return

    doc = fitz.open(input_path)

    for page_str, regions in pages_data.items():
        page_num = int(page_str) - 1  # fitz uses 0-based
        if page_num < 0 or page_num >= len(doc):
            continue

        page = doc[page_num]
        page_width = page.rect.width    # in points (72 dpi)
        page_height = page.rect.height

        # Conversion factor: pixel coords at render DPI → PDF points
        scale = 72.0 / dpi

        for region in regions:
            bbox = region.get("bbox_2d", region.get("bbox"))
            text = region.get("text", "")
            if not bbox or not text:
                continue

            # Convert pixel coords to PDF points
            x0 = bbox[0] * scale
            y0 = bbox[1] * scale
            x1 = bbox[2] * scale
            y1 = bbox[3] * scale

            # Clamp to page bounds
            x0 = max(0, min(x0, page_width))
            y0 = max(0, min(y0, page_height))
            x1 = max(0, min(x1, page_width))
            y1 = max(0, min(y1, page_height))

            box_width = x1 - x0
            box_height = y1 - y0
            if box_width < 1 or box_height < 1:
                continue

            # Font metrics for Helvetica
            font = fitz.Font("helv")
            ascender = font.ascender    # ~0.905
            descender = font.descender  # ~-0.299
            extent = ascender - descender  # ~1.204

            # Fontsize based on box height
            fontsize = max(3.0, min(72.0, box_height / extent))

            # Measure natural text width at this fontsize
            natural_width = font.text_length(text, fontsize=fontsize)
            if natural_width < 0.1:
                continue

            # Horizontal scale to fill the box width
            target_width = max(1.0, box_width * 0.98)
            scale_x = target_width / natural_width

            # Baseline: bottom of box, adjusted for descender
            baseline = fitz.Point(x0, y1 + descender * fontsize)

            # Transformation matrix for horizontal scaling
            morph = (baseline, fitz.Matrix(scale_x, 1.0))

            page.insert_text(
                baseline,
                text,
                fontsize=fontsize,
                fontname="helv",
                render_mode=3,  # invisible
                color=(0, 0, 0),
                morph=morph,
            )

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} input.pdf output.pdf regions.json",
              file=sys.stderr)
        sys.exit(1)

    embed_ocr(sys.argv[1], sys.argv[2], sys.argv[3])
