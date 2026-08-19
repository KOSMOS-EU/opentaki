#!/usr/bin/env python3
"""Structural PDF check before OCR layer remount.

Usage:
    taki-pdf-check.py input.pdf

Outputs JSON on stdout:
{
  "ok": true,
  "needs_pass": false,
  "signed": false,
  "pdfa_part": 0,
  "page_count": 12,
  "pages": [[595.28, 841.89], ...]
}

pages[i] is the visible (rotation-applied) size of page i in points.
Always exits 0 on a readable PDF (also for password-protected ones);
the Go side decides whether the PDF may be remounted.
"""

import json
import re
import sys

import pymupdf as fitz


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} input.pdf", file=sys.stderr)
        return 1

    result = {
        "ok": True,
        "needs_pass": False,
        "signed": False,
        "pdfa_part": 0,
        "page_count": 0,
        "pages": [],
    }

    try:
        doc = fitz.open(sys.argv[1])
    except Exception as e:
        result["ok"] = False
        print(json.dumps(result))
        print(f"open failed: {e}", file=sys.stderr)
        return 0

    try:
        if doc.needs_pass:
            result["needs_pass"] = True
            print(json.dumps(result))
            return 0

        result["page_count"] = doc.page_count
        for page in doc:
            r = page.rect  # rotation-aware visible rect
            result["pages"].append([r.width, r.height])

        # Signed: signature widgets (primary), then their xref objects,
        # then the AcroForm field list.
        for page in doc:
            for annot in page.annots() or []:
                signed = False
                try:
                    signed = annot.field_type == fitz.PDF_FIELD_TYPE_SIGNATURE
                except Exception:
                    signed = False
                if not signed:
                    try:
                        obj = doc.xref_object(annot.xref)
                        signed = "/Sig" in obj and "/FT" in obj
                    except Exception:
                        signed = False
                if signed:
                    result["signed"] = True
                    break
            if result["signed"]:
                break

        if not result["signed"]:
            try:
                for field in doc.fields() or []:
                    if field.get("ft") == "Sig":
                        result["signed"] = True
                        break
            except Exception:
                pass

        # PDF/A part from XMP (element or attribute form)
        try:
            xmp = doc.get_xml_metadata() or ""
            m = re.search(r"<pdfaid:part>(\d+)</pdfaid:part>", xmp)
            if not m:
                m = re.search(r'pdfaid:part="(\d+)"', xmp)
            if m:
                result["pdfa_part"] = int(m.group(1))
        except Exception:
            pass
    finally:
        doc.close()

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
