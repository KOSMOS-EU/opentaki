#!/usr/bin/env python3
"""Extract vector drawing clusters from a PDF page and render them via pdftoppm.

Usage:
    python3 taki-vector-crop.py <pdf> <page> <render_dir> <dpi>

Prints JSON to stdout:
    [{"x0":48.2,"y0":120.5,"x1":348.2,"y1":300.8,"n_paths":12,"has_raster":false,"size_pt":30000.0,"path":"/tmp/.../vec-p012-0-001.png"}, ...]

Heuristics:
  - Collect all vector paths (drawings) with rect > 50x50 pt
  - Collect all raster images with bbox > 100x100 pt
  - Cluster: union-find, merge boxes within 20 pt gap
  - Filter: min 3 vector paths OR 1 raster, min 50x50 pt
  - Max 4 clusters/page (largest first)
  - Render each cluster via pdftoppm -x/-y/-W/-H crop (2pt padding)
"""
import json
import os
import subprocess
import sys
from collections import defaultdict

MIN_W = 20       # pt: min drawing size (icons are ~10-15pt, diagrams start at 20)
MIN_H = 20       # pt
MIN_PATHS = 3    # min vector paths per cluster (or 1 raster)
MAX_CLUSTERS = 4 # max per page
GAP = 15         # pt gap for merging (tighter: 20pt merged too much)


def main():
    if len(sys.argv) < 5:
        print("usage: taki-vector-crop.py <pdf> <page> <render_dir> <dpi>", file=sys.stderr)
        sys.exit(2)

    pdf_path = sys.argv[1]
    page_num = int(sys.argv[2])  # 1-based
    render_dir = sys.argv[3]
    dpi = int(sys.argv[4])

    try:
        import pymupdf
    except ImportError:
        print("PyMuPDF not available", file=sys.stderr)
        sys.exit(1)

    doc = pymupdf.open(pdf_path)
    if page_num < 1 or page_num > len(doc):
        print("[]")
        return

    page = doc[page_num - 1]

    # Collect vector drawings (fills, strokes, both)
    drawings = []
    for d in page.get_drawings():
        rect = d.get("rect")
        if not rect:
            continue
        w = rect.x1 - rect.x0
        h = rect.y1 - rect.y0
        if w >= MIN_W and h >= MIN_H:
            drawings.append({"x0": rect.x0, "y0": rect.y0, "x1": rect.x1, "y1": rect.y1})

    # Collect raster images with bbox
    rasters = []
    for img in page.get_image_info():
        bbox = img.get("bbox")
        if not bbox:
            continue
        x0, y0, x1, y1 = bbox
        w = x1 - x0
        h = y1 - y0
        if w >= 100 and h >= 100:
            rasters.append({"x0": x0, "y0": y0, "x1": x1, "y1": y1})

    all_boxes = list(drawings) + list(rasters)
    if not all_boxes:
        print("[]")
        return

    # Union-find: merge boxes within GAP
    parent = list(range(len(all_boxes)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(len(all_boxes)):
        for j in range(i + 1, len(all_boxes)):
            bi, bj = all_boxes[i], all_boxes[j]
            if (bi["x0"] <= bj["x1"] + GAP and bi["x1"] >= bj["x0"] - GAP and
                    bi["y0"] <= bj["y1"] + GAP and bi["y1"] >= bj["y0"] - GAP):
                union(i, j)

    # Build clusters from union-find groups
    groups = defaultdict(list)
    for i in range(len(all_boxes)):
        groups[find(i)].append(i)

    clusters = []
    for members in groups.values():
        cx0 = min(all_boxes[i]["x0"] for i in members)
        cy0 = min(all_boxes[i]["y0"] for i in members)
        cx1 = max(all_boxes[i]["x1"] for i in members)
        cy1 = max(all_boxes[i]["y1"] for i in members)
        cw, ch = cx1 - cx0, cy1 - cy0
        n_paths = sum(1 for i in members if i < len(drawings))
        n_raster = sum(1 for i in members if i >= len(drawings))
        if cw < MIN_W or ch < MIN_H:
            continue
        if n_paths < MIN_PATHS and n_raster < 1:
            continue
        clusters.append({
            "x0": round(cx0, 1), "y0": round(cy0, 1),
            "x1": round(cx1, 1), "y1": round(cy1, 1),
            "n_paths": n_paths, "has_raster": n_raster > 0,
            "size_pt": round(cw * ch, 1),
        })

    # Limit: largest first, max 4
    clusters.sort(key=lambda c: c["size_pt"], reverse=True)
    clusters = clusters[:MAX_CLUSTERS]

    if not clusters:
        print("[]")
        return

    # Render each cluster via pdftoppm crop
    os.makedirs(render_dir, exist_ok=True)
    results = []
    for idx, c in enumerate(clusters):
        cw_pt = c["x1"] - c["x0"]
        ch_pt = c["y1"] - c["y0"]
        px = max(0, int(c["x0"] * dpi / 72) - 2)
        py = max(0, int(c["y0"] * dpi / 72) - 2)
        pw = int(cw_pt * dpi / 72) + 4
        ph = int(ch_pt * dpi / 72) + 4

        prefix = os.path.join(render_dir, f"vec-p{page_num:03d}-{idx}")
        cmd = [
            "pdftoppm", "-png",
            "-r", str(dpi),
            "-f", str(page_num), "-l", str(page_num),
            "-x", str(px), "-y", str(py),
            "-W", str(pw), "-H", str(ph),
            pdf_path, prefix,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        except (subprocess.CalledProcessError, subprocess.TimeoutError, OSError) as e:
            print(f"pdftoppm crop failed: {e}", file=sys.stderr)
            continue

        # pdftoppm names output <prefix>-<NNN>.png
        for f in sorted(os.listdir(render_dir)):
            if f.startswith(f"vec-p{page_num:03d}-{idx}-") and f.endswith(".png"):
                c["path"] = os.path.join(render_dir, f)
                break
        if "path" not in c:
            continue
        results.append(c)

    print(json.dumps(results))


if __name__ == "__main__":
    main()
