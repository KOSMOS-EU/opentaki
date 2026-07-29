#!/usr/bin/env python3
"""Test document classification against OpenCloud documents via taki.

Run on cloud.brandis.eu (inside the Pod network via nsenter).
Usage: python3 taki-test.py [--drive NAME] [--limit N] [--folder PATH]
"""
import json, sys, subprocess, urllib.request, urllib.parse, base64, argparse

OPENCLOUD = "http://localhost:9200"
TIKA = "http://localhost:9998"
AUTH = base64.b64encode(b"admin:$Brandis2o25.").decode()

def api(method, url, body=None, ct=None):
    headers = {"Authorization": f"Basic {AUTH}"}
    if ct:
        headers["Content-Type"] = ct
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

def list_drives():
    return api("GET", f"{OPENCLOUD}/graph/v1.0/me/drives").get("value", [])

def list_children(drive_id, item_id=None):
    """List children of a drive folder. If item_id is None, lists root."""
    if item_id is None:
        item_id = drive_id  # root item has same ID as drive
    url = f"{OPENCLOUD}/graph/v1.0/drives/{drive_id}/items/{item_id}/children"
    return api("GET", url).get("value", [])

def find_files_recursive(drive_id, item_id=None, max_depth=3, depth=0, path=""):
    """Recursively find files in a drive, up to max_depth."""
    if depth > max_depth:
        return []
    children = list_children(drive_id, item_id)
    if isinstance(children, dict) and "error" in children:
        return []
    files = []
    for item in children:
        item_path = f"{path}/{item['name']}" if path else item['name']
        if "folder" in item and not item["name"].startswith("."):
            files.extend(find_files_recursive(drive_id, item["id"], max_depth, depth + 1, item_path))
        elif "file" in item:
            item["_path"] = item_path
            files.append(item)
    return files

def download_file(drive_id, item_id, webdav_url=None):
    # Download via WebDAV path (dav/spaces/...) — the Graph /content endpoint has encoding issues
    import subprocess
    if webdav_url:
        url = webdav_url.replace("https://cloud.brandis.eu", OPENCLOUD)
    else:
        return None, "no webDavUrl"
    result = subprocess.run(
        ["curl", "-s", "-L", "-o", "-",
         "-u", "admin:$Brandis2o25.", url],
        capture_output=True, timeout=60
    )
    if result.returncode != 0:
        return None, f"curl error: {result.returncode}"
    body = result.stdout
    if len(body) < 50 and (b"404" in body or b"error" in body.lower()):
        return None, f"error: {body.decode(errors='replace')[:100]}"
    if len(body) == 0:
        return None, "empty response"
    return body, None

def taki_extract(body, content_type):
    # Use curl to avoid urllib issues with large PUT bodies
    import subprocess
    result = subprocess.run(
        ["curl", "-s", "-X", "PUT",
         "-H", f"Content-Type: {content_type}",
         "-H", "X-Taki-Protocol: v2",
         "-H", "X-Taki-Features: docmeta",
         "--data-binary", "@-",
         f"{TIKA}/rmeta/text"],
        input=body, capture_output=True, timeout=300
    )
    if result.returncode != 0:
        return {"error": f"curl exit {result.returncode}"}
    try:
        parsed = json.loads(result.stdout)
        if isinstance(parsed, list) and len(parsed) > 0:
            return parsed[0]
        return parsed
    except Exception as e:
        return {"error": str(e), "raw": result.stdout[:200].decode(errors="replace")}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drive", default="Posteingang", help="Drive name")
    parser.add_argument("--limit", type=int, default=10, help="Max files to test")
    parser.add_argument("--depth", type=int, default=3, help="Max folder depth")
    parser.add_argument("--list-drives", action="store_true", help="Just list drives")
    parser.add_argument("--list-files", action="store_true", help="Just list files")
    args = parser.parse_args()

    drives = list_drives()
    if args.list_drives:
        for d in drives:
            print(f"{d['driveType']:10s} {d['name']:30s} {d['id']}")
        return

    # Find target drive
    drive = None
    for d in drives:
        if d["name"] == args.drive:
            drive = d
            break
    if not drive:
        print(f"ERROR: Drive '{args.drive}' not found")
        print("Available:", ", ".join(d["name"] for d in drives))
        return

    drive_id = drive["id"]
    # Get WebDAV base URL for file downloads
    webdav_base = drive.get("root", {}).get("webDavUrl", "")
    print(f"Drive: {drive['name']} ({drive_id})")
    print(f"WebDAV: {webdav_base}")

    print("Scanning folders (recursive)...")
    files = find_files_recursive(drive_id)
    print(f"Files found: {len(files)} (limit: {args.limit})\n")

    if args.list_files:
        for f in files:
            mime = f.get("file", {}).get("mimeType", "?")
            print(f"  {mime:45s} {f.get('size',0):>10d}  {f.get('_path', f['name'])}")
        return

    # Test each file
    results = []
    for f in files[:args.limit]:
        mime = f.get("file", {}).get("mimeType", "?")
        name = f["name"]
        size = f.get("size", 0)
        item_id = f["id"]

        # Skip large files
        if size > 50_000_000:
            print(f"  SKIP (too large: {size//1024//1024}MB)  {name}")
            continue

        print(f"  {name} ({mime}, {size} bytes)...", end="", flush=True)
        # Construct WebDAV URL from drive base + relative path
        file_path = f.get("_path", name)
        file_webdav = f"{webdav_base}/{urllib.parse.quote(file_path)}" if webdav_base else None
        body, _ct = download_file(drive_id, item_id, file_webdav)
        if body is None:
            print(f" DOWNLOAD ERROR: {_ct}")
            continue

        result = taki_extract(body, mime)
        docmeta = result.get("X-TAKI:docmeta")
        if docmeta:
            doc_type = docmeta.get("doc", {}).get("type", "?")
            doc_date = docmeta.get("doc", {}).get("date", "?")
            sender = docmeta.get("sender", {}).get("company", "?")
            source = docmeta.get("source", "?")
            uncertain = len(docmeta.get("uncertain", []))
            print(f"  type={doc_type}  date={doc_date}  sender={sender}  source={source}  uncertain={uncertain}")
            results.append({"name": name, "mime": mime, "doc_type": doc_type, "date": doc_date, "sender": sender, "uncertain": uncertain})
        else:
            method = result.get("X-TAKI:method", "?")
            print(f"  no docmeta (method={method})")
            results.append({"name": name, "mime": mime, "doc_type": None, "method": method})

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY: {len(results)} files tested")
    types = {}
    for r in results:
        t = r.get("doc_type") or "no-docmeta"
        types[t] = types.get(t, 0) + 1
    for t, count in sorted(types.items(), key=lambda x: -x[1]):
        print(f"  {t:20s}  {count}")

if __name__ == "__main__":
    main()
