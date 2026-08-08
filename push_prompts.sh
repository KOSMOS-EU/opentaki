#!/bin/bash
set -euo pipefail

# Push taki prompt files as a ZIP package to GitHub Releases.
# No build needed — just zips and pushes the prompt files.
#
# Usage:
#   ./push_prompts.sh                  # push with auto-generated tag
#   ./push_prompts.sh 20260808-1500    # push with specific tag

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/DIST"

TAG="${1:-$(date +%Y%m%d-%H%M)}"
PACKAGE_NAME="taki-prompts"
REPO="${REPO:-opentaki}"
OWNER="${PUSH_ORG:-KOSMOS-EU}"
# Use GitHub token from git remote if PACKAGES_TOKEN not set
TOKEN="${PACKAGES_TOKEN:-$(git remote get-url github 2>/dev/null | sed -n 's|.*://[^:]*:\([^@]*\)@.*|\1|p')}"
TOKEN="${TOKEN:-${PUSH_TOKEN:-}}"

# Files to include
PROMPT_FILES=(
    docmeta_prompt.txt
    docmeta_rescue_prompt.txt
    docmeta_schema.json
    field_rules.json
    store_detect_prompt.txt
    aktenplan.txt
)

# Create ZIP
TMPDIR=$(mktemp -d)
ZIP_PATH="$TMPDIR/${PACKAGE_NAME}-${TAG}.zip"

cd "$SCRIPT_DIR"
zip -j "$ZIP_PATH" "${PROMPT_FILES[@]}"
echo "=== ZIP: ${ZIP_PATH} ($(du -h "$ZIP_PATH" | cut -f1)) ==="

# Push to GitHub Releases
echo "=== Push ${PACKAGE_NAME}:pkg-${TAG} to github:${OWNER}/${REPO} ==="

# Create release
RELEASE_TAG="pkg-${TAG}"
RESP=$(curl -sf -X POST \
    -H "Authorization: token ${TOKEN}" \
    -H "Content-Type: application/json" \
    "https://api.github.com/repos/${OWNER}/${REPO}/releases" \
    -d "{\"tag_name\": \"${RELEASE_TAG}\", \"name\": \"${PACKAGE_NAME} ${TAG}\", \"draft\": false}" \
    2>&1) || {
    # Release might exist — try to get it
    RESP=$(curl -sf \
        -H "Authorization: token ${TOKEN}" \
        "https://api.github.com/repos/${OWNER}/${REPO}/releases/tags/${RELEASE_TAG}")
}

UPLOAD_URL=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['upload_url'].split('{')[0])")

# Upload asset
curl -sf -X POST \
    -H "Authorization: token ${TOKEN}" \
    -H "Content-Type: application/zip" \
    "${UPLOAD_URL}?name=${PACKAGE_NAME}-${TAG}.zip" \
    --data-binary "@${ZIP_PATH}" > /dev/null

echo "=== Pushed ${PACKAGE_NAME}:pkg-${TAG} ==="

# Cleanup
rm -rf "$TMPDIR"
