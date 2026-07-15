#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="$(date +%Y%m%d-%H%M)"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Build ${IMAGE}: ${FULL_IMAGE}:${TAG} ==="

# Build Go binary (use Go 1.26 if available)
echo "--- Go build ---"
cd "${SCRIPT_DIR}"
GO_BIN="go"
if [ -x "$HOME/go1.26/bin/go" ]; then
    GO_BIN="$HOME/go1.26/bin/go"
fi
echo "  using: $($GO_BIN version)"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $GO_BIN build -ldflags="-s -w" -o open_taki_linux .

# Build container image with date tag + latest
echo "--- Container build ---"
podman build \
    --no-cache \
    -t "${FULL_IMAGE}:${TAG}" \
    -t "${FULL_IMAGE}:latest" \
    .

echo ""
echo "=== Built: ${FULL_IMAGE}:${TAG} ==="
echo "  Tag: ${TAG}"
echo ""
echo "Next steps:"
echo "  1. Push:    ./push.sh ${TAG}"
echo "  2. Deploy:  ./deploy.sh ${TAG}"
