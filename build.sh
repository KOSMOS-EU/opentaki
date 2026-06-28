#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="$(date +%Y%m%d)"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Build ${IMAGE}: ${FULL_IMAGE}:${TAG} ==="

# Build Go binary
echo "--- Go build ---"
cd "${SCRIPT_DIR}"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o open_taki_linux .

# Build container image with date tag + latest
echo "--- Container build ---"
podman build \
    -t "${FULL_IMAGE}:${TAG}" \
    -t "${FULL_IMAGE}:latest" \
    .

echo ""
echo "=== Built: ${FULL_IMAGE}:${TAG} ==="
echo ""
echo "Next steps:"
echo "  1. Test:    podman run --rm ${FULL_IMAGE}:${TAG} --help"
echo "  2. Push:    ./push.sh"
echo "  3. Deploy:  ./deploy.sh"
