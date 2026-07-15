#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Source DIST if present (not always available in build-worker)
[ -f DIST ] && . DIST

IMAGE="${IMAGE:-open_taki}"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-codeberg.org}"
DOCKER_NS="${DOCKER_NS:-kosmos-eu}"
TAG="${TAG:-$(date +%Y%m%d-%H%M)}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Build ${IMAGE}: ${FULL_IMAGE}:${TAG} ==="

# Container build (multi-stage Dockerfile compiles Go inside)
echo "--- Container build ---"
if command -v buildah &>/dev/null; then
    buildah bud --no-cache --network=host --security-opt label=disable \
        -t "${FULL_IMAGE}:${TAG}" \
        -t "${FULL_IMAGE}:latest" \
        .
else
    podman build --no-cache --network=host \
        -t "${FULL_IMAGE}:${TAG}" \
        -t "${FULL_IMAGE}:latest" \
        .
fi

# Push if token available (build-worker provides PUSH_TOKEN)
if [ -n "${PUSH_TOKEN:-}" ]; then
    echo "--- Push ---"
    buildah push --creds="token:${PUSH_TOKEN}" "${FULL_IMAGE}:${TAG}"
    buildah tag "${FULL_IMAGE}:${TAG}" "${FULL_IMAGE}:latest"
    buildah push --creds="token:${PUSH_TOKEN}" "${FULL_IMAGE}:latest"
fi

echo ""
echo "=== Built: ${FULL_IMAGE}:${TAG} ==="
