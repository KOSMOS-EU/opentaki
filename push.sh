#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

# In build-worker context, build.sh already pushed — nothing to do
if [ -n "${PUSH_TOKEN:-}" ] && command -v buildah &>/dev/null; then
    echo "=== Push: already done by build.sh ==="
    exit 0
fi

TAG="${1:-latest}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Push ${FULL_IMAGE}:${TAG} + latest ==="

podman push "${FULL_IMAGE}:${TAG}"
podman push "${FULL_IMAGE}:latest"

echo "=== Pushed: ${FULL_IMAGE}:${TAG} ==="
