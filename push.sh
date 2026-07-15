#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="${1:-latest}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

if [ "${TAG}" = "latest" ]; then
    echo "Usage: ./push.sh <tag>  (e.g. ./push.sh 20260715-1800)"
    echo "Tags available:"
    podman images --format '{{.Tag}}' "${FULL_IMAGE}" | sort -r | head -5
    exit 1
fi

echo "=== Push ${FULL_IMAGE}:${TAG} + latest ==="

podman push "${FULL_IMAGE}:${TAG}"
podman push "${FULL_IMAGE}:latest"

echo "=== Pushed: ${FULL_IMAGE}:${TAG} ==="
