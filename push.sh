#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="${1:-$(date +%Y%m%d)}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Push ${FULL_IMAGE}:${TAG} + latest ==="

podman push "${FULL_IMAGE}:${TAG}"
podman push "${FULL_IMAGE}:latest"

echo "=== Pushed ==="
