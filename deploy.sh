#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="${1:-latest}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}"

echo "=== Deploy ${IMAGE}:${TAG} to ${HOST} ==="

ssh "root@${HOST}" "
    cd ${DATAPATH} &&
    podman pull ${FULL_IMAGE}:${TAG} &&
    podman compose down tika &&
    podman compose up -d tika
" 2>&1 | tail -5

echo ""
sleep 3
echo "=== Checking ==="
ssh "root@${HOST}" "podman logs --tail 5 ${CONTAINER_NAME} 2>&1"
echo ""
ssh "root@${HOST}" "curl -s http://localhost:9998/ 2>/dev/null | python3 -m json.tool 2>/dev/null" || true
