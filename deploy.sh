#!/bin/bash
# deploy.sh — nuhost6 image update for a single container in a pod
#
# Usage:
#   ./deploy.sh              # deploy latest
#   ./deploy.sh 20260729-1805  # deploy specific tag
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/DIST"

TAG="${1:-latest}"
FULL_IMAGE="${DOCKER_REGISTRY}/${DOCKER_NS}/${IMAGE}:${TAG}"
COMPOSE_DIR="/nu/container/${TARGET}/compose"
CONF="${COMPOSE_DIR}/compose.nuhost6.conf"

echo "=== Deploy ${IMAGE}:${TAG} to ${HOST} (target: ${TARGET}, service: ${SERVICE}) ==="

# 1. Update pin in compose.nuhost6.conf
echo "  [pin] ${SERVICE} = ${FULL_IMAGE}"
ssh "root@${HOST}" "
    sed -i 's|^${SERVICE} = .*|${SERVICE} = ${FULL_IMAGE}|' ${CONF}
"

# 2. Show diff
echo ""
echo "=== Diff ==="
ssh "root@${HOST}" "nu compose --diff ${COMPOSE_DIR}" 2>&1 || true

# 3. Apply
echo ""
echo "=== Apply ==="
ssh "root@${HOST}" "nu compose --auto-apply ${COMPOSE_DIR}" 2>&1

# 4. Restart only the affected service
echo ""
echo "=== Restart ${TARGET}-${SERVICE} ==="
ssh "root@${HOST}" "systemctl restart ${TARGET}-${SERVICE}.service" 2>&1

sleep 3

# 5. Check
echo ""
echo "=== Logs ==="
ssh "root@${HOST}" "journalctl -u ${TARGET}-${SERVICE}.service --no-pager -n 15" 2>&1

echo ""
echo "=== Health ==="
ssh "root@${HOST}" "curl -s http://localhost:9998/ 2>/dev/null | python3 -m json.tool 2>/dev/null" || true
