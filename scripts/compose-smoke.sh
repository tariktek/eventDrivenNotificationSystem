#!/usr/bin/env bash
# Smoke test of the README quick start on a clean machine: start the whole
# stack with Docker Compose, send one SMS, and wait until the mock provider
# accepted it. CI runs it on amd64 and arm64; it needs Docker, curl and jq.
#
#   scripts/compose-smoke.sh
set -euo pipefail

api=${API_URL:-http://localhost:${API_PORT:-8080}}
key=${API_KEY:-dev-api-key-change-me}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    docker compose ps -a || true
    docker compose logs --no-color --tail 200 migrate api worker mockprovider || true
  fi
  docker compose down -v --remove-orphans || true
  exit "$status"
}
trap cleanup EXIT

docker compose up -d --build

echo "waiting for the API to be ready"
for _ in $(seq 1 90); do
  if curl -fsS "$api/ready" >/dev/null 2>&1; then break; fi
  sleep 2
done
curl -fsS "$api/ready"
echo

id=$(curl -fsS "$api/api/v1/notifications" \
  -H "X-API-Key: $key" -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"compose smoke test","priority":"high"}' | jq -r .id)
echo "created notification $id"

for _ in $(seq 1 30); do
  status=$(curl -fsS "$api/api/v1/notifications/$id" -H "X-API-Key: $key" | jq -r .status)
  echo "status: $status"
  if [ "$status" = sent ]; then
    curl -fsS "$api/api/v1/notifications/$id" -H "X-API-Key: $key" | jq '.delivery_attempts'
    exit 0
  fi
  sleep 1
done
echo "notification $id was not sent within 30 s" >&2
exit 1
