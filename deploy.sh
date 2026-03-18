#!/bin/bash

readonly ENV_FILE=${1:-".env.dev"}
readonly PORT=${2:-"60666"}
echo "Creating [mictlanx] network..."

docker network create --driver=bridge mictlanx || true

echo "Removing existing routers"
docker compose --env-file $ENV_FILE -f docker-compose.yml down

echo "Starting a new MictlanX Cluster "
docker compose --env-file .mictlanxrm${ENV_FILE} --env-file $ENV_FILE -f docker-compose.yml up -d
# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' is required but not installed. Please install 'jq' to run this script."
    echo "You can install 'jq' using your package manager. For example:"
    echo "  - On Debian/Ubuntu: sudo apt-get install jq"
    echo "  - On macOS with Homebrew: brew install jq"
    exit 1
fi
# -------------------------------
# Healthcheck: wait for peers
# -------------------------------
API="http://localhost:$PORT/api/v4/peers/stats"
DEADLINE=$((SECONDS + 180))   # timeout after 180s; adjust as needed

echo "Waiting for peers to appear at $API ..."



while true; do
  # fetch JSON (fail on non-2xx; quiet errors to stderr)
  if json="$(curl -fsS "$API")"; then
    count="$(jq -r 'length' <<<"$json" 2>/dev/null || echo 0)"
    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -gt 0 ]; then
      echo "✅ MictlanX cluster is healthy (peers: $count)"
      break
    else
      echo "⏳ No peers yet (length=$count)"
    fi
  else
    echo "⏳ API not ready yet (curl failed)"
  fi

  # optional timeout
  if [ $SECONDS -ge $DEADLINE ]; then
    echo "❌ Timed out waiting for peers at $API"
    exit 1
  fi

  sleep 2
done