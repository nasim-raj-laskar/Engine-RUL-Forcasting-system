#!/bin/sh
# register_kafka_connector.sh
# Waits for Kafka Connect REST API, then registers the Solace source connector.
# Run as a one-shot container after kafka-connect is healthy.

set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_CONFIG="/config/solace_kafka_connector.json"
MAX_RETRIES=30
RETRY_DELAY=5

echo "==> Waiting for Kafka Connect REST API at ${CONNECT_URL}..."
for i in $(seq 1 ${MAX_RETRIES}); do
  if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
    echo "    Kafka Connect is ready."
    break
  fi
  echo "    Attempt ${i}/${MAX_RETRIES} — retrying in ${RETRY_DELAY}s..."
  sleep ${RETRY_DELAY}
  if [ "$i" -eq "${MAX_RETRIES}" ]; then
    echo "ERROR: Kafka Connect did not become ready in time." >&2
    exit 1
  fi
done

# Check if connector already exists
CONNECTOR_NAME=$(cat "${CONNECTOR_CONFIG}" | grep '"name"' | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/')
echo "==> Checking for existing connector: ${CONNECTOR_NAME}"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status")

if [ "$HTTP_CODE" = "200" ]; then
  echo "    Connector '${CONNECTOR_NAME}' already exists. Updating..."
  # Extract just the config object for PUT
  CONFIG_ONLY=$(cat "${CONNECTOR_CONFIG}" | sed -n '/"config"/,/^}/p' | sed '1s/.*"config" *: *//' | sed '$s/}$//')
  curl -s -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
    -H "Content-Type: application/json" \
    -d "${CONFIG_ONLY}" > /dev/null
  echo "    Connector updated."
else
  echo "==> Registering new connector: ${CONNECTOR_NAME}"
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @"${CONNECTOR_CONFIG}")

  if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
    echo "    Connector registered successfully (HTTP ${HTTP_CODE})."
  elif [ "$HTTP_CODE" = "409" ]; then
    echo "    Connector already exists (HTTP 409)."
  else
    echo "    WARNING: Unexpected HTTP ${HTTP_CODE} when registering connector."
  fi
fi

# Verify connector status
echo ""
echo "==> Connector status:"
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || echo "    Could not fetch status."
echo ""
echo "==> Done."
