#!/bin/bash
set -eo pipefail

# CDAP System Services check script.
# Relies on exported environment variables: NAMESPACE_FLAG, ROUTER_POD.

if [[ -z "$ROUTER_POD" ]]; then
  echo "Error: ROUTER_POD environment variable is not set." >&2
  exit 1
fi

echo "=== [Check 2/3] Verifying CDAP System Services Health via Router ==="

RESPONSE=$(kubectl ${NAMESPACE_FLAG} exec "${ROUTER_POD}" -- curl -s -H "X-Inverting-Proxy-User-ID: cdap" http://localhost:11015/v3/system/services/status || true)

if [[ -z "$RESPONSE" ]]; then
  echo "Error: Failed to communicate with CDAP Router pod ${ROUTER_POD}." >&2
  exit 1
fi

if [[ "$RESPONSE" == *"Error"* || "$RESPONSE" == *"connect to"* ]]; then
  echo "Error: Router API returned an error: $RESPONSE" >&2
  exit 1
fi

# Print formatted response
if command -v python3 &>/dev/null; then
  echo "$RESPONSE" | python3 -m json.tool || echo "$RESPONSE"
elif command -v python &>/dev/null; then
  echo "$RESPONSE" | python -m json.tool || echo "$RESPONSE"
else
  echo "$RESPONSE"
fi

# Check for any status value that is not "OK"
# Service statuses usually look like: "status": "OK"
NON_OK_SERVICES=$(echo "$RESPONSE" | grep '"status"' | grep -v '"OK"' || true)

if [[ -n "$NON_OK_SERVICES" ]]; then
  echo "Error: Found system services that are not OK!" >&2
  echo "$NON_OK_SERVICES" >&2
  exit 1
fi

echo "Success: All system services are running successfully!"
exit 0
