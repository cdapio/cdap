#!/bin/bash
set -eo pipefail

# CDAP API connectivity check script (getAllApps equivalent).
# Relies on exported environment variables: NAMESPACE_FLAG, ROUTER_POD.

if [[ -z "$ROUTER_POD" ]]; then
  echo "Error: ROUTER_POD environment variable is not set." >&2
  exit 1
fi

echo "=== [Check 3/3] Verifying CDAP API Connectivity (getAllApps) via Router ==="

HTTP_CODE=$(kubectl ${NAMESPACE_FLAG} exec "${ROUTER_POD}" -- curl -s -o /dev/null -w "%{http_code}" -H "X-Inverting-Proxy-User-ID: cdap" http://localhost:11015/v3/namespaces/default/apps || echo "500")

if [[ "$HTTP_CODE" -ne 200 ]]; then
  echo "Error: CDAP getAllApps API check failed with HTTP status code: $HTTP_CODE" >&2
  exit 1
fi

echo "Success: CDAP Router API is up and serving requests (HTTP 200)!"
exit 0
