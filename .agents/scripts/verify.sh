#!/bin/bash
set -eo pipefail

# CDAP deployment verification orchestrator.
# Usage: ./verify.sh [--test <test_type>] [--namespace <namespace>] [--max-wait <seconds>] [--check-interval <seconds>]

MAX_WAIT=300
CHECK_INTERVAL=10
TEST_TYPE="all"
NAMESPACE="default"

while [[ $# -gt 0 ]]; do
  case $1 in
    --max-wait)
      MAX_WAIT="$2"
      shift 2
      ;;
    --check-interval)
      CHECK_INTERVAL="$2"
      shift 2
      ;;
    --test|-t)
      TEST_TYPE="$2"
      shift 2
      ;;
    --namespace|-n)
      NAMESPACE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate check type
if [[ "$TEST_TYPE" != "all" && "$TEST_TYPE" != "pods" && "$TEST_TYPE" != "services" && "$TEST_TYPE" != "api" ]]; then
  echo "Error: Invalid test type '$TEST_TYPE'. Supported types: all, pods, services, api" >&2
  exit 1
fi

# Set namespace flag
export NAMESPACE_FLAG=""
if [[ -n "$NAMESPACE" ]]; then
  export NAMESPACE_FLAG="-n $NAMESPACE"
fi

export MAX_WAIT
export CHECK_INTERVAL
export NAMESPACE

echo "=== Running CDAP GKE Verification Suite ==="
echo "Namespace: ${NAMESPACE:-default}"
echo "Check Type: ${TEST_TYPE}"

# Resolve Router pod for API checks
if [[ "$TEST_TYPE" == "all" || "$TEST_TYPE" == "services" || "$TEST_TYPE" == "api" ]]; then
  echo "Resolving active CDAP Router pod..."
  export ROUTER_POD=$(kubectl ${NAMESPACE_FLAG} get pods -l cdap.container.Router -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$ROUTER_POD" ]]; then
    echo "Warning: Could not resolve any active CDAP Router pod."
    # If the user explicitly requested services or api test, but we have no router pod, fail the check.
    if [[ "$TEST_TYPE" != "all" ]]; then
      echo "Error: Router pod is required for test type '$TEST_TYPE'." >&2
      exit 1
    fi
  else
    echo "Found CDAP Router pod: ${ROUTER_POD}"
  fi
fi

# Run the selected scripts
run_check() {
  local script_name="$1"
  local script_path="./.agents/scripts/verification/${script_name}"
  
  if [[ ! -f "$script_path" ]]; then
    echo "Error: Verification script '${script_path}' not found." >&2
    exit 1
  fi
  
  chmod +x "$script_path"
  "$script_path"
}

case $TEST_TYPE in
  pods)
    run_check "check_pods.sh"
    ;;
  services)
    run_check "check_services.sh"
    ;;
  api)
    run_check "check_api.sh"
    ;;
  all)
    run_check "check_pods.sh"
    # If router pod exists, execute services and API checks
    if [[ -n "$ROUTER_POD" ]]; then
      run_check "check_services.sh"
      run_check "check_api.sh"
    else
      echo "Skipping CDAP Services & API checks because no active CDAP Router pod was found."
    fi
    ;;
esac

echo "=== All Verification Checks Passed Successfully ==="
exit 0
