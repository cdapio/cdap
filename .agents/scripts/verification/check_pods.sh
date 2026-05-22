#!/bin/bash
set -eo pipefail

# CDAP Pod status check script.
# Relies on exported environment variables: NAMESPACE_FLAG, MAX_WAIT, CHECK_INTERVAL.

MAX_WAIT="${MAX_WAIT:-300}"
CHECK_INTERVAL="${CHECK_INTERVAL:-10}"

echo "=== [Check 1/3] Verifying CDAP GKE Pods Readiness (Timeout: ${MAX_WAIT}s) ==="

START_TIME=$(date +%s)
END_TIME=$((START_TIME + MAX_WAIT))

while true; do
  CURRENT_TIME=$(date +%s)
  if [[ $CURRENT_TIME -ge $END_TIME ]]; then
    echo "Error: Pod readiness verification timed out after ${MAX_WAIT} seconds." >&2
    exit 1
  fi

  # Get pod list
  PODS_DATA=$(kubectl get pods ${NAMESPACE_FLAG} --selector=custom-resource=v1alpha1.CDAPMaster --no-headers -o custom-columns=NAME:.metadata.name,STATUS:.status.phase 2>/dev/null || true)

  if [[ -z "$PODS_DATA" ]]; then
    echo "Waiting for CDAP pods to be created..."
    sleep "$CHECK_INTERVAL"
    continue
  fi

  ALL_READY=true
  ANY_FAIL=false
  FAILING_PODS=""

  echo "--------------------------------------------------------------------------------"
  printf "%-55s %-8s %-12s %-8s\n" "POD NAME" "READY" "STATUS" "RESTARTS"
  echo "--------------------------------------------------------------------------------"

  while read -r line; do
    if [[ -z "$line" ]]; then continue; fi
    NAME=$(echo "$line" | awk '{print $1}')
    
    CONTAINERS_READY=$(kubectl get pod "$NAME" ${NAMESPACE_FLAG} -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null || echo "false")
    CONTAINER_PHASE=$(kubectl get pod "$NAME" ${NAMESPACE_FLAG} -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    TOTAL_CONTAINERS=$(kubectl get pod "$NAME" ${NAMESPACE_FLAG} -o jsonpath='{.spec.containers[*].name}' 2>/dev/null | wc -w || echo "1")
    
    READY_COUNT=0
    for rc in $CONTAINERS_READY; do
      if [[ "$rc" == "true" ]]; then
        READY_COUNT=$((READY_COUNT + 1))
      fi
    done
    
    RESTARTS=$(kubectl get pod "$NAME" ${NAMESPACE_FLAG} -o jsonpath='{range .status.containerStatuses[*]}{.restartCount}{" "}{end}' 2>/dev/null || echo "0")
    MAX_RESTARTS=0
    for rest in $RESTARTS; do
      if [[ $rest -gt $MAX_RESTARTS ]]; then
        MAX_RESTARTS=$rest
      fi
    done

    # Print pod status line
    printf "%-55s %d/%d     %-12s %-8s\n" "$NAME" "$READY_COUNT" "$TOTAL_CONTAINERS" "$CONTAINER_PHASE" "$MAX_RESTARTS"

    # Evaluate readiness
    if [[ $READY_COUNT -lt $TOTAL_CONTAINERS ]]; then
      ALL_READY=false
    fi

    # Check for failure states
    WAITING_REASONS=$(kubectl get pod "$NAME" ${NAMESPACE_FLAG} -o jsonpath='{.status.containerStatuses[*].state.waiting.reason}' 2>/dev/null || echo "")
    for reason in $WAITING_REASONS; do
      if [[ "$reason" == "CrashLoopBackOff" || "$reason" == "ErrImagePull" || "$reason" == "ImagePullBackOff" ]]; then
        ANY_FAIL=true
        FAILING_PODS="${FAILING_PODS} ${NAME}"
      fi
    done

    if [[ $MAX_RESTARTS -gt 0 ]]; then
      ANY_FAIL=true
      FAILING_PODS="${FAILING_PODS} ${NAME}"
    fi

  done <<< "$PODS_DATA"

  if [[ "$ALL_READY" == "true" ]]; then
    echo "--------------------------------------------------------------------------------"
    echo "Success: All CDAP pods are running and ready!"
    exit 0
  fi

  if [[ "$ANY_FAIL" == "true" ]]; then
    echo "--------------------------------------------------------------------------------"
    echo "Warning: Identified potential failures or restarts in pods:${FAILING_PODS}"
    for pod in $FAILING_PODS; do
      echo "--- Recent Events for pod $pod ---"
      kubectl get events ${NAMESPACE_FLAG} --field-selector involvedObject.name="$pod" --sort-by='.metadata.creationTimestamp' 2>/dev/null || true
      echo "--- Log trail for pod $pod ---"
      kubectl logs "$pod" ${NAMESPACE_FLAG} --tail=50 || true
    done
  fi

  echo "Re-checking in ${CHECK_INTERVAL}s..."
  sleep "$CHECK_INTERVAL"
done
