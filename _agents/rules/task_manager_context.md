# Task Manager & Sticky Lease Context

## Architecture
*   **Environment:** Cloud Data Fusion (CDF) on GKE (ZooKeeper-free).
*   **Service Discovery:** Headless DNS expansion to pod IPs.
*   **Coordination:** Standalone single-replica HTTP `TaskManager` service.
*   **Local Guard:** `StickyLeaseManager` on Task Worker pods enforcing "First-Write Wins" lease lock.

## Concurrency & Workload Limits
*   **Concurrency:** Max 10 concurrent tasks per pod (enforced by `podActiveTaskCounts` / `activeTasks`).
*   **Lifetime Limit:** Max 10 tasks before reset (enforced by `podTotalTaskProcessedCounts`).
    *   *Increment in `resolve`:* Enforces strict limit of 10 tasks started (safe, current behavior).
    *   *Increment in `finish`:* Allows better utilization but pod can process up to 19 tasks due to concurrency.

## Reliability & Recovery
*   **Downtime:** `RemoteClient` falls back to local consistent hashing if `TaskManager` is down.
*   **State Recovery:** To recover from `TaskManager` restarts without polling, use a self-correction pattern:
    *   Task Worker returns `409 Conflict` (with active namespace in body) on lease mismatch.
    *   `RemoteClient` parses 409 and notifies `TaskManager` to update its lease map.
