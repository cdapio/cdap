# CDAP RBAC Warm Sticky Leases & Task Manager Service

When working on the RBAC Everywhere feature, Namespaced Service Accounts (NSA), or Task Worker pod scaling in this repository:

1.  **Architecture Context**:
    *   Refer to the detailed Warm Sticky Lease research notes here:
        [rbac_taskmanager_research_notes.md](file:///usr/local/google/home/venkataramansh/.gemini/jetski/brain/2099d0c8-9e2b-4db5-a81a-5cfb437c1660/rbac_taskmanager_research_notes.md)
    *   This feature resolves the "429 collision storm" and cold-start latencies (~40s) by shifting tenant isolation from the request level to the namespace/pod lease level.

2.  **Core Routing Rules**:
    *   **Direct Routing**: `RemoteClient` must bypass K8s round-robin load balancing. It resolves the headless task-worker service via DNS expansion to individual pod IPs, queries the `TaskManager` service to resolve the lease, and routes directly to the leased pod IP.
    *   **Lease Registry**: The `TaskManager` service holds the lease maps in-memory to prevent Spanner database write contention. It uses a `ReentrantLock` to serialize checks/actions and prevent race conditions.
    *   **Local Guard**: Individual Task Workers use `StickyLeaseManager` as a fail-safe to reject mismatching namespace requests locally with a `429`.

3.  **Key Classes**:
    *   App Fabric / Client: `RemoteClient`, `RemoteTaskExecutor`, `KubeDiscoveryService`
    *   Task Manager: `TaskManager`, `TaskManagerHttpHandler`, `TaskManagerService`, `TaskManagerMain`
    *   Task Worker: `StickyLeaseManager`, `TaskWorkerHttpHandlerInternal`
