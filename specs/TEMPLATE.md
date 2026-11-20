# PR Delta Regression Test Plan Template

Copy this template into `specs/<feature-or-issue>.md` (or into the PR description under `## Delta Regression Test Plan`) for changes touching `src/main/`, `bin/`, or `conf/`.

## Summary of Change and Blast Radius

- **Modified Modules**: `<e.g., cdap-app-fabric, cdap-runtime-ext-dataproc>`
- **Modified Runtime Surfaces / Pods**: `<e.g., appfabric, runtime, task-worker, preview-runner>`
- **Modified Config Keys (`cdap-default.xml` / `cConf`)**: `<list keys or None>`

## 7-Axis Orthogonality Matrix Evaluation

Every row must state `IMPACTED` or `NOT_IMPACTED`.
- `IMPACTED` rows must name a concrete test scenario and a non-vacuous oracle (real output sink record count, persisted state assertion, or exact HTTP error status and message; never a no-op `Trash` sink or bare HTTP 200).
- `NOT_IMPACTED` rows must cite a concrete file path, module boundary, or configuration key showing why the axis does not intersect the diff.

| Axis | Status (`IMPACTED` / `NOT_IMPACTED`) | Scenario or Code/Config Evidence | Verification Oracle |
| :--- | :--- | :--- | :--- |
| Network Topology | `NOT_IMPACTED` | `<cite file/module, e.g., cdap-data-fabric/src/main/... does not touch SSH, proxy, or network endpoint resolution>` | `Static module boundary check` |
| Identity & Auth | `IMPACTED` | `<e.g., Exercise credential token propagation with RBAC and workload identity enabled>` | `<e.g., Assert HTTP 200 with valid token and HTTP 403 on missing permission without leaked stack trace>` |
| Edition & Storage/HA | `NOT_IMPACTED` | `<cite file/module, e.g., Change stays inside cdap-gateway router handler above StructuredTable SPI>` | `Static SPI boundary check` |
| Security & Data Governance | `NOT_IMPACTED` | `<cite file/module, e.g., No changes in cdap-kms, cdap-encryption-ext-tink, or lineage writer paths>` | `Static module boundary check` |
| Action & Instance Options | `IMPACTED` | `<e.g., Test default cConf value and explicit override in cdap-site.xml>` | `<e.g., Unit test asserts both fallback default and custom override behavior>` |
| Execution Surface | `IMPACTED` | `<e.g., Run pipeline preview in preview-runner and batch run via runtime>` | `<e.g., Destination table row count matches input records and zero errors in service logs>` |
| Lifecycle & Upgrade | `NOT_IMPACTED` | `<cite file/module, e.g., No schema, StructuredTable specification, or UpgradeJobMain changes in cdap-master>` | `Static schema boundary check` |

## Hunk-Revert Non-Vacuity Check (`RED` -> `GREEN`)

- **Reverted Production Hunks**: `<file:line range>`
- **RED Outcome (production hunks reverted, new tests kept)**: `<Exact failing assertion or compile-time signature boundary>`
- **GREEN Outcome (full PR applied)**: `<mvn test -pl <module> -Dtest=<TestClass> exit 0>`
