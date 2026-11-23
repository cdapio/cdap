# CDAP Repository Code Review Guidelines

Apply these standards when reviewing pull requests in `cdapio/cdap`:

## 1. 7-Axis Delta Regression Test Plan

For any pull request modifying production runtime code (`src/main/`, `bin/`, or `conf/`), check that the PR description or an accompanying `specs/*.md` file evaluates all 7 runtime orthogonality axes:

1. **Network Topology** (public endpoints, private peering, private service connect, shared network routing, runtime monitor proxy)
2. **Identity & Auth** (unauthenticated/standard mode, role-based access control, system/user service account separation, workload identity / namespaced service accounts)
3. **Edition & Storage/HA** (single-node Postgres vs. distributed Spanner / multi-replica stateless `appFabric` and `appFabricProcessor`)
4. **Security & Data Governance** (customer-managed encryption keys, Tink/KMS secure store, audit and lineage event publishing)
5. **Action & Instance Options** (`cdap-default.xml` defaults, `cdap-site.xml` overrides, runtime arguments, feature flags)
6. **Execution Surface** (Studio pipeline validation, `preview-runner`, `task-worker` / `system-worker` pods, external cluster provisioners)
7. **Lifecycle & Upgrade** (fresh initialization, in-place configuration update, upgrade compatibility from previous minor release)

Flag `NOT_IMPACTED` claims that lack a concrete code, config, or SPI boundary citation, and flag code changes that read `cConf` keys, auth contexts, or network endpoints without testing the corresponding axis.

## 2. Non-Vacuous Test Oracles and Hunk-Revert (`RED` -> `GREEN`) Discipline

- **No Tautological Tests**: Every bug fix or behavioral change must include a test that fails (`RED`) when the production hunk is reverted and passes (`GREEN`) with the production hunk applied. Flag mock setups that merely assert the mock's own stubbed return value.
- **Real Assertions**: Flag tests that assert only `HTTP 200` or `COMPLETED` without verifying output data, persisted state transitions, or exact negative-path error status and message substrings. Never accept no-op `Trash` sinks as proof of data-plane correctness.

## 3. Security, Concurrency, and Open-Source Hygiene

- **No Internal Identifiers**: Reject any comment, commit message, spec, or test fixture containing private issue-tracker shortlinks, private corporate hostnames, or internal email addresses.
- **Apache License 2.0 Headers**: Every new source, script, or workflow file not excluded by `apache-rat-plugin` must carry the standard `Copyright © 2026 Cask Data, Inc.` Apache 2.0 header.
- **Resource & Thread Safety**: Verify streams, iterators, transactions, and HTTP connections use `try-with-resources` or deterministic `finally` cleanup, and verify shared state across Netty handlers or Twill runnables is thread-safe.
