# Verify CDAP Skill

This skill monitors and verifies the health of the CDAP deployment after updates or patches are applied.

Rather than running GKE checks manually, use the automated script:
[.agents/scripts/verify.sh](file:///usr/local/google/home/priyanshujha/CDF/cdap-fin/cdap/.agents/scripts/verify.sh).

---

## 1. Parameters Required

The verification script accepts the following optional flags:

| Flag | Default Value | Description |
|---|---|---|
| `--test <type>` (or `-t`) | `all` | The verification check type: `all`, `pods`, `services`, or `api`. |
| `--namespace <ns>` (or `-n`) | `default` | The Kubernetes namespace of the CDAP master resource. |
| `--max-wait <seconds>` | `300` | Maximum time to wait for pods to become ready. |
| `--check-interval <seconds>` | `10` | Time interval between status checks. |

---

## 2. Interactive Execution (Recommended)

To verify all layers of the deployment:

```bash
./.agents/scripts/verify.sh --test all
```

To run a specific check (e.g. CDAP System Services status):

```bash
./.agents/scripts/verify.sh --test services --namespace default
```

---

## 3. Script Core Operations

The verification suite performs the following checks depending on the `--test` value:

1. **Pods Status (`pods`)**: Continually monitors ready counts, restart counts, and status phases for all pods matching the selector `custom-resource=v1alpha1.CDAPMaster`. If a pod crashes or fails to start, GKE events and container log trails are automatically dumped.
2. **System Services Health (`services`)**: Locates the active Router pod and queries the system health status API:
   `curl http://localhost:11015/v3/system/services/status`
   Ensures all system services (metrics, appfabric, logs, etc.) report status `OK`.
3. **API Connectivity (`api`)**: Queries the namespaces endpoint on the CDAP Router to verify core API response (equivalent to `getAllApps` check):
   `curl http://localhost:11015/v3/namespaces/default/apps`
   Ensures it returns a HTTP status code `200`.
