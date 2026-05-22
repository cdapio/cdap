# CDAP DevOps Workflow

This directory automates building, containerizing, deploying, and verifying CDAP on GKE.

---

## 🚀 How to Use the Custom Agent

To build, deploy, and verify your changes automatically using the **CDAP DevOps Agent**:

1. Select or mention **`@cdap-devops-agent`** in the IDE agent panel.
2. Prompt it with your target parameters, for example:
   ```text
   deploy to GKE
   TP: <TP_PROJECT>
   Cluster: <CLUSTER_NAME>
   Customer Project: <CUSTOMER_PROJECT>
   run the maven build
   ```
3. The agent will prompt for confirmation (if needed), execute the deployment script, and verify the rollout health automatically.

---

## 1. Directory Structure

```
.agents/
├── README.md                           # Main usage documentation
├── scripts/
│   ├── deploy.sh                       # Core deployment script
│   ├── verify.sh                       # Core verification orchestrator (Entrypoint)
│   └── verification/                   # Modular verification checks
│       ├── check_pods.sh               # Checks pod readiness & container counts
│       ├── check_services.sh           # Checks CDAP system services health (OK status)
│       └── check_api.sh                # Checks API connectivity (getAllApps test)
└── skills/
    ├── deploy-cdap/
    │   ├── Dockerfile                  # Stage packaging Dockerfile
    │   └── SKILL.md                    # Deploy documentation
    └── verify-cdap/
        └── SKILL.md                    # Verify documentation
```

---

## 2. Interactive Execution (Recommended Mode)

To run the deployment interactively, execute the scripts directly in the primary terminal/chat.

### Execution Command Flow

#### Step 1: Run the Deployment
```bash
./.agents/scripts/deploy.sh \
  --tp-project <TP_PROJECT> \
  --cluster-name <CLUSTER_NAME> \
  --customer-project <CUSTOMER_PROJECT>
```

*Optional: Add `--run-maven` if you need to compile the latest source code from scratch.*

#### Step 2: Verify the Rollout
```bash
./.agents/scripts/verify.sh --test all --namespace default
```

---

## 3. Required Parameters

### Deployment Script Parameters

| Parameter Name | Prompt Key | Description | Default Value |
|---|---|---|---|
| Tenant Project ID | `TP_PROJECT` | GCP Project ID hosting GKE cluster. | *Required* |
| Cluster Name | `CLUSTER_NAME` | Target GKE cluster name. | *Required* |
| Customer Project ID | `CUSTOMER_PROJECT` | GCP Project ID hosting Artifact Registry. | *Required* |
| Cluster Region | `CLUSTER_REGION` | The region of the GKE cluster. | `us-east1` |
| Artifact Registry Region | `AR_REGION` | The region of the Artifact Registry cdf repository. | `us-east1` |

### Verification Script Parameters

| Parameter Name | Flag | Description | Default Value |
|---|---|---|---|
| Test Type | `--test` / `-t` | The check type to run: `all`, `pods`, `services`, or `api`. | `all` |
| Namespace | `--namespace` / `-n` | The Kubernetes namespace CDAP is deployed in. | `default` |
| Max Wait | `--max-wait` | Maximum time (in seconds) to wait for checks to pass. | `300` |
| Check Interval | `--check-interval` | Interval (in seconds) between status checks. | `10` |
