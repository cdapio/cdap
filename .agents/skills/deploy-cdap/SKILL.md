# Deploy CDAP Skill

This skill builds, packages, containerizes, pushes the CDAP image to Google Artifact Registry, patches the Custom Resource (CR) in GKE, and restarts the pods.

Rather than running individual commands manually, use the automated script:
[.agents/scripts/deploy.sh](file:///usr/local/google/home/priyanshujha/CDF/cdap-fin/cdap/.agents/scripts/deploy.sh).

---

## 1. Parameters Required

The deployment script accepts the following flags:

| Flag | Prompt/Environment Key | Description | Default Value |
|---|---|---|---|
| `--tp-project` | `TP_PROJECT` | GCP Project ID hosting GKE cluster. | *Required* |
| `--cluster-name` | `CLUSTER_NAME` | The target GKE cluster name. | *Required* |
| `--customer-project` | `CUSTOMER_PROJECT` | GCP Project ID hosting Artifact Registry. | *Required* |
| `--cluster-region` | `CLUSTER_REGION` | The region of GKE cluster. | `us-east1` |
| `--ar-region` | `AR_REGION` | The region of Artifact Registry repository. | `us-east1` |
| `--run-maven` | `RUN_MAVEN` | Build the project using Maven first (~15 mins). | *Omitted (Skip)* |

---

## 2. Interactive Execution (Recommended)

To run the deployment interactively via the **main agent** so you can see live logs and approve credentials:

```bash
./.agents/scripts/deploy.sh \
  --tp-project <TP_PROJECT> \
  --cluster-name <CLUSTER_NAME> \
  --customer-project <CUSTOMER_PROJECT>
```

Add `--run-maven` if you need to build the packages from clean source code.

---

## 3. Script Core Operations

For transparency, the script executes the following stages:
1. **Resolve Tenant Project Number**: Retrieves the numeric ID to generate the Compute Engine service account name.
2. **Retrieve GKE Credentials**: Runs `gcloud container clusters get-credentials`.
3. **IAM Permissions**: Automatically grants `roles/artifactregistry.reader` to the GKE nodes service account on the customer's registry repository.
4. **Build & Containerize**: Builds the local Docker image using [.agents/skills/deploy-cdap/Dockerfile](file:///usr/local/google/home/priyanshujha/CDF/cdap-fin/cdap/.agents/skills/deploy-cdap/Dockerfile).
5. **Push**: Configures registry authentication and pushes the image.
6. **Patch CDAP CR**: Patches **both** `spec.image` and `status.imageToUse` in the `CDAPMaster` custom resource to avoid operator sync loops.
7. **Restart**: Deletes CDAP Master pods to trigger immediate rollout.
