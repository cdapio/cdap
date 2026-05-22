#!/bin/bash
set -eo pipefail

# CDAP deployment automation script.
# Usage: ./deploy.sh --tp-project <TP_PROJECT> --cluster-name <CLUSTER_NAME> --customer-project <CUSTOMER_PROJECT> [--cluster-region <CLUSTER_REGION>] [--ar-region <AR_REGION>] [--run-maven]

CLUSTER_REGION="us-east1"
AR_REGION="us-east1"
RUN_MAVEN="false"

while [[ $# -gt 0 ]]; do
  case $1 in
    --tp-project)
      TP_PROJECT="$2"
      shift 2
      ;;
    --cluster-name)
      CLUSTER_NAME="$2"
      shift 2
      ;;
    --customer-project)
      CUSTOMER_PROJECT="$2"
      shift 2
      ;;
    --cluster-region)
      CLUSTER_REGION="$2"
      shift 2
      ;;
    --ar-region)
      AR_REGION="$2"
      shift 2
      ;;
    --run-maven)
      RUN_MAVEN="true"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$TP_PROJECT" || -z "$CLUSTER_NAME" || -z "$CUSTOMER_PROJECT" ]]; then
  echo "Error: Missing required arguments."
  echo "Usage: $0 --tp-project <TP_PROJECT> --cluster-name <CLUSTER_NAME> --customer-project <CUSTOMER_PROJECT> [--cluster-region <CLUSTER_REGION>] [--ar-region <AR_REGION>] [--run-maven]"
  exit 1
fi

echo "=== [1/7] Resolving Tenant Project Number ==="
TP_PROJECT_NUMBER=$(gcloud projects describe "$TP_PROJECT" --format="value(projectNumber)")
echo "Tenant project number: $TP_PROJECT_NUMBER"

echo "=== [2/7] Authenticating with GKE Cluster ==="
gcloud container clusters get-credentials "$CLUSTER_NAME" --region "$CLUSTER_REGION" --project "$TP_PROJECT"

echo "=== [3/7] Setting up Cross-Project Artifact Registry Reader Permissions ==="
# Check AR Repository exists
gcloud artifacts repositories describe cdf --project="$CUSTOMER_PROJECT" --location="$AR_REGION" >/dev/null

# Add policy binding
echo "Ensuring service account ${TP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com has roles/artifactregistry.reader..."
gcloud artifacts repositories add-iam-policy-binding cdf \
  --project="$CUSTOMER_PROJECT" \
  --location="$AR_REGION" \
  --member="serviceAccount:${TP_PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader" >/dev/null

if [[ "$RUN_MAVEN" == "true" ]]; then
  echo "=== [4/7] Running Maven Build ==="
  mvn clean install -DskipTests -pl cdap-master,cdap-app-templates/cdap-etl/cdap-data-pipeline3_2.12,cdap-app-templates/cdap-etl/cdap-data-streams3_2.12,cdap-app-templates/cdap-program-report -am -P templates,dist,k8s -Drat.skip=true -Dcheckstyle.skip=true
else
  echo "=== [4/7] Skipping Maven Build (using existing stage artifacts) ==="
fi

echo "=== [5/7] Building Container Image ==="
IMAGE_URL="${AR_REGION}-docker.pkg.dev/${CUSTOMER_PROJECT}/cdf/cloud-data-fusion:latest"
docker build --no-cache -f .agents/skills/deploy-cdap/Dockerfile cdap-master/target/stage-packaging -t "$IMAGE_URL"

echo "=== [6/7] Pushing Image to Artifact Registry ==="
gcloud auth configure-docker "${AR_REGION}-docker.pkg.dev" --quiet
docker push "$IMAGE_URL"

echo "=== [7/7] Patching CDAP Custom Resource & Triggering Rollout ==="
echo "Patching CDAPMaster CR '$CLUSTER_NAME' spec.image and status.imageToUse..."
kubectl patch cdapmaster "$CLUSTER_NAME" --type='merge' -p "{\"spec\":{\"image\":\"$IMAGE_URL\",\"imagePullPolicy\":\"Always\"},\"status\":{\"imageToUse\":\"$IMAGE_URL\"}}"

echo "Restarting CDAP Pods to pull the new image..."
kubectl delete pod --selector=custom-resource=v1alpha1.CDAPMaster

echo "=== Deployment Finished Successfully ==="
