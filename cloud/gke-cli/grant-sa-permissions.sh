#!/usr/bin/env bash
# grant-sa-permissions.sh — Grant a GCP service account the IAM roles needed
# to create, manage, and tear down Dimster GKE clusters via `gke-setup`.
#
# Roles granted to the deployer service account (--sa):
#   * roles/container.admin          - create/manage/delete GKE clusters & node pools
#   * roles/artifactregistry.admin   - create/push/delete Artifact Registry Docker repos
#   * roles/compute.networkAdmin     - create/delete Cloud Router & Cloud NAT for private nodes
#   * roles/compute.storageAdmin     - list & sweep orphaned GCE disks on teardown
#   * roles/compute.viewer           - list zones & inspect Compute Engine metadata
#   * roles/iam.serviceAccountUser   - attach the node VM SA during GKE creation
#
# Role granted to the GKE node VM service account (--node-sa, defaults to
# Compute Engine default SA):
#   * roles/artifactregistry.reader  - allow GKE nodes to pull Dimster images
#
# Optional role granted to a human caller (--impersonator):
#   * roles/iam.serviceAccountTokenCreator on --sa (enables gcloud impersonation)
#
# Usage:
#   ./cloud/gke-cli/grant-sa-permissions.sh \
#       --project <PROJECT_ID> \
#       --sa <DEPLOYER_SA_EMAIL> \
#       [--node-sa <NODE_SA_EMAIL>] \
#       [--impersonator user:<EMAIL>] \
#       [--dry-run]

set -euo pipefail

PROJECT=""
SA_EMAIL=""
NODE_SA_EMAIL=""
IMPERSONATOR=""
DRY_RUN=false

usage() {
    cat <<EOF >&2
Usage: $(basename "$0") --project <PROJECT_ID> --sa <SA_EMAIL> [options]

Required:
  --project <PROJECT_ID>     GCP project ID
  --sa <SA_EMAIL>            Service account email that gcloud impersonates

Optional:
  --node-sa <SA_EMAIL>       GKE node VM service account (defaults to the
                             Compute Engine default SA for --project)
  --impersonator <MEMBER>    Principal allowed to impersonate --sa, e.g.
                             user:alice@example.com (grants Token Creator)
  --dry-run                  Print gcloud commands without executing them
  -h, --help                 Show this help message

Example:
  $(basename "$0") \\
    --project my-gcp-project \\
    --sa dimster-gke@my-gcp-project.iam.gserviceaccount.com \\
    --impersonator user:me@example.com
EOF
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --project)       PROJECT="${2:-}"; shift 2 ;;
        --sa)            SA_EMAIL="${2:-}"; shift 2 ;;
        --node-sa)       NODE_SA_EMAIL="${2:-}"; shift 2 ;;
        --impersonator)  IMPERSONATOR="${2:-}"; shift 2 ;;
        --dry-run)       DRY_RUN=true; shift ;;
        -h|--help)       usage ;;
        *)               echo "error: unknown argument '$1'" >&2; usage ;;
    esac
done

[ -z "$PROJECT" ] && { echo "error: --project is required" >&2; usage; }
[ -z "$SA_EMAIL" ] && { echo "error: --sa is required" >&2; usage; }

# Run under the caller's own admin credentials even if gcloud currently has
# auth/impersonate_service_account set to the SA being granted permissions.
export CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=""

run_cmd() {
    echo "  -> $*"
    if [ "$DRY_RUN" = false ]; then
        "$@"
    fi
}

# Resolve default Compute Engine node service account if --node-sa was omitted.
if [ -z "$NODE_SA_EMAIL" ]; then
    if [ "$DRY_RUN" = true ]; then
        NODE_SA_EMAIL="<PROJECT_NUMBER>-compute@developer.gserviceaccount.com"
    else
        PROJECT_NUMBER="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')"
        NODE_SA_EMAIL="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    fi
fi

echo "Project:          $PROJECT"
echo "Deployer SA:      $SA_EMAIL"
echo "GKE Node VM SA:   $NODE_SA_EMAIL"
[ -n "$IMPERSONATOR" ] && echo "Impersonator:     $IMPERSONATOR"
echo

echo "=== 1. Enable required GCP APIs ==="
run_cmd gcloud services enable \
    container.googleapis.com \
    compute.googleapis.com \
    artifactregistry.googleapis.com \
    cloudresourcemanager.googleapis.com \
    --project="$PROJECT"

echo
echo "=== 2. Grant project-level roles to deployer SA ==="
for ROLE in \
    roles/container.admin \
    roles/artifactregistry.admin \
    roles/compute.networkAdmin \
    roles/compute.storageAdmin \
    roles/compute.viewer; do
    run_cmd gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="$ROLE" \
        --condition=None
done

echo
echo "=== 3. Allow deployer SA to attach the GKE node VM SA (actAs) ==="
run_cmd gcloud iam service-accounts add-iam-policy-binding "$NODE_SA_EMAIL" \
    --project="$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/iam.serviceAccountUser"

echo
echo "=== 4. Allow GKE node VM SA to pull images from Artifact Registry ==="
run_cmd gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${NODE_SA_EMAIL}" \
    --role="roles/artifactregistry.reader" \
    --condition=None

if [ -n "$IMPERSONATOR" ]; then
    echo
    echo "=== 5. Allow impersonator to mint tokens for deployer SA ==="
    run_cmd gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
        --project="$PROJECT" \
        --member="$IMPERSONATOR" \
        --role="roles/iam.serviceAccountTokenCreator"
fi

echo
echo "Done! Next steps:"
echo "  gcloud config set project $PROJECT"
echo "  gcloud config set auth/impersonate_service_account $SA_EMAIL"
echo "  ./gke-setup check"
