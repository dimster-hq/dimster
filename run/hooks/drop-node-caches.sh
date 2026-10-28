#!/usr/bin/env bash
# Drops the whole page cache of every node that runs a Kafka broker
# (sync; echo 1 > /proc/sys/vm/drop_caches). A blunt fallback for
# evict-topic-cache.sh: it needs a privileged pod (which the namespace's Pod
# Security admission must allow) and also empties the cache of anything else
# on those nodes, such as co-located workers. Runs inside the coordinator pod
# with its ServiceAccount (run/deploy/kubernetes/base/coordinator/rbac.yaml).
#
# Optional environment:
#   KAFKA_POD_SELECTOR  label selector of the broker pods (default app=kafka)
#   HELPER_IMAGE        image with a shell (default busybox:1.36)
#   HELPER_TIMEOUT      seconds to wait for each helper (default 300)
set -euo pipefail

SELECTOR="${KAFKA_POD_SELECTOR:-app=kafka}"
IMAGE="${HELPER_IMAGE:-busybox:1.36}"
TIMEOUT="${HELPER_TIMEOUT:-300}"
RUN_ID="$(date +%s)-$$"

nodes=$(kubectl get pods -l "$SELECTOR" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
if [ -z "$nodes" ]; then
  echo "No broker pods match $SELECTOR" >&2
  exit 1
fi

helpers=()
cleanup() {
  for h in "${helpers[@]}"; do kubectl delete pod "$h" --ignore-not-found --wait=false >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

i=0
for node in $nodes; do
  helper="dimster-drop-caches-${i}-${RUN_ID}"
  i=$((i + 1))
  helpers+=("$helper")
  kubectl apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${helper}
  labels:
    app: dimster-hook-helper
spec:
  nodeName: ${node}
  restartPolicy: Never
  tolerations:
    - operator: Exists
  containers:
    - name: drop
      image: ${IMAGE}
      securityContext:
        privileged: true
      command: ["sh", "-c", "before=\$(grep '^Cached:' /proc/meminfo); sync && echo 1 > /proc/sys/vm/drop_caches && echo \"\$before -> \$(grep '^Cached:' /proc/meminfo)\""]
YAML
done

status=0
for helper in "${helpers[@]}"; do
  deadline=$(( $(date +%s) + TIMEOUT ))
  phase=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    phase=$(kubectl get pod "$helper" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 2
  done
  if [ "$phase" = "Succeeded" ]; then
    echo "$helper ($(kubectl get pod "$helper" -o jsonpath='{.spec.nodeName}')): $(kubectl logs "$helper")"
  else
    echo "$helper did not succeed (phase ${phase:-unknown}):" >&2
    kubectl logs "$helper" >&2 || true
    status=1
  fi
done
exit $status
