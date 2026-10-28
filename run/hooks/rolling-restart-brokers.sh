#!/usr/bin/env bash
# Restarts the Kafka brokers one at a time, resetting broker JVM state (JIT,
# heap, mmapped indexes) between test points. Runs inside the coordinator pod
# with its ServiceAccount (run/deploy/kubernetes/base/coordinator/rbac.yaml).
#
# A restart does NOT clear the page cache (it belongs to the node's kernel),
# so pair this with evict-topic-cache.sh, listed after it, for a cold start.
# Leadership is left skewed by the restarts; in drain-backlog mode the
# coordinator then elects preferred leaders and waits for a full ISR before
# the drain (drainBacklogConfig.settleTimeoutMinutes).
#
# Brokers are restarted in reverse ordinal order, and the next one only goes
# down once the cluster has no under-replicated partitions, so no partition
# loses two replicas (or the KRaft quorum two voters) at once.
#
# Optional environment:
#   KAFKA_POD_SELECTOR   label selector of the broker pods (default app=kafka)
#   KAFKA_GRACE_SECONDS  shutdown grace period; long enough for a controlled
#                        shutdown avoids log recovery on start (default 120)
#   KAFKA_READY_TIMEOUT  seconds to wait for each broker to rejoin (default 600)
set -euo pipefail

SELECTOR="${KAFKA_POD_SELECTOR:-app=kafka}"
GRACE="${KAFKA_GRACE_SECONDS:-120}"
TIMEOUT="${KAFKA_READY_TIMEOUT:-600}"

brokers=$(kubectl get pods -l "$SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort -t- -k2 -n -r)
if [ -z "$brokers" ]; then
  echo "No broker pods match $SELECTOR" >&2
  exit 1
fi

# Under-replicated partitions as seen from a broker other than $1.
under_replicated() {
  local skip="$1" via
  via=$(printf '%s\n' $brokers | grep -vx "$skip" | head -1)
  via="${via:-$skip}"
  kubectl exec "$via" -c kafka -- bash -c \
    'unset KAFKA_OPTS && /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions' \
    2>/dev/null | grep -c "Partition:" || true
}

for broker in $brokers; do
  old_uid=$(kubectl get pod "$broker" -o jsonpath='{.metadata.uid}')
  echo "Restarting $broker (grace ${GRACE}s)"
  kubectl delete pod "$broker" --grace-period="$GRACE" --wait=false >/dev/null

  deadline=$(( $(date +%s) + TIMEOUT ))
  # Wait for the StatefulSet's replacement pod, then for it to be Ready.
  until uid=$(kubectl get pod "$broker" -o jsonpath='{.metadata.uid}' 2>/dev/null) && [ -n "$uid" ] && [ "$uid" != "$old_uid" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "$broker was not recreated within ${TIMEOUT}s" >&2; exit 1; }
    sleep 2
  done
  remaining=$(( deadline - $(date +%s) ))
  kubectl wait --for=condition=Ready "pod/$broker" --timeout="${remaining}s" >/dev/null

  # Ready only means the port is open: wait for its replicas to catch up.
  while urp=$(under_replicated "$broker") && [ "$urp" != "0" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "$urp partitions still under-replicated after ${TIMEOUT}s" >&2; exit 1; }
    echo "    $broker is back; waiting for $urp under-replicated partitions"
    sleep 5
  done
  echo "    $broker restarted and caught up"
done
