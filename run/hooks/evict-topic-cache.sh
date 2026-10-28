#!/usr/bin/env bash
# Evicts the page cache of the backlog topics on every Kafka broker, so each
# drain starts from a cold cache. Intended as a drain-backlog beforeDrain
# hook (engineConfig.hooks.beforeDrain); runs inside the coordinator pod with
# its ServiceAccount (run/deploy/kubernetes/base/coordinator/rbac.yaml).
#
# Restarting a broker does NOT clear its page cache: the cache belongs to the
# node's kernel and survives the process (and, on local NVMe volumes, the
# pod). So for each broker this starts a short-lived helper pod on the
# broker's node that mounts the broker's data volume and, for every file of
# the backlog topics, flushes it (fsync) and drops its cached pages
# (posix_fadvise DONTNEED). Every replica is evicted, not just leaders, since
# leadership can move between drains. No privileges are needed.
#
# Reads DIMSTER_TOPICS (comma-separated, set by the coordinator).
# Optional environment:
#   KAFKA_POD_SELECTOR  label selector of the broker pods (default app=kafka)
#   KAFKA_DATA_VOLUME   name of the volumeClaimTemplate (default kafka-data)
#   KAFKA_LOG_SUBDIR    log.dirs path within the volume (default kafka-logs)
#   HELPER_IMAGE        image with python3 (default python:3-slim)
#   HELPER_TIMEOUT      seconds to wait for each helper (default 600)
set -euo pipefail

: "${DIMSTER_TOPICS:?DIMSTER_TOPICS is not set: run this as a beforeDrain hook}"
SELECTOR="${KAFKA_POD_SELECTOR:-app=kafka}"
VOLUME="${KAFKA_DATA_VOLUME:-kafka-data}"
LOG_SUBDIR="${KAFKA_LOG_SUBDIR:-kafka-logs}"
IMAGE="${HELPER_IMAGE:-python:3-slim}"
TIMEOUT="${HELPER_TIMEOUT:-600}"
RUN_ID="$(date +%s)-$$"

# Runs in each helper. Reports how much of the topics was resident before
# and after (mincore), so the coordinator log shows the eviction worked.
read -r -d '' EVICT_PY <<'PY' || true
import ctypes, ctypes.util, mmap, os, sys

root, topics = sys.argv[1], sys.argv[2].split(",")
libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
page = os.sysconf("SC_PAGE_SIZE")

def resident(path, size):
    """Bytes of the file in the page cache, or None if mincore is unavailable."""
    if size == 0:
        return 0
    try:
        with open(path, "rb") as f, mmap.mmap(f.fileno(), size, access=mmap.ACCESS_COPY) as m:
            pages = (size + page - 1) // page
            vec = (ctypes.c_ubyte * pages)()
            addr = ctypes.addressof(ctypes.c_char.from_buffer(m))
            if libc.mincore(ctypes.c_void_p(addr), ctypes.c_size_t(size), vec) != 0:
                return None
            return sum(v & 1 for v in vec) * page
    except (OSError, ValueError, BufferError):
        return None

files = before = after = total = 0
measured = True
for d in sorted(os.listdir(root)):
    if not any(d.startswith(t + "-") and d[len(t) + 1:].isdigit() for t in topics):
        continue
    for name in os.listdir(os.path.join(root, d)):
        path = os.path.join(root, d, name)
        if not os.path.isfile(path):
            continue
        size = os.path.getsize(path)
        r = resident(path, size)
        measured = measured and r is not None
        before += r or 0
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)  # DONTNEED only drops clean pages
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
        r = resident(path, size)
        after += r or 0
        files += 1
        total += size

mb = lambda b: b / 1e6
if measured:
    print(f"evicted {files} files ({mb(total):.0f} MB): resident {mb(before):.0f} MB -> {mb(after):.0f} MB")
else:
    print(f"evicted {files} files ({mb(total):.0f} MB) (residency not measurable here)")
PY

pods=$(kubectl get pods -l "$SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}={.spec.nodeName}{"\n"}{end}')
if [ -z "$pods" ]; then
  echo "No broker pods match $SELECTOR" >&2
  exit 1
fi

helpers=()
cleanup() {
  for h in "${helpers[@]}"; do kubectl delete pod "$h" --ignore-not-found --wait=false >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

# The Python goes in as a YAML block scalar, indented under args.
script_block=$(printf '%s\n' "$EVICT_PY" | sed 's/^/            /')

for entry in $pods; do
  broker="${entry%%=*}"
  node="${entry#*=}"
  helper="dimster-evict-${broker}-${RUN_ID}"
  helpers+=("$helper")
  # Pinned to the broker's node (an RWO volume can be mounted by several
  # pods there); tolerates any taint a dedicated Kafka node group has.
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
    - name: evict
      image: ${IMAGE}
      command: ["python3", "-c"]
      args:
        - |
${script_block}
        - /data/${LOG_SUBDIR}
        - ${DIMSTER_TOPICS}
      volumeMounts:
        - name: data
          mountPath: /data
          readOnly: true
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${VOLUME}-${broker}
        readOnly: true
YAML
done

# Waits for a helper to finish; fails fast if it fails.
await_helper() {
  local helper="$1" deadline=$(( $(date +%s) + ${TIMEOUT%s} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    case "$(kubectl get pod "$helper" -o jsonpath='{.status.phase}' 2>/dev/null)" in
      Succeeded) return 0 ;;
      Failed) return 1 ;;
    esac
    sleep 2
  done
  return 1
}

status=0
for helper in "${helpers[@]}"; do
  if await_helper "$helper"; then
    echo "$helper: $(kubectl logs "$helper")"
  else
    echo "$helper did not succeed:" >&2
    kubectl logs "$helper" >&2 || true
    kubectl get pod "$helper" -o jsonpath='{.status}' >&2 || true
    status=1
  fi
done
exit $status
