# Workload Hooks

Hooks run your own scripts at fixed points in a workload's lifecycle: before and after the workload, before and after each test point, and (in drain-backlog mode) just before each drain. Use them to act on the environment between test points. For example, you can evict a topic's page cache, restart brokers, take a snapshot, or call a cloud API.

The coordinator knows nothing about what a hook does. It runs the script, waits for it with a timeout, records the outcome in the result JSON, and fails or continues as configured. Anything infrastructure-specific, such as `kubectl`, `docker` or a cloud CLI, lives in the script. That is what keeps the coordinator free of Kubernetes knowledge.

## Configuring hooks

Hooks live under `engineConfig.hooks` in any workload:

```yaml
engineConfig:
  hooks:
    beforeWorkload:
      - script: hooks/check-cluster.sh
    beforeTestPoint:
      - script: hooks/note.sh
        args: ["starting"]
    afterTestPoint:
      - script: hooks/note.sh
        args: ["finished"]
        onFailure: CONTINUE
    beforeDrain:                       # drain-backlog only
      - script: evict-topic-cache.sh
        timeoutSeconds: 900
```

| Hook point | When it runs |
|------------|--------------|
| `beforeWorkload` | Once, before the first test point. Runs again when a workload is resumed. |
| `afterWorkload` | Once, after the last test point of a workload that completed. |
| `beforeTestPoint` | Before each test point, ahead of its topic and client setup. |
| `afterTestPoint` | After each test point, once its clients are stopped and its topics deleted (except a backlog kept for [reuse](../modes/drain-backlog.md#reusing-a-backlog)). |
| `beforeDrain` | Drain-backlog only. After the backlog exists (built or reused) and producers are paused, before consumers start. |

Each point takes a list of hooks, which run in order.

| Field | Default | Description |
|-------|---------|-------------|
| `script` | required | The script to run (see [Where scripts come from](#where-scripts-come-from)). |
| `args` | `[]` | Arguments passed to the script. |
| `timeoutSeconds` | `600` | The script is killed, along with anything it started, after this long. |
| `onFailure` | `FAIL` | `FAIL` fails the workload when the script exits non-zero or times out. The checkpoint is kept, so `dimster resume` retries from that test point. `CONTINUE` records the failure and carries on. |

The workload fails at start, before any test point, if a script is missing, a timeout is not positive, or `beforeDrain` is used outside drain-backlog mode. A misspelt hook point also fails the workload: workload YAML is parsed strictly.

## What a script sees

Scripts get their context as environment variables:

| Variable | Value |
|----------|-------|
| `DIMSTER_HOOK_POINT` | e.g. `beforeDrain` |
| `DIMSTER_MODE` | `run`, `explore`, `correctness` or `drain-backlog` |
| `DIMSTER_WORKLOAD` | The workload's name (its file stem) |
| `DIMSTER_TEST_POINT_INDEX` | 0-based index of the test point; `-1` for workload-level hooks |
| `DIMSTER_TEST_POINT_COUNT` | Number of test points (test-point hooks only) |
| `DIMSTER_TEST_POINT_LABEL` | The test point's label |
| `DIMSTER_SCENARIO` | The test point's scenario |
| `DIMSTER_BOOTSTRAP_SERVERS` | The test point's `bootstrap.servers` |
| `DIMSTER_TOPICS` | Comma-separated topics holding data at this point. For `beforeDrain`, the backlog topics (Flink input topics with a Flink stage). For `afterTestPoint`, a backlog kept for reuse. Unset when there are none. |

Write scripts so they can run twice in a row: a resumed workload repeats `beforeWorkload` and the hooks of the test point it resumes at.

## Where scripts run

On Kubernetes, hooks run inside the coordinator pod. The coordinator image includes `kubectl`. The pod runs as the `benchmark-coordinator` ServiceAccount, whose Role (`run/deploy/kubernetes/base/coordinator/rbac.yaml`) lets scripts do the following in the Dimster namespace only:

- manage pods, including exec and logs
- read PVCs
- read and patch StatefulSets and Deployments

`kubectl` inside the pod already targets that namespace.

Because scripts run in the cluster, hooks keep working when the CLI is detached or your laptop is closed.

In [embedded mode](../getting-started/11-embedded-mode-without-k8s.md) hooks run as local processes next to the engine, so a script can use `docker`, `ssh` or whatever controls your brokers.

## Where scripts come from

The CLI reads every script a workload references and sends its contents in the run request. The coordinator runs only scripts sent this way. A script path is resolved in this order:

1. an absolute path
2. relative to the workload file
3. relative to the current directory
4. a built-in reference script in `run/hooks/`

The CLI fails before starting the run if a script cannot be found. Copies of the scripts are saved under `configs/hooks/` in the results directory, so a result records exactly what ran.

## What gets recorded

Every execution is recorded in the result JSON under `metadata.hooks`, with these fields:

- hook point
- script and args
- test point
- start time
- duration
- exit code
- whether it timed out
- the last 50 lines of output

The full output streams into the coordinator log, prefixed `[hook <script>]`.

```json
"hooks": [
  {
    "hookPoint": "beforeDrain",
    "script": "evict-topic-cache.sh",
    "testPointIndex": 1,
    "testPointLabel": "2-consumers",
    "startTime": "2026-10-28T08:33:58.101Z",
    "durationMs": 5121,
    "exitCode": 0,
    "timedOut": false,
    "outputTail": ["dimster-evict-kafka-0-...: evicted 30 files (982 MB): resident 283 MB -> 1 MB", "..."]
  }
]
```

## Reference scripts

`run/hooks/` ships scripts for drain-backlog isolation. Reference them by file name, e.g. `script: evict-topic-cache.sh`. Each one documents its optional settings at the top.

| Script | What it does |
|--------|--------------|
| `evict-topic-cache.sh` | Drops the page cache of the `DIMSTER_TOPICS` files on every broker, so a drain starts cold. For each broker it starts a helper pod on that broker's node that mounts the broker's volume, then flushes and `fadvise(DONTNEED)`s each file. It reports how much was resident before and after. Covers every replica, since leadership can move between drains. Needs no privileges. |
| `rolling-restart-brokers.sh` | Restarts brokers one at a time (reverse ordinal order) with a long grace period. It waits for each broker to be Ready and for no under-replicated partitions before the next. Resets broker JVM state. It does not clear the page cache, so list `evict-topic-cache.sh` after it. |
| `pause.sh` | Waits `args: ["<seconds>"]` (default 60). List it last under `beforeDrain` to let the cluster quieten down after eviction or restarts. |
| `drop-node-caches.sh` | Drops the whole page cache of each broker node with a privileged pod. A blunt fallback: it also empties the cache of anything else on those nodes. |

## See also

- [Drain Backlog Mode: reusing a backlog](../modes/drain-backlog.md#reusing-a-backlog), the main use of hooks today.
- [Architecture](architecture.md)
