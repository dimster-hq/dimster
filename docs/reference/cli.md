# Dimster CLI Reference

The `dimster` CLI manages the full lifecycle of Dimster benchmarks: building, deploying, running, monitoring, and collecting results.

## Usage

Compile the CLI and place the binary in the project root.

```bash
cd cli && cargo build --release && cp target/release/dimster .. && cd ..
```
Use `--help` to discover the list of commands


```bash
./dimster --help
```

Many commands are targeted at a named environment in the `dimster-config.yaml`. Run `dimster init` to create a `dimster-config.yaml` with example environments. See [dimster-config.yaml reference](dimster-config.md) for the full schema.

---

## Commands

### init

Initialize `dimster-config.yaml` with a starter set of environments.

```
dimster init [--local-k8s-distro <distro>]
```

Creates `dimster-config.yaml` in the project root, populated with:

- `local` -- active minimal environment (no kafka/workers/coordinator sections, so deploy uses tiny built-in defaults), pre-filled for the chosen `--local-k8s-distro` (defaults to `minikube`).
- `exampleLocalFull` -- commented example: chosen local k8s distro + Kafka deployed in-cluster.
- `exampleLocalExt` -- commented example: chosen local k8s distro + external Kafka (the host hostname in `bootstrap.servers` is set to the chosen distro's convention — `host.minikube.internal`, `host.k3d.internal`, or `host.docker.internal`).
- `exampleRemoteFull` -- commented example: remote Kubernetes (EKS/GKE/etc.) + Kafka deployed in-cluster.
- `exampleRemoteExt` -- commented example: remote Kubernetes + external Kafka (Confluent Cloud, MSK, etc.).

Uncomment an example block and edit the values to suit your cluster.

Refuses to overwrite an existing `dimster-config.yaml` -- delete or rename the file to generate a fresh one.

---

### build

Build the project and Docker image and load into the k8s cluster identified in the environment.

```
dimster build [env] [--in-docker]
```

| Argument / Flag | Required | Description                                                                                                       |
|-----------------|----------|-------------------------------------------------------------------------------------------------------------------|
| `env` | Yes      | Environment name. Builds Java, the Docker image, and pushes/loads the image for that environment.                 |
| `--in-docker` | No       | Run the Maven build inside Docker instead of on the host. Default is host-build, but requires Java 25 on the host |

---

### deploy

Deploy Kafka, workers, coordinator, and monitoring stack to Kubernetes.

```
dimster deploy <env> 
```

Generates a dynamic Kustomize overlay from the environment configuration, applies it to the Kubernetes cluster, and waits for all pods to become ready. In `external` deploy mode, Kafka pods are not deployed.

Use the `--env-override` argument to override specific environment field values. For example:

```bash
./dimster deploy myEnv --env-override kafka.replicas=5
```

---

### All engine commands (run, explore, drain-backlog, correctness)

| Argument / Flag | Required | Description                                                                                                            |
|-----------------|----------|------------------------------------------------------------------------------------------------------------------------|
| `-m`, `--metadata` | Optional | Pass arbitrary metadata to pass through to the result JSON. Additionally `-m series=val -m instance=val` are special values for infrastructure dimensions |
| `-d`, `--extra-dimension` | Optional | Pass infrastructure dimensions, as pass-through metadata that lands in the result JSON file |
| `--metrics-output <outputs>` | Optional | Metrics to save after the run: a comma-separated list of `html` and `snapshot`, or `none`. Overrides the environment's `metricsOutput` (default `html`). Also accepted by `resume` and `reattach`. See [results](#results). |

### run

Run a standard benchmark.

```
dimster run <env> -w <workload> [options]
```

| Argument / Flag | Required | Description                                                                                                            |
|-----------------|----------|------------------------------------------------------------------------------------------------------------------------|
| `env` | Yes | Environment name (e.g. `local`, `aws`).                                                                                |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if the config is embedded in the workload.                                         |
| `-w`, `--workload <path>` | Conditional | Path to workload YAML. Can be specified multiple times for sequential runs. Required for a normal run; not required (and ignored if supplied) when `--modify` is used. |
| `--modify` | No | Modify a running benchmark instead of starting a new one. Sends a side-channel HTTP request to the coordinator to change parameters on the in-progress test point. Must be combined with at least one `--set` override or action flag (see below). |
| `--set <key>=<value> ...` | No | (with `--modify`) Set workload or Kafka config fields using `key=value` syntax. Multiple key=value pairs can follow a single `--set` flag. Workload fields use camelCase (e.g. `producerRate=50000`, `consumersPerGroup=8`). Kafka config uses dotted paths (e.g. `kafkaConfig.producerConfig.batch.size=65536`). Example: `--set producerRate=1000 kafkaConfig.producerConfig.batch.size=65536`. |
| `--terminate` | No | (with `--modify`) End the in-progress test point as if its duration limit had been reached, advancing the run to the next test point. Distinct from `dimster stop` (which ends the whole run). Must be sent alone. |
| `--skip-phase` | No | (with `--modify`) End the current ramp-up or warm-up phase and proceed straight to the next phase without affecting the test phase. Rejected during test phase (use `--terminate` instead). Must be sent alone. Only action flag accepted during ramp-up. |
| `--discard-stats` | No | (with `--modify`) Wipe per-period stats and cumulative histograms accumulated so far in the current test phase. Effectively converts the elapsed test phase into additional warmup. Test phase only and cannot be combined with field overrides. Often paired with `--run-for-minutes`. |
| `--run-for-minutes <N>` | No | (with `--modify`) Reset the test phase remaining duration to N minutes from the moment the engine processes the request. Stands alone (extends the current test phase) or pairs with `--discard-stats` (wipes stats AND gives a fresh N-minute window). Test phase only. Must be > 0. |
| `--restart-consumers <N>` | No | (with `--modify`) Restart N consumers selected uniformly without replacement from the complete topology, or all consumers if N is at least the total count. In correctness mode, selection uses `consumerRestartSeed` and is accepted only during measured `CONCURRENT` work with dynamic assignment. Must be sent alone. |
| `--restart-producers <N>` | No | (with `--modify`) Restart N producers selected uniformly without replacement from the complete topology, or all producers if N is at least the total count. Run-mode test phase only. Must be sent alone. |

The CLI checks Kafka and coordinator readiness, discovers worker pods, injects `bootstrap.servers` into configs, sends the run request to the coordinator, and streams coordinator logs until the benchmark completes. Results are automatically copied from the coordinator PVC to the local results directory, along with component logs and dashboard snapshots.

When `--modify` is used the CLI takes a much shorter path: no readiness check, no workload staging, just an HTTP POST to the coordinator's `/modify` endpoint. The original `dimster run` window keeps running and tailing the coordinator log; modify invocations exit as soon as the coordinator responds. See [Live Workload Modification](../concepts/live-modification.md) for the full guide and workflow patterns.

---

### explore

Find maximum sustainable throughput under latency constraints using a three-phase algorithm (ramp, search, sustain).

```
dimster explore <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to explore workload YAML. |

---

### correctness

Run correctness checks (duplicate detection, ordering verification, optional CRC).

```
dimster correctness <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to correctness workload YAML. |

---

### drain-backlog

Measure the time to drain a consumer backlog.

```
dimster drain-backlog <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to drain-backlog workload YAML. |

---

### stop

Ctrl+C will detach you from the coordinator output (under the hood it is kubectl following the coordinator pod logs). From there you can gracefully stop a running benchmark.

```
dimster stop <env>
```

Sends a stop request to the coordinator. The running benchmark completes its current test point and shuts down cleanly. To download the results of any completed test points run `./dimster results <env>`

---

### reattach

Reconnect to a coordinator that is currently running (or stopping) a benchmark and start streaming its log output, and -- once the benchmark finishes -- download results to the local machine. Use this after detaching with Ctrl+C, or from a different shell while a long benchmark runs in the background. If the coordinator is already idle, `reattach` simply downloads the latest results.

```
dimster reattach <env>
```

Pressing Ctrl+C while reattached detaches again without affecting the benchmark; the same hint about `dimster reattach` and `dimster stop` is printed.

---

### resume

Restart a benchmark from the coordinator's last checkpoint. Used after a previous `run`/`explore`/`correctness`/`drain-backlog` failed. The coordinator persists its progress in a checkpoint file after each successful test point. If the coordinator fails, `resume` picks up at the next test point with a freshly-discovered worker pod list.

```
dimster resume <env>
```

Refuses to run if the coordinator is currently `RUNNING` or `STOPPING` (use `dimster reattach` or `dimster stop` first), or if no checkpoint is present. After resuming, behaves like `run`: streams coordinator output, downloads results on completion, and preserves the checkpoint for another `resume` if the benchmark fails again.

---

### results

Copy results from the coordinator's PVC to the local machine. Also saves coordinator and worker logs alongside the result files and generates the charts. This is usually unnecessary as everything is copied at the end of a run. However, if you want to finish early, you can detach, stop and then run this `results` command.

```
dimster results <env> [output_dir]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `env` | Yes | Environment name. |
| `output_dir` | No | Local output directory. Defaults to the `results.dir` value from config. |
| `--data` | No | Only download the result JSON files. |
| `--logs` | No | Only download coordinator, worker and Kafka logs. |
| `--metrics-output [outputs]` | No | Only export metrics. With no value, uses the environment's `metricsOutput`; otherwise a comma-separated list of `html` and `snapshot`, or `none`. `--dashboards` is an alias for `--metrics-output html`. |
| `--charts` | No | Only generate charts and CSVs from the local result JSON. |
| `--full` | No | Capture full coordinator/worker log files without the per-run time filter. |

With no step flags, every step runs.

#### Metrics outputs

Which metrics are saved is set per environment with `metricsOutput` in `dimster-config.yaml`, and can be overridden per command with `--metrics-output`:

| Output | Written to | Contents |
|--------|------------|----------|
| `html` | `dashboards/*.html` | Static Chart.js pages of the Grafana dashboards. |
| `snapshot` | `prometheus-snapshot/` | Raw samples of every metric the dashboards query, for the run window (plus 10 minutes either side), as gzipped OpenMetrics, together with the dashboard JSON. View it with `dimster dashboard --snapshot`. |

Leaving `metricsOutput` out means `[html]`; `[]` saves no metrics. A snapshot can be taken after the fact with `dimster results <env> --metrics-output snapshot`, as long as the cluster's Prometheus still holds the run (it keeps 7 days).

---

### csv

Generate CSV files from local JSON result files (no cluster access required). Detects `run` vs. `explore` results from each file's `mode` field and writes the appropriate column set.

```
dimster csv <path> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `path` | Yes | Path to a JSON result file or a directory of JSON files. |
| `-o`, `--output <file>` | No | Output file path. Defaults to writing a `.csv` next to each input `.json`. |

---

### chart

Generate HTML charts from result JSON files.

```
dimster chart [results_dir]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `results_dir` | No | Local results directory. Defaults to the `results.dir` value from config. |

Runs `charts/generate_charts.py` against the results directory. Requires Python with dependencies from `charts/requirements.txt`.

---

### compare

Generate side-by-side comparison charts from two or more compatible `run` or
`correctness` result JSON files.

You can specify a directory (it will recursively search for result JSON files), or pass label=result-file-path.

```
dimster compare <LABEL=FILE> <LABEL=FILE> [LABEL=FILE ...] [-o <output>]
```

| Argument / Flag | Description |
|-----------------|----------|-------------|
| `LABEL=FILE` | A short label and the path to a result JSON file (e.g. `baseline=results/2026-04-20/run.json`). |
| `--directory` | The parent directory of a set of result directories to recursively search |
| `-o`, `--output <dir>` | Output directory for the generated charts. Defaults to the directory of the first input file. |

Runs `charts/generate_compare_charts.py`. Requires Python with dependencies
from `charts/requirements.txt`. Each input file must exist; missing files fail
before the script runs.

For correctness results, the comparison contains aggregate bars for message
loss, duplicates, out-of-order records, and CRC failures. It also contains
cumulative sampled timelines for duplicates, out-of-order records, and CRC
failures. Message loss is shown only as a final aggregate because intermediate
missing-message counts primarily represent consumer lag. Older results fall
back to the timestamped detailed violation list, which may be capped.

Example:

```bash
dimster compare baseline=results/2026-04-15/run.json tuned=results/2026-04-20/run.json -o results/compare/
```

Compare all compatible correctness results found recursively under a directory:

```bash
dimster compare --directory results/correctness-protocols
```

---

### teardown

Remove Dimster resources from Kubernetes.

```
dimster teardown <env> [--only <components>]
```

Deletes the pods and PVCs. The namespace itself is left in place.

| Argument / Flag | Required | Default | Description |
|-----------------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `-o`, `--only <components>` | No | all | Comma-separated component list: `kafka`, `workers`, `coordinator`, `monitoring`. Partial teardown leaves omitted components and the namespace in place. |

Example:

```
dimster teardown local --only kafka,coordinator,workers
```

This removes Kafka, workers, the coordinator, and the results PVC while leaving monitoring running.

---

### dashboard

Port-forward Grafana and Prometheus to localhost for live monitoring, or view a saved run's Prometheus snapshot.

```
dimster dashboard <env> [grafana_port] [prometheus_port]
dimster dashboard --snapshot <result-dir>
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `env` | Yes, unless `--snapshot` | -- | Environment name. |
| `--snapshot <result-dir>` | No | -- | View the `prometheus-snapshot/` saved in a result directory instead of the live cluster. |
| `grafana_port` | No | `3000` | Local port for Grafana. Not used with `--snapshot`. |
| `prometheus_port` | No | `9090` | Local port for Prometheus. Not used with `--snapshot`. |

Runs until interrupted with Ctrl+C.

With `--snapshot`, no cluster is needed, only Docker. The CLI loads the snapshot into a temporary Prometheus container, starts Grafana with the dashboards the snapshot was taken with, and prints a link to each dashboard set to the run's time range. It uses ports 3000 and 9090 when they're free, and otherwise picks free ports, so it can run alongside `dimster dashboard <env>`. Ctrl+C removes the containers and temporary data. Snapshots are saved when `snapshot` is in the environment's `metricsOutput` or passed with `--metrics-output` (see [results](#results)).

---

### logs

View pod logs from cluster components.

```
dimster logs <env> [component] [options]
```

| Argument / Flag | Required | Default | Description |
|-----------------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `component` | No | `worker` | Component to view logs for: `worker`, `coordinator`, or `kafka`. |
| `-f`, `--follow` | No | `false` | Follow (stream) log output. |
| `-p`, `--previous` | No | `false` | Show logs from the previous (crashed) container. |
| `--tail <n>` | No | -- | Show only the last N lines from the end of logs. |

When multiple pods match (multiple workers, mulitple brokers), then kubectl streams all matching pods concurrently and interleaves their output, with --prefix tagging each line with its pod name so you can tell them apart.

---

### download-kafka-logs

Copy Kafka server log files (`*.log`, `*.log.*`) off every running broker pod into a per-pod subdirectory on the local machine. Useful for post-mortem inspection when Grafana/`dimster logs` aren't enough -- the GC log, controller log, and rolled-over server logs are all included. Not useful in `external` deploy mode (no Dimster-managed brokers).

```
dimster download-kafka-logs <env> [-o <output>]
```

| Argument / Flag | Required | Default | Description |
|-----------------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `-o`, `--output <dir>` | No | `.` | Local output directory. One subdirectory is created per broker pod (e.g. `<output>/kafka-0/`). |

Each broker is queried for files under `/opt/kafka/logs` matching `*.log` or `*.log.*`, then copied with `kubectl cp`. Failures on individual files are logged as warnings rather than aborting the download.

---

### health

Check health and readiness of all worker pods.

```
dimster health <env>
```
If a benchmark is in progress, it reports the status of each client (producer/consumer) on each worker pod. Statuses: `PENDING`, `RUNNING`, `FAILED`.

---

### kafka

Run Kafka CLI tools inside a broker pod. Not available in `external` deploy mode.

```
dimster kafka <env> [broker_id] [-- <command>...]
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `broker_id` | No | `0` | Broker pod ordinal number. |
| `command` | No | -- | Command to run (passed after `--`). If omitted, opens an interactive shell. |

Example:

```
dimster kafka aws 0 -- kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### inspect

Print a read-only snapshot of Kafka cluster state by exec'ing into the `kafka-0` broker pod. Not available in `external` deploy mode.

```
dimster inspect <env>
```

Dumps three sections:

- **Topics** -- `kafka-topics.sh --describe` for every topic (partition/replica/ISR layout).
- **Consumer Groups** -- per-group `--describe`, `--members`, and `--state`. Uses `kafka-groups.sh --consumer` when available, since `kafka-consumer-groups.sh --list` can miss classic groups in Kafka 4.x.
- **Share Groups** -- per-group `--describe`, `--members`, and `--state`, when `kafka-share-groups.sh` is present in the broker image.
