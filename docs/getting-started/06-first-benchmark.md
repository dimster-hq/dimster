# 06 — Your first benchmark

Time to actually run something. We'll use the smallest workload Dimster ships with — a 2-minute fixed-rate test against a single topic.

## Run it

`./dimster run <env` runs a fixed throughput workload. A workload file must be provided using `-w` argument. Optionally, a separate Kafka client configuration file can be supplied using the `-k` argument. Kafka client configs can also be embedded into the workload file itself.

```bash
./dimster run local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-run-test.yaml
```

What each flag means:

- `local` — the env to deploy against
- `-k` — Kafka client config (acks, replication factor, commit mode …).
  `kafka_local.yaml` is the simple single-broker variant with `replicationFactor: 1`.
- `-w` — the workload definition (the shape of the workload).

The kafka_local.yaml sets RF=1, and some other Kafka client configs. Any configs beginning with `dimster.` determine how Kafka clients are utilized, such as what type of commit (async or sync).

```yaml
name: kafka-local

replicationFactor: 1

topicConfig:
  min.insync.replicas: 1

classicConsumerConfig:
  auto.offset.reset: earliest
  enable.auto.commit: false
  dimster.commit.mode: CommitAsync  # CommitAsync|CommitSync (the type of commit when enable.auto.commit=false) 
  dimster.commit.linger.ms: 0       # How many ms to defer a commit (0ms means after each non-empty poll)

shareConsumerConfig:
  dimster.commit.mode: CommitAsync  # Poll|CommitAsync|CommitSync (the type of commit)
  dimster.commit.linger.ms: 0       # How many ms to defer a commit (not used with Poll commit mode)
```

The workload file determines how many topics, partitions, producers, consumers, producerRate etc:

```yaml
name: Quick run test

engineConfig:
  perPartitionMetrics: true   # Producer/consumer metrics with partition-level reporting
  perConsumerMetrics: true    # Per-consumer metrics (disable for large consumer counts)

runConfig:
  testDurationMinutes: 2      # The duration of each test point with latency recording
  warmupDurationMinutes: 1    # The initial duration of each test point without latency recording

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  messageDistributor: "NO_KEY"
  messageSize: 500
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 10
  producersPerTopic: 1
  producerRate: 1000

```

## What the workload does

`quick-run-test.yaml` is an intentionally minimal workload:

| Setting          | Value      | Notes                                       |
|------------------|------------|---------------------------------------------|
| Test duration    | 2 minutes  | Plus a 0-minute warmup                      |
| Topics           | 1          |                                             |
| Partitions/topic | 10         |                                             |
| Producers        | 1          | Producing at a fixed rate                   |
| Producer rate    | 1000 msg/s | Steady throughout the test                 |
| Consumer groups  | 1          | Classic consumer group (not share)          |
| Consumers/group  | 10         |                                             |
| Message size     | 500 bytes  |                                             |

> Note that Dimster treats 1KB as 1000 bytes, 1MB as 1000000 bytes and so on. 

You can open the file at `run/workloads/quick-test/quick-run-test.yaml` to see the full schema in action.

## Watching the run

The CLI streams the live coordinator log to your terminal (via kubectl). You can press Ctrl+C to detach from the output and the benchmark will continue to run. You can reattach with `./dimster reattach local`. 

## Open the live dashboards

While a run is in progress (or after, against historical data), port-forward Grafana and Prometheus to localhost:

```bash
./dimster dashboard local
```

This forwards two ports and runs until you press Ctrl+C:

- **Grafana**: <http://localhost:3000>
- **Prometheus**: <http://localhost:9090>

Three pre-provisioned dashboards live under the **Dimster** folder in
Grafana:

- **Client Metrics** — Dimster metrics (publish + end-to-end latency percentiles, message
  and byte rates, errors) and Apache Kafka client metrics.
- **Kafka Broker** — cluster health, throughput, request latency
  breakdown, replication, log flush, JVM memory/GC.
- **System (Linux)** — CPU, memory, disk I/O, filesystem, network (via
  node_exporter).

Use custom ports if 3000/9090 are taken:

```bash
./dimster dashboard local 8080 9191
```

## Where the results live

At the end of a test campaign, the results and pod logs are downloaded into the `./results/` directory (by default). Each run gets its own timestamped folder containing:

- `*.json` — the structured result file (the canonical artefact).
- `logs/coordinator.log`, `worker-*.log`, `kafka-*.log` — full component logs.
- `dashboards/*.html` — Grafana dashboard snapshots rendered at the end of
  the run (great for sharing).
- `prometheus-snapshot/` — only if you asked for it with `metricsOutput: [html, snapshot]` in the environment or `--metrics-output html,snapshot` on the run. It holds the raw Prometheus data behind the dashboards for this run. `./dimster dashboard --snapshot results/<run-dir>` opens it in a local Grafana (needs Docker), even after the cluster is gone.
- `chart/*.html` — a detailed set of charts visualizing the performance results.

Results are downloaded at the end of a test campaign. However, if an error occurred and the campaign halted you can execute `dimster results <env>` to download the results and logs for any test points that did complete. If you pressed Ctrl+C to detach from the workload then stopped the test with `dimster stop <env>` you can also run `dimster results <env>` to download the results and logs.

## If something looks wrong

- A common error when running in a local environment is using a Kafka config file with `replicationFactor: 3` with only a single broker deployed.
