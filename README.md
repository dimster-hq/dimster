# Dimster — DIMensional performance teSTER for Apache Kafka

A dimensional benchmarking framework for Apache Kafka. Define many configurations in one workload file, run them as a single campaign, and get reproducible, shareable artifacts.

> A *dimension* is any workload aspect you'd want to vary across runs such as partition count, consumer count, consumer type, message size, batch size, producer rate, client settings. Pick one or more and Dimster (co-)varies them across a single campaign, with results landing side-by-side in charts for comparison.

Dimster's core design principle is making it easy to compare performance across multiple configurations in a single run. Instead of manually running separate benchmarks for each variation, you define all variations in one workload file.

All results are shareable assets:
- Detailed results in JSON and CSV format which includes:
    - producer/consumer throughput and various latencies
    - per-partition and per-consumer latencies
    - the hardware configuration (for reproducibility)
- Configuration:
    - Broker version and configuration.
    - The workload, Kafka client configs.
- Charts as a single interactive html file
- Logs of all Kafka brokers, workers and the coordinator
- Grafana dashboards converted to HTML files
- Optionally, a Prometheus snapshot of the run to reopen in Grafana later, without the cluster

Latency charts examples:

![e2e latency ](./docs/images/cg-vs-sg-50mbs-e2e-dist.png)

*Fig 1. End-to-end latencies distribution*

![e2e latency ](./docs/images/cg-vs-sg-50mbs-e2e-over-time.png)

*Fig 2. End-to-end latencies over time*

Run fully automated test suites or use interactive mode to shape the workload as it runs.

> **Status: 0.x — early, expect change.** Workload schema, CLI flags, and deployment manifests may shift between minor versions. See [CHANGELOG.md](CHANGELOG.md) for what's changed. Always rebuild the CLI and Dimster framework after a `git pull`.

## What can I ask it?

A Dimster test campaign is formed from one or more scenarios, where each scenario consists of one or more test points which varies a selected dimension or dimensions. Each scenario generates a set of charts where the test points are the data series. Use this format to ask questions.

### "How does end-to-end latency change as I scale share consumers from 1 to 50?"

`dimster run <env> -w workload.yaml` executes the test points.

```yaml
baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  producersPerTopic: 5
  producerRate: 50000
  messageSize: 500
  consumerType: SHARE_GROUP
  
scenarios:
  - name: scaling-consumers
    label:             ["1-con", "10-con", "20-con", "30-con", "40-con", "50-con"]
    consumersPerGroup: [1,        10,       20,       30,       40,       50]
```

One set of charts with 6 data series.

### "How does linger.ms and batch.size impact throughput and latency?"

One test campaign with two scenarios exploring `batch.size` as a dimension and then `linger.ms` as a dimension.

```yaml
# baseWorkload ...

scenarios:
  - name: batch-size
    label: ["batch-8k", "batch-16k", "batch-32k", "batch-64k", "batch-128k", "batch-256k"]
    kafkaConfig:
      classicConsumerConfig:
        linger.ms: 100
        batch.size: [8192, 16384, 32768, 65536, 131072, 262144]
        
  - name: linger-ms
    label: ["linger-0ms", "linger-50ms", "linger-100ms"]
    kafkaConfig:
      producerConfig:
        linger.ms: [0, 50, 100]
```

Two sets of charts with 6 and 3 data series.

### "How do CONSUMER_GROUP consumers compare to SHARE_GROUP at three throughput levels?"

```yaml
scenarios:
  - name: 50MBs
    producerRate: 50000
    label:        ["classic",      "share"]
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]
  - name: 100MBs
    producerRate: 100000
    label:        ["classic",      "share"]
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]
  - name: 200MBs
    producerRate: 200000
    label:        ["classic", "share"]
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]
```

Three sets of charts with two data series.

###  "What's the highest throughput my cluster sustains under p75 ≤ 50 ms?"

`dimster explore` ramps the rate, binary-searches for the breaking point, then sustains the discovered ceiling. Create multiple scenarios and test points, fully automated.

![Sustainable throughput](./docs/images/explore-sharegroup-1-part-throughput.png)

*Fig 3. Peak sustainable throughput on a single partition as consumers scale out*


### "Does `acks=1` actually lose messages under broker churn?"

`dimster correctness` produces sequentially-keyed payloads and checks every one for loss, duplication, ordering, and CRC integrity — pass/fail with per-detector counts.

### How fast can my consumers drain a backlog under load?

`dimster drain-backlog` builds a backlog then starts consumers to time how long it takes to drain it. Optionally configure a producer load while the drain takes place.

![Backlog drain](./docs/images/backlog-drain-under-load.png)

*Fig 4. Backlog drain under load*

### What hardware requirements in terms of network and disk bandwidth does my benchmark require?

`dimster resources` takes your workload file and tells you the amount of resources needed to run it.

```
./dimster resources -b 3 -w run/workloads/run-test/run-200MBs-classic-vs-share.yaml

=== Workload: 200MBs-classic-vs-share (run) ===

  Scenario: 200-MB/s (RF=3)

  LABEL                     INGRESS       REP IN      REP OUT     CONS OUT       NET IN      NET OUT    NET TOTAL      DISK WR
  consumer-group         200.0 MB/s   400.0 MB/s   400.0 MB/s   200.0 MB/s   600.0 MB/s   600.0 MB/s     1.2 GB/s   600.0 MB/s
  share-group            200.0 MB/s   400.0 MB/s   400.0 MB/s   200.0 MB/s   600.0 MB/s   600.0 MB/s     1.2 GB/s   600.0 MB/s

  Per broker (3 brokers):
  LABEL                     INGRESS       REP IN      REP OUT     CONS OUT       NET IN      NET OUT    NET TOTAL      DISK WR
  consumer-group          66.7 MB/s   133.3 MB/s   133.3 MB/s    66.7 MB/s   200.0 MB/s   200.0 MB/s   400.0 MB/s   200.0 MB/s
  share-group             66.7 MB/s   133.3 MB/s   133.3 MB/s    66.7 MB/s   200.0 MB/s   200.0 MB/s   400.0 MB/s   200.0 MB/s

  Peak: with 3 brokers, each broker must handle up to 400.0 MB/s network and 200.0 MB/s disk write
  Peak aggregate: 1.2 GB/s network total, 600.0 MB/s disk write total

  Note: per-broker numbers assume uniform load distribution and do not account
  for increased load when brokers are offline. This is not a capacity planning tool.
```

## What a campaign produces

![Dimster result dir](./docs/images/dimster-results.png)

*Fig 5. A result directory*

Every run writes a single timestamped folder containing everything you need to reproduce, share, or publish the result:

- **JSON and CSV results** — throughput and latency percentiles for every test point.
- **Captured config** — the broker config, the client configs, the workload file, the hardware shape (instance type, CPU, memory, storage class). No "what version was that on?" later.
- **Charts** — interactive HTML, all test points overlaid.
- **Grafana snapshots** — broker, client, and system dashboards exported to static HTML at the moment the run finished.
- **Prometheus snapshot** (opt-in) — the raw metrics behind those dashboards, reopened in a local Grafana with `dimster dashboard --snapshot`.
- **Logs** — coordinator, every worker, every Kafka broker.

If you can ship the folder, someone else can read the result. If you have the workload YAML, you can re-run it.

## Version & compatibility

Choose the broker version and the client versions. All versions 3.0+ are supported.

## Getting started

Run some small-scale tests locally on your k8s distro of choice (minikube, k3d, kind, orbstack etc): **[docs/getting-started/](docs/getting-started/README.md)**.

## Documentation

Full docs: **[docs/index.md](docs/index.md)**.

Highlights:
- [Getting Started](docs/getting-started/README.md) — 10-page walkthrough from zero to dimensional sweeps.
- [Architecture](docs/concepts/architecture.md) — CLI, coordinator, workers; what Kubernetes does and doesn't do for you.
- [Dimensional Testing](docs/concepts/dimensional-testing.md) — the mental model behind workload files.
- [Deployment](docs/deployment/README.md) — what Dimster expects from any cluster; per-target guides.
- [CLI Reference](docs/reference/cli.md) · [`dimster-config.yaml`](docs/reference/dimster-config.md) · [Workload Reference](docs/reference/workloads.md).

## AI assisted development and inspiration

Dimster has been developed with a mix of by-hand coding and Claude Code. The Java-based benchmark framework code has had a lot of oversight and review of AI-generated code, with the CLI, k8s manifests and charting being primarily delegated to Claude. Claude wrote most of the docs with human edits, you may find some em-dashes in there sorry!

While Claude assisted in development, Dimster has been run extensively by humans and users should find it to be very reliable. Open an issue if you encounter any problems.

The benchmark framework design is heavily inspired by [OpenMessaging Benchmark](https://github.com/openmessaging/benchmark), but diverges on a few things:

- **Kafka-centric**, built as an in-house perf tool at Confluent for Kafka testing. For other systems, use OpenMessaging Benchmark.
- **Dimensional testing as a first-class primitive**.
- **Reproducible artifact bundles** — every run captures enough config and context to re-run or audit.
- **Kubernetes-centric** — Run benchmarks on any cloud, any hardware, with k8s as the standardized runtime.

## Acknowledgements

A handful of utility classes in Dimster were originally copied from OpenMessaging Benchmark (Apache 2.0). Some remain near-identical; others have been heavily modified. See [`NOTICE`](NOTICE) for the per-file breakdown and the relevant license terms.

## Contributing

If you want to hack on the Java engine, the Rust CLIs, or the Kubernetes manifests, see [docs/development.md](docs/development.md). For feature ideas or larger changes, open an issue first.
