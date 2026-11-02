# Compare command vs dimensions

[Dimensional testing](dimensional-testing.md) handles every comparison where the only thing changing is the workload itself. But some comparisons need the *cluster* to differ — the Kafka version, the broker config, the hardware, the broker count. Those can't be dimensions, because the cluster is built once at deploy time and shared by every test point in a campaign. For those, you run two or more test campaigns on different environments and stitch the results together with the `dimster compare` command. This command takes the results of each test campaign and generates a chart overlaying the results for easy comparison.

## What can't be a dimension

A dimension is anything you can vary inside a workload file. By construction, that excludes anything baked into the cluster at deploy time:

| Thing you want to compare                                            | Why it can't be a dimension |
|----------------------------------------------------------------------|-----------------------------|
| **Broker config** (`num.network.threads`, `log.segment.bytes`, etc.) | Loaded by the broker at startup. Changing it means restarting brokers, which means a new campaign. |
| **Kafka version** (3.7 vs 4.2, Apache Kafka vs a Kafka service)      | The broker image is set per env block, not per test point. |
| **Broker count** (3 vs 5 brokers)                                    | StatefulSet replica count is set at deploy time. |
| **Hardware / instance type**                                         | The node placement and resource requests are part of the env block. |
| **Storage class** (gp3 vs io2, EBS vs instance store)                | PVC binds to a storage class at deploy time. |
| **Auth / network setup**                                             | Listeners and protocols are broker-level config. |

Anything else — partition count, consumer count, message size, batch settings, producer rate, client config keys, scenario/workload structure — can be explore and compared as dimensions in regular test campaigns. If it lives under `baseWorkload` (including `kafkaConfig`) or `scenarios` in the workload schema, it can vary per test point.

## The pattern

When the thing you want to compare lives outside the workload, the workflow is as follows:

1. Define **two or more** env blocks in `dimster-config.yaml` that differ only in the thing you want to compare.
2. Deploy each env separately and run **the same workload** against each. Each run produces its own timestamped results folder with a JSON result file. The timing of the benchmarks does not matter. One could be from today and the other from last year (though running the same version of Dimster is advisable in case there are breaking changes).
3. Run `dimster compare LABEL=path/to/result.json LABEL=path/to/result.json [...]` to generate side-by-side charts.

The compare command is a **chart overlay only** command so it doesn't re-run anything. It also doesn't validate that the workloads match (sometimes you also want to compare different workloads using the compare command).

Drain-backlog results can be compared the same way: drain durations, consumer throughput and disk read fractions as bars, and backlog and consume rate over time per test point.

When the run or drain-backlog results have [resources report](results-and-charts.md#resources-report) data beside them, compare also writes a resources comparison. For each test point, it sets every entry's per-service CPU, CPU throttling, disk and network side by side, along with page cache vs disk and bottlenecks. Per-service figures need the environments to set `monitoring.cgroupMetrics`. For charts over time, open each run's own resources report.

## Worked example — Kafka 3.7 vs 4.2

Goal: see how throughput and latency differ between Kafka 3.7.2 and 4.2.0 on the same hardware, same workload, same broker config.

### 1. Two env blocks differing only in `kafka.version`

```yaml
environments:
  benchmark-3-7:
    image: dimster
    context: my-eks-context
    deploy: full
    kafka:
      replicas: 3
      version: 3.7.2          # only difference
      pvcSize: 500Gi
      heap: -Xms6g -Xmx6g
      requests: { cpu: 4, memory: 16Gi }
      brokerConfig: run/broker-configs/default.yaml
    workers: { replicas: 4, ... }
    coordinator: { ... }

  benchmark-4-2:
    image: dimster
    context: my-eks-context
    deploy: full
    kafka:
      replicas: 3
      version: 4.2.0          # only difference
      pvcSize: 500Gi
      heap: -Xms6g -Xmx6g
      requests: { cpu: 4, memory: 16Gi }
      brokerConfig: run/broker-configs/default.yaml
    workers: { replicas: 4, ... }
    coordinator: { ... }
```

Everything else — replicas, heap, CPU/memory requests, broker config file, worker sizing — is identical. The only delta is the broker image tag.

### 2. Deploy and run the same workload against each

```bash
./dimster build benchmark-3-7
./dimster deploy benchmark-3-7
./dimster run benchmark-3-7 \
  -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/run-test/run-classic-scaling-10-120MBs.yaml
./dimster teardown benchmark-3-7

./dimster build benchmark-4-2
./dimster deploy benchmark-4-2
./dimster run benchmark-4-2 \
  -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/run-test/run-classic-scaling-10-120MBs.yaml
./dimster teardown benchmark-4-2
```

Each run produces its own timestamped folder under `results/`, e.g.:

```
results/2026-04-22-14-12-03/run-classic-scaling-10-120MBs-2026-04-22-14-12-03.json
results/2026-04-23-09-47-15/run-classic-scaling-10-120MBs-2026-04-23-09-47-15.json
```

### 3. Compare them

```bash
./dimster compare \
  3.7.2=results/2026-04-22-14-12-03/run-classic-scaling-10-120MBs-2026-04-22-14-12-03.json \
  4.2.0=results/2026-04-23-09-47-15/run-classic-scaling-10-120MBs-2026-04-23-09-47-15.json
```

The labels (`3.7.2`, `4.2.0`) become the series names on the comparison charts. Output charts default to the directory of the first file; pass `--output <dir>` to write them somewhere else.

The same pattern works for any cluster-level comparison: swap the broker config, swap the instance type, swap the broker count, swap the storage class. One env block per variant, same workload, then `dimster compare`.

## Where to go next

- [Dimensional Testing](dimensional-testing.md) — the workload-file model that compare composes with.
- [Results and Charts](results-and-charts.md) — what's in the JSON result files compare consumes.
- [CLI Reference](../reference/cli.md) — every `dimster compare` flag.
