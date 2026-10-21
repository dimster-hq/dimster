# Dimster Documentation

## Start here

- [Getting Started](getting-started/README.md) — a hands-on walkthrough that takes you from an empty laptop to your first finished benchmark on a local Kubernetes cluster, then through dimensional testing and the four benchmark modes.

## Alternative paths

- [Running without Kubernetes](getting-started/11-embedded-mode-without-k8s.md) — single-JVM embedded mode against a local broker, for smoke tests and CI integration. Read this only after you've understood the standard k8s flow above.

## Core Concepts

- [Architecture](concepts/architecture.md) — the three roles (CLI, coordinator, workers), what's under test vs what runs the test, and where Kubernetes fits.
- [Kafka Versioning](concepts/kafka-versioning.md) — independent client and broker versions, the supported `kafka_client_version` values, and the build/deploy workflow for switching.
- [Dimensional Testing](concepts/dimensional-testing.md) — campaigns, scenarios, test points, and the co-variance rule that turns one workload file into many runs.
- [Compare command vs dimensions](concepts/comparing-campaigns.md) — when the thing you want to vary is the cluster, not the workload.
- [Results and Charts](concepts/results-and-charts.md) — what lands in your results directory and how the charts get generated.
- [Data Generation](concepts/data-generation.md) — generated datasets (presets, cardinality, skew, clustering, Avro and JSON records) for format, compression experiments.
- [Live Workload Modification](concepts/live-modification.md) — change throughput or end a test point mid-run via `dimster run --modify`. Useful for exploratory throughput discovery without committing to a full Explore campaign.

## Benchmark Modes

- [Run](modes/run.md) — fixed-rate throughput and end-to-end latency.
- [Explore Limits](modes/explore-limits.md) — find max sustainable throughput under a latency SLO.
- [Drain Backlog](modes/drain-backlog.md) — measure consumer catch-up after a backlog.
- [Correctness](modes/correctness.md) — detect data loss, duplicates, and ordering violations.
- [Availability](modes/availability.md) — measure producer/consumer availability during disruption.

## Deployment

- [Overview](deployment/README.md) — when to leave `local` behind and how the per-target pages are organised.
- [01 — Mental model](deployment/01-mental-model.md) — what Dimster expects from any Kubernetes cluster.
- [02 — Deploy modes](deployment/02-deploy-modes.md) — `full` vs `external`.
- [03 — Scheduling and storage](deployment/03-scheduling-and-storage.md) — `nodeLabel` and `pvcStorageClass`.
- [04 — Local](deployment/04-local.md) — the laptop story (pointer back to Getting Started).
- [05 — AWS EKS](deployment/05-aws-eks.md) — production-scale benchmarks via the `eks-setup` wizard.
- [06 — External Kafka](deployment/06-external-kafka.md) — point Dimster workers at Confluent Cloud, MSK, or any other managed Kafka.

## Reference

- [Dimster CLI](reference/cli.md) — every command and flag.
- [EKS Setup CLI](reference/eks-setup-cli.md) — the EKS provisioning helper.
- [`dimster-config.yaml`](reference/dimster-config.md) — env-block schema.
- [Kafka Configs](reference/kafka-configs.md) — broker, producer, consumer settings.
- [Workload Reference](reference/workloads.md) — every field you can put in a workload file.

## Examples

- [Run Workloads](examples/run-workloads.md)
- [Explore Workloads](examples/explore-workloads.md)
- [Drain Backlog Workloads](examples/drain-backlog-workloads.md)
- [Correctness Workloads](examples/correctness-workloads.md)

## Contributing

- [Modifying Dimster itself](development.md) — extra setup if you want to hack on the Java engine or the CLIs.
