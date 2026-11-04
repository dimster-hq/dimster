# 04 — Local clusters

Local-cluster deployment is fully covered by the [Getting Started](../getting-started/) section. There's nothing extra in this chapter to read — this page is a pointer back to those pages plus a single decision: minimal env (like the starter `local`) or your own beefier env with explicit sizing?

## The two starting points

| Env | When to use | Where it's documented |
|-----|-------------|-----------------------|
| `local` (minimal) | Smoke tests, learning the CLI, CI runs that just need to prove the workload parses and the binary runs | [Getting Started 03 — Start a local Kubernetes](../getting-started/03-start-local-k8s.md) |
| `localBeefy` (you define) | Pushing more representative load locally, multi-broker tests on a laptop with headroom (≥8 CPU, ≥16 GiB free) | [Getting Started 10 — A beefier local env](../getting-started/10-localBeefy-env.md) |

## When local is enough

- Verifying a workload YAML parses and a benchmark completes end-to-end before shipping it to a real cluster.
- Reproducing a bug that doesn't depend on broker hardware (e.g. workload-generation logic, coordinator behaviour, dashboard rendering).
- CI smoke tests.
- Learning Dimster.

## When local is not enough

- Anything where the *number that comes out* matters — local default storage and shared-with-everything CPU mean throughput and latency figures are noise.
- Anything that needs more than ~3 brokers' worth of memory.
- Anything that wants to test storage-class behaviour, or instance-type-specific behaviour. None of those exist on a laptop. (For disk latency and IOPS/throughput caps alone, [09 — Emulated network-attached storage](09-emulated-network-storage.md) gets close on a Linux machine or in Colima.)
- Comparisons between two configurations — even if your laptop is fast, background processes (browser, video calls, OS updates) make run-to-run variance dominate the signal.

If any of those describe what you're trying to do, jump straight to [05 — AWS EKS](05-aws-eks.md), or — if you're on GKE / AKS / on-prem — bring your own cluster, label its nodegroups, define a StorageClass, and use the same env-block shape as the `exampleRemoteFull` template.

## What's next

- [05 — AWS EKS](05-aws-eks.md) — the deepest first-class cloud path, with the `eks-setup` wizard.
- [06 — External Kafka](06-external-kafka.md) — point local workers at a managed Kafka service if you only want to benchmark client-side behaviour without standing up brokers.
