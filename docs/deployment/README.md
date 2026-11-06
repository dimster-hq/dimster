# Deploying Dimster

You've finished [Getting Started](../getting-started/) and run benchmarks against a local Kubernetes cluster. This section is for deploying Dimster to anything bigger — a managed Kubernetes service, a real cloud cluster, or an external Kafka you want to benchmark against.

## How to read this

The pages split into two halves: **concepts** (01–03) build the mental model of what Dimster expects from a Kubernetes cluster and how `dimster-config.yaml` maps to k8s resources; **per-target guides** (04–06) are pick-the-one-you-need walkthroughs.

| Page | What it covers |
|------|----------------|
| [01 — Mental model](01-mental-model.md) | What Dimster assumes about your cluster; how `dimster-config.yaml` becomes Kubernetes manifests |
| [02 — Deploy modes](02-deploy-modes.md) | `deploy: full` vs `deploy: external` — when to use each, what each provisions |
| [03 — Scheduling and storage](03-scheduling-and-storage.md) | Why a serious benchmark needs `nodeLabel` and `pvcStorageClass`, and how to use them |
| [04 — Local clusters](04-local.md) | Pointer back to getting-started; when local is enough |
| [05 — AWS EKS](05-aws-eks.md) | End-to-end: provision an EKS cluster with the `eks-setup` wizard, build, deploy, run, plus EC2 instance selection guidance |
| [06 — External Kafka](06-external-kafka.md) | Connecting to Confluent Cloud, MSK, self-managed Kafka, or local Docker Compose |
| [07 — In-cluster mTLS](07-mtls-full-mode.md) | Enable mTLS for `deploy: full` — auto-generated certs, per-workload opt-in, side-by-side TLS vs PLAINTEXT runs |
| [08 — Infrastructure dimensions](08-infra-dimensions.md) | Learn how to include infrastructure in dimensional testing |
| [09 — Emulated network-attached storage](09-emulated-network-storage.md) | Give local brokers gp3-like disk latency, IOPS and throughput caps with dm-delay and cgroup `io.max` |

Read pages 01–03 once. After that, pick the one target page that matches what you're shipping to.

There's also one un-numbered reference page that's optional reading:

| Page | What it covers |
|------|----------------|
| [Internals — what `nodeLabel` and `pvcStorageClass` actually do](internals-scheduling-storage.md) | The exact Kustomize patches, supported/unsupported features. Skip unless you're debugging or extending Dimster. |

## Quick jumps

| You want to... | Go to |
|----------------|-------|
| Benchmark Kafka on AWS EKS using the wizard | [05 — AWS EKS](05-aws-eks.md) |
| Benchmark on GKE / AKS / on-prem | Bring your own cluster — read [03 — Scheduling and storage](03-scheduling-and-storage.md) for the env-block requirements, then refer to your cloud provider's docs for cluster setup |
| Benchmark Confluent Cloud, MSK, or another managed Kafka | [06 — External Kafka](06-external-kafka.md) |
| Benchmark locally against gp3-like disks | [09 — Emulated network-attached storage](09-emulated-network-storage.md) |
| Just smoke-test on a laptop | [Getting Started](../getting-started/) (you don't need this section) |
