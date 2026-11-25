# 01 — What Dimster expects from Kubernetes

The whole deployment story fits on one slide: **Dimster assumes nothing about your Kubernetes cluster except that you have a working `kubectl` context against it**. Everything else is configuration in `dimster-config.yaml`.

That includes cluster sizing, storage classes, image registries, node placement, and external Kafka credentials. None of it is hard-coded; all of it lives in the named env block you pass to `dimster <command> <env>`.

## The flow, end to end

```
┌────────────────────────────┐
│   dimster-config.yaml      │
│   environments:            │
│     myEnv:                 │
│       image: ...           │
│       context: ...         │
│       deploy: full         │
│       kafka: {...}         │
│       workers: {...}       │
└────────────┬───────────────┘
             │  dimster deploy myEnv
             ▼
┌────────────────────────────┐
│   dimster CLI (Rust)       │
│   1. Reads env block       │
│   2. Copies base manifests │
│      from run/deploy/      │
│      kubernetes/base/      │
│   3. Generates Kustomize   │
│      patches from env      │
│      fields                │
│   4. Writes a temp overlay │
└────────────┬───────────────┘
             │  kubectl apply -k <overlay>
             ▼
┌────────────────────────────┐
│   Your Kubernetes cluster  │
│   (whatever it happens to  │
│    be: EKS, GKE, AKS,      │
│    minikube, on-prem)      │
└────────────────────────────┘
```

There is no helm chart, no operator, no per-cloud provisioner. Just base YAML manifests + Kustomize patches + `kubectl apply`. Anything Kubernetes can run, Dimster can deploy to.

## What Dimster *does* manage

For each named env, the CLI deploys some subset of these workloads into the configured namespace (default: `dimster`):

- **Kafka StatefulSet** — only when `deploy: full`.
- **Worker Deployment** — runs producers and consumers, always deployed.
- **Coordinator Deployment** — orchestrates each benchmark, owns the result PVC, always deployed.
- **Monitoring** — Prometheus + Grafana + node-exporter, always deployed.

That's the entire shopping list. Per env, the CLI sizes each of these (replicas, JVM heap, CPU/memory requests), optionally pins them to specific nodes, and points the workers at either an in-cluster Kafka StatefulSet (`full` mode) or an external bootstrap server (`external` mode). Page 02 covers that mode split in detail. Keeping the heap sizing and k8s requests(CPU/memory) together in one place makes it easier to size your benchmark setup coherently.

## What Dimster *doesn't* manage

These are all on you (or on your platform team):

- **Cluster creation** — minikube/k3d/kind on your laptop, EKS via `eks-setup` (page 05), GKE via `gke-setup` (page 11), AKS/on-prem by hand (refer to your cloud provider's docs).
- **IAM, networking, DNS** — security groups, VPC peering, IRSA roles, ingress, DNS.
- **Image registry credentials** — Dimster pushes images to whatever registry you point `image:` at, but you handle the auth (e.g. `aws ecr get-login-password`, `gcloud auth configure-docker`).
- **Storage class definitions** — Dimster *uses* a storage class via `kafka.pvcStorageClass`, but you create the StorageClass object in the cluster yourself.
- **Node labels** — Dimster *requires* labelled nodes when you set `kafka.nodeLabel: role=kafka`, but the labels themselves come from your cluster's nodegroup configuration.

The deliberate split keeps Dimster cluster-agnostic. The downside is that "deploy to a real cluster" is a few more steps than "deploy to a real cluster on the one cloud the tool happens to know about". The upside is that any of those clouds work the same way.

## The four shapes of env block

Run `dimster init` and you get one active env (`local`) plus four commented templates that illustrate the four shapes you'll ever need:

| Template name | k8s | Deploy mode | Use case |
|---------------|-----|-------------|----------|
| `local` (active) | local distro | full (default) | Smoke tests on your laptop |
| `exampleLocalFull` | local distro | full | Local with explicit sizing |
| `exampleLocalExt` | local distro | external | Local clients + Docker Compose Kafka |
| `exampleRemoteFull` | remote (EKS / GKE / on-prem) | full | Production-shape benchmark |
| `exampleRemoteExt` | remote | external | Benchmarking Confluent Cloud, MSK, etc. |

The four `example*` blocks live in `cli/src/commands/init.rs`. Open `dimster-config.yaml` after `dimster init` and they're all there as commented YAML — copy one, uncomment, edit. Every per-target page in this section ultimately produces an env block of one of these four shapes.

## Where the field reference lives

This page is a tour of the model; it deliberately doesn't enumerate every field. For the full schema:

- The Rust struct definitions in `cli/src/config.rs` are the source of truth.
- [`dimster-config.yaml` reference](../reference/dimster-config.md) covers every field with examples.

Pages 02 and 03 in this section walk through the two clusters of fields that actually shape the deployment: deploy mode, and scheduling + storage.

## What's next

- [02 — Deploy modes](02-deploy-modes.md): the `full` vs `external` split.
- [03 — Scheduling and storage](03-scheduling-and-storage.md): `nodeLabel` and `pvcStorageClass`, the two fields that turn a "works on my laptop" deployment into one whose results you'd publish.
