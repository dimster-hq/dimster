# 05 — Cloud Kubernetes (AWS EKS, GCP GKE, AKS & others)

When you're ready for production-scale benchmarks where throughput and latency numbers actually matter, deploy Dimster onto a multi-node cloud Kubernetes cluster.

Dimster ships first-class provisioning wizards under `cloud/` for **AWS EKS** (`eks-setup`) and **GCP GKE** (`gke-setup`). Both provision a container registry, a single-zone/single-AZ cluster with isolated node pools (`role=kafka`, `role=worker`, `role=infra`), a tuned Kafka `StorageClass` (or Local NVMe SSD static PVs), and a ready-to-use env block in `dimster-config.yaml`.

## Choose your cloud

| Platform | Setup CLI | Detailed guide | CLI reference |
|----------|-----------|----------------|---------------|
| **AWS EKS** | `./eks-setup` (`cloud/eks-cli/`) | [cloud/eks-cli/README.md](../../cloud/eks-cli/README.md) | [eks-setup-cli.md](../reference/eks-setup-cli.md) |
| **GCP GKE** | `./gke-setup` (`cloud/gke-cli/`) | [cloud/gke-cli/README.md](../../cloud/gke-cli/README.md) | [gke-setup-cli.md](../reference/gke-setup-cli.md) |
| **Azure AKS / other k8s / on-prem** | Bring your own cluster | [03 — Scheduling and storage](03-scheduling-and-storage.md) | [dimster-config.md](../reference/dimster-config.md) |

Both wizards also support `--answers <file>` for deterministic replay, `--dry-run` for previewing commands and manifests without changing anything, and `--generate-script` for emitting a standalone bash script you can inspect or hand to IaC.

## Shared cluster architecture

Regardless of cloud provider, every Dimster cloud benchmark cluster follows the same topology:

- **Single availability zone.** Every node pool sits in one zone so broker replication and producer/consumer traffic stay inside a single AZ — eliminating cross-zone egress fees and cross-zone latency variance.
- **Three isolated node pools.** Hard node affinity separates Kafka brokers (`role=kafka`), benchmark workers (`role=worker`), and coordinator + Prometheus + Grafana (`role=infra`) so client load and monitoring never compete with broker CPU, network, or page cache.
- **Tuned Kafka storage.** Each broker pod claims its own PVC backed by an explicit, benchmark-grade `StorageClass` (`gp3` / `gp3-fast` on EKS; `hyperdisk-balanced`, `hyperdisk-extreme`, `hyperdisk-throughput`, `pd-ssd`, or `pd-balanced` on GKE) or local NVMe SSDs (`local-storage`).

## High-level workflow

1. **Estimate demand first.** Run `./dimster resources` against your target workload to calculate per-broker network and disk write requirements before picking machine types and disks.
2. **Build and run the setup wizard** for your cloud (see [AWS EKS guide](../../cloud/eks-cli/README.md) or [GCP GKE guide](../../cloud/gke-cli/README.md)). Both wizards read curated hardware catalogs (`cloud/*/instance-types.yaml`) to suggest heap/CPU/memory defaults and catch hardware ceilings up front.
3. **Authenticate Docker, build, deploy, and run.** Log into your cloud container registry (ECR or Artifact Registry), then run the standard Dimster loop:

```bash
./dimster build <env>
./dimster deploy <env>
./dimster run <env> -k <kafka-config> -w <workload>
./dimster dashboard <env>
```

4. **Tear down when done.** Use `./dimster teardown <env>` to remove only the Dimster workload while keeping the cluster warm for the next run, or `./eks-setup delete` / `./gke-setup delete` to delete the cluster, disks, and container registry when you're finished.

## Bring-your-own Kubernetes (AKS / on-prem / custom)

If you're targeting AKS, self-managed Kubernetes, or an existing cluster without a setup wizard:

1. Create three node pools in a single zone and label them `role=kafka`, `role=worker`, and `role=infra`.
2. Define a Kafka-appropriate `StorageClass` (for example `managed-csi-premium` or Premium SSD v2 on AKS, or `local-storage` for local NVMe).
3. Copy the `exampleRemoteFull` block from `dimster-config.yaml.example`, point `image:` at your container registry and `context:` at your `kubectl` context, and set `nodeLabel:` and `pvcStorageClass:`.

See [03 — Scheduling and storage](03-scheduling-and-storage.md) for the exact env-block requirements.

## What's next

- [AWS EKS setup guide](../../cloud/eks-cli/README.md) — full walkthrough, EC2 baseline-vs-burst guidance, EBS vs NVMe sizing, and troubleshooting.
- [GCP GKE setup guide](../../cloud/gke-cli/README.md) — full walkthrough, IAM helper script, live quota & hourly cost estimation, Hyperdisk vs Local NVMe SSD tuning, and troubleshooting.
- [06 — External Kafka](06-external-kafka.md) — if you chose `deploy: external` to benchmark Confluent Cloud, MSK, or another managed Kafka.
- [Internals — what `nodeLabel` and `pvcStorageClass` actually do](internals-scheduling-storage.md) — exact Kustomize patches and supported/unsupported features.
