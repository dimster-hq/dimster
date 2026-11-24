# 03 — Scheduling and storage

The two fields covered on this page — `nodeLabel` and `pvcStorageClass` — are required for any serious benchmarking (`pvcStorageClass` can be omitted if the cluster's default storage class is good enough). Skip this page if you're only smoke-testing on a laptop. Read it before you stand anything up on a real cluster.

> **Easy on-ramp for AWS and GCP users.** If you're benchmarking on AWS EKS or GCP GKE, the [`eks-setup`](../../cloud/eks-cli/README.md) and [`gke-setup`](../../cloud/gke-cli/README.md) wizards set both of these fields for you — they provision labelled node groups/pools, define a tuned `gp3` / Hyperdisk / Persistent Disk / Local NVMe `StorageClass`, and write the right env block (see [05 — Cloud Kubernetes](05-cloud-k8s.md)). You can read this page for understanding, but you won't have to manually do any of what it describes. For AKS / on-prem there's no equivalent wizard — refer to your cloud provider's docs for nodegroup labelling and StorageClass setup, then plug the resulting names into the env block as shown below.

## The problem

By default, the Kubernetes scheduler puts each pod wherever it fits. On a multi-node cluster, that means:

- A Kafka broker can land on a small, network-constrained node.
- A Kafka broker can share a node with a worker pod, so producer/consumer load competes with broker CPU and the kernel page cache.
- Two brokers can land on the same node, so a single node failure takes out two replicas.
- The Kafka PVC binds to whichever default storage class the cluster ships with — typically `gp2` on EKS, `standard` on GKE, `default` on AKS. None of those are the right shape for Kafka.

Any of these is enough to make your numbers meaningless. You can't compare two broker configs if one run happened to land on a fast node and the other didn't.

The fix is two env-block fields: `nodeLabel` (which nodes a component can run on) and `pvcStorageClass` (which storage class the broker PVCs use).

## Node affinity via `nodeLabel`

Each component block accepts a `nodeLabel` field, formatted `key=value`:

```yaml
environments:
  myEks:
    kafka:
      nodeLabel: role=kafka
    workers:
      nodeLabel: role=worker
    coordinator:
      nodeLabel: role=infra
    monitoring:
      nodeLabel: role=infra
```

If no node carries the matching label, the pod stays `Pending` forever — there is no soft fallback. That's deliberate: silently scheduling Kafka onto the wrong node is the failure mode this field exists to prevent.

You apply the labels to your cluster's nodes — typically by configuring nodegroups to label nodes at boot. On AWS and GCP, the `eks-setup` and `gke-setup` wizards handle this for you (see [page 05](05-cloud-k8s.md)). On other clouds, see your provider's nodegroup-labelling docs (`gcloud container node-pools create --node-labels=...`, `az aks nodepool add --labels ...`, or `kubectl label node` for static clusters).

### Recommended pattern

For a serious benchmark, use **three nodegroups** with three labels:

| Label | What runs there | What this gives you |
|-------|-----------------|---------------------|
| `role=kafka` | Kafka brokers | Brokers isolated from client load; broker count == nodegroup size, so you can size by changing one number |
| `role=worker` | Workers (producers/consumers) | Client load isolated from brokers; can scale independently |
| `role=infra` | Coordinator, Prometheus, Grafana | Small cheap nodes; benchmark control plane out of the way |

This pattern matches the `exampleRemoteFull` template in `cli/src/commands/init.rs`, and it's what `eks-setup` produces by default.

## Storage classes via `pvcStorageClass`

Only the Kafka block carries a storage field — workers and coordinator are stateless.

```yaml
environments:
  myEks:
    kafka:
      pvcSize: "500Gi"
      pvcStorageClass: gp3-fast
```

Each broker pod gets its own PVC, so for `replicas: 3, pvcSize: 500Gi` you provision 1.5 TiB total, all on the named StorageClass.

### Why you almost always need to set it

Default StorageClasses on managed clusters are sized for general workloads, not for Kafka:

| Cluster | Default class | What you actually want |
|---------|---------------|------------------------|
| EKS | `gp2` (3 IOPS/GiB, capped 16k IOPS, 250 MB/s burst) | A `gp3` class with explicit IOPS/throughput, or `io2` for very high IOPS |
| GKE | `standard` (HDD!) | `premium-rwo` (SSD), or a tuned `pd-ssd` / `hyperdisk-balanced` class |
| AKS | `default` (HDD) | `managed-csi-premium` (SSD), or Premium SSD v2 |

You define the StorageClass once in your cluster (StorageClass is cluster-scoped, not per-namespace), then reference it by name from every env that targets that cluster.

On AWS EKS, the `eks-setup` wizard creates a `gp3` (or `gp3-fast`) StorageClass for you — see [cloud/eks-cli/README.md](../../cloud/eks-cli/README.md). On GCP GKE, the `gke-setup` wizard creates a tuned Hyperdisk, `pd-ssd`, `pd-balanced`, or `local-storage` (Local NVMe SSD) StorageClass — see [cloud/gke-cli/README.md](../../cloud/gke-cli/README.md). On other clouds (e.g. AKS `managed-csi-premium` or Premium SSD v2), apply the StorageClass YAML once with `kubectl apply -f`, then set `pvcStorageClass:` to its name.

### NVMe instances

If you're benchmarking on instances with local NVMe (i3, i3en, i4i, im4gn on AWS; equivalents elsewhere), set `pvcStorageClass: local-storage`. This is the most performant option but adds operational work — you provision the PVs yourself. See the [internals page](internals-scheduling-storage.md#local-storage-storageclass-nvme-instances) for what the CLI does in this mode.

## When you can skip these

The only legitimate "skip both" case is **a single-node local cluster** running smoke tests:

- One node means scheduler choice is moot — there's nowhere else for pods to land.
- Default storage on a laptop is fine because you're not measuring anything publishable.

This is exactly the `local` env shape from getting-started: no `nodeLabel`, no `pvcStorageClass`, default everything. Page 04 covers it.

## When you can't skip these

The cutoff is approximately "a multi-node cluster with results you'd share." Specifically:

- Any AWS / GCP / Azure cluster with more than one node — set both fields.
- Any benchmark whose numbers will end up in a graph someone else looks at — set both fields.
- Any "Kafka X vs Kafka Y" comparison — set both fields, *and* keep them identical between the two runs.

The cost of setting them is one nodegroup-config commit and one `StorageClass` YAML — or zero work if you use `eks-setup`. The cost of *not* setting them is a benchmark you can't trust.

## What's next

- [04 — Local](04-local.md): when a single-node cluster is genuinely fine.
- [05 — Cloud Kubernetes](05-cloud-k8s.md): the `eks-setup` and `gke-setup` wizards set both fields for you.
- [Internals — what `nodeLabel` and `pvcStorageClass` actually do](internals-scheduling-storage.md): exact Kustomize patches, supported/unsupported features, code references. Optional reading.
