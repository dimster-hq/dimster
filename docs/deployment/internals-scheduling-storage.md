# Internals — what `nodeLabel` and `pvcStorageClass` actually do

Reference material for users who want to know exactly how Dimster translates the env-block scheduling and storage fields into Kubernetes manifests, what the CLI does and doesn't support, and a couple of behavioural quirks that aren't obvious from the schema alone.

If you just want to get a benchmark running, [03 — Scheduling and storage](03-scheduling-and-storage.md) is enough — you don't need this page.

## What the CLI renders for `nodeLabel`

Each `nodeLabel: key=value` becomes a Kustomize patch that adds a `nodeAffinity` block to the corresponding StatefulSet/Deployment:

```yaml
spec:
  template:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: role
                    operator: In
                    values: ["kafka"]
```

`requiredDuringSchedulingIgnoredDuringExecution` means: if no node carries the label, the pod stays `Pending` forever. There is no soft fallback. That's deliberate — silently scheduling Kafka onto the wrong node is the failure mode this field exists to prevent.

The patch generation lives in `cli/src/commands/deploy.rs` — search for `nodeAffinity` to find the Kafka, worker, and monitoring/coordinator patches.

## What the CLI renders for `pvcStorageClass`

`pvcStorageClass: gp3-fast` becomes a patch on the Kafka StatefulSet's `volumeClaimTemplates`:

```yaml
spec:
  volumeClaimTemplates:
    - spec:
        storageClassName: gp3-fast
        resources:
          requests:
            storage: 500Gi
```

Each broker pod gets its own PVC, so for `replicas: 3, pvcSize: 500Gi` you provision 1.5 TiB total, all on the named StorageClass. The patch generation lives in `cli/src/commands/deploy.rs` — search for `storageClassName`.

## Coordinator-placement quirk

The coordinator pod follows whichever component label is "closest", in this priority order:

1. If `coordinator.nodeLabel` is set explicitly → that wins.
2. Otherwise, if `monitoring.nodeLabel` is set → the coordinator follows the monitoring label.
3. Otherwise, if `workers.nodeLabel` is set → the coordinator follows the workers label.
4. Otherwise no affinity is applied — the scheduler picks.

This is to keep the coordinator off the broker nodes by default, even if you forget to label it. The logic lives in `cli/src/commands/deploy.rs`.

## Local-storage StorageClass (NVMe instances)

When `pvcStorageClass: local-storage` is set, the CLI runs additional logic to reset any `Released`/`Failed` PVs back to `Available` so the StatefulSet can rebind to them after a teardown/redeploy cycle. Without this, local-storage PVs stay stuck in `Released` and new broker pods sit in `Pending` waiting for a PVC that never binds.

The detection is on the exact string `local-storage` — no other StorageClass name triggers it.

You're still on the hook for everything that lives outside the cluster:

- Creating the `local-storage` StorageClass itself.
- Provisioning the underlying PVs (typically with the [local volume static provisioner](https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner) or by hand).
- Formatting and mounting the NVMe devices on each node.

For most benchmarks, a tuned `gp3` class on EBS-backed instances is the right starting point. Reach for local-storage only when you're hitting EBS bandwidth ceilings.

## What's not supported

The affinity and scheduling support is intentionally narrow. Specifically, **none of the following render to anything**:

- `tolerations` — Dimster doesn't render tolerations, so tainted nodes (e.g. GPU nodes, spot pools with a `NoSchedule` taint) won't accept Dimster pods even with a matching `nodeLabel`.
- `nodeSelector` shorthand — only the full `nodeAffinity.required...` form.
- `preferredDuringSchedulingIgnoredDuringExecution` (soft affinity) — only the hard form.
- Multi-key match expressions (`role=kafka,zone=us-west-2a`) — pick one key/value pair.
- Pod affinity / anti-affinity — you can't say "spread brokers across zones" or "no two brokers on the same node" via Dimster config. Your nodegroup design has to enforce that (one node per AZ, etc.).

If your cluster needs any of the above, you have two options: (a) edit the base manifests under `run/deploy/kubernetes/base/` directly before deploy, or (b) open an issue. Most benchmarks don't need any of this.

## See also

- [03 — Scheduling and storage](03-scheduling-and-storage.md): the user-facing version of this material.
- [05 — Cloud Kubernetes](05-cloud-k8s.md): the `eks-setup` and `gke-setup` wizards generate env blocks that use these fields correctly.
