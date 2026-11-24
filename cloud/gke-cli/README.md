# GCP GKE Setup via `gke-setup`

The `gke-setup` CLI is a wizard that provisions a single-zone GKE Standard cluster sized for Dimster, an Artifact Registry Docker repository for your Dimster image, a tuned Hyperdisk / Persistent Disk / Local NVMe SSD `StorageClass`, and a populated env block in your `dimster-config.yaml`. After it finishes, three commands (`build`, `deploy`, `run`) take you to a benchmark.

If you'd rather drive `gcloud` and `kubectl` by hand — for IaC integration, audit review, or restricted environments — run `gke-setup` with `--generate-script` to emit a bash script you can read, edit, and run.

## What `gke-setup` builds

Starting with a GCP project:

```
┌───────────────────────────────────────────────────────────────────┐
│   GCP project                                                     │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │ Artifact Registry (Docker): {region}-docker.pkg.dev/        │ │
│   │   {project}/{dimsterName}/dimster                           │ │
│   └─────────────────────────────────────────────────────────────┘ │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │ GKE Standard zonal cluster: {dimsterName}-benchmark         │ │
│   │ (all control plane + node pools in 1 zone, Dataplane V2)    │ │
│   │                                                             │ │
│   │   Node pool: kafka        (label role=kafka)                │ │
│   │     N × {kafkaMachineType}, gVNIC, optional Local NVMe SSD  │ │
│   │   Node pool: workers      (label role=worker)               │ │
│   │     M × {workerMachineType}, autoscaling min..max, gVNIC    │ │
│   │   Node pool: default-pool (label role=infra)                │ │
│   │     1 × {infraMachineType} (coordinator + Prom + Grafana)   │ │
│   │                                                             │ │
│   │   StorageClass: dimster-hyperdisk-balanced[-fast]           │ │
│   │                 / dimster-pd-ssd / local-storage (NVMe)     │ │
│   │   GKE PD CSI driver (pd.csi.storage.gke.io)                 │ │
│   └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
               ▲
               │  populates env block in
               │
    dimster-config.yaml
      environments:
        gke:
          image:   {region}-docker.pkg.dev/{project}/{dimsterName}/dimster
          context: gke_{project}_{zone}_{dimsterName}-benchmark
          kafka:   { nodeLabel: role=kafka, pvcStorageClass: ..., ... }
          workers: { nodeLabel: role=worker, ... }
          monitoring: { nodeLabel: role=infra, cgroupMetrics: true, ... }
```

**Single-zone on purpose.** GKE supports zonal clusters (`--zone <zone>`), so the control plane and every node pool land in the same zone. That eliminates cross-zone egress fees, keeps broker replication latency uniform, and cuts cluster creation time to ~5–8 minutes.

**Reusing `default-pool` for `infra`.** GKE always creates an initial `default-pool` during cluster creation. `gke-setup` sizes `default-pool` as the 1-node `role=infra` pool directly instead of creating a throwaway pool and deleting it, saving ~4 minutes on every cluster build.

## Prerequisites

Install the Google Cloud CLI, `kubectl`, and the GKE auth plugin:

```bash
# Verify installed tools
gcloud --version
kubectl version --client
gke-gcloud-auth-plugin --version
```

If `gke-gcloud-auth-plugin` is missing, install it with:

```bash
gcloud components install gke-gcloud-auth-plugin
```

### IAM permissions and service account impersonation

If you run `gcloud` by impersonating a dedicated service account (recommended), that service account needs more than just `roles/container.admin`. Specifically:

| Principal | Role | Why it is needed |
|-----------|------|------------------|
| Deployer SA (`--sa`) | `roles/container.admin` | Create, manage, and delete GKE clusters and node pools |
| Deployer SA (`--sa`) | `roles/artifactregistry.admin` | Create/delete Artifact Registry Docker repos and push images |
| Deployer SA (`--sa`) | `roles/compute.storageAdmin` | List and sweep orphaned GCE disks during `gke-setup delete` |
| Deployer SA (`--sa`) | `roles/compute.viewer` | Query available zones, regional quotas, and live VM/disk usage |
| Deployer SA (`--sa`) on Node VM SA | `roles/iam.serviceAccountUser` | Required (`iam.serviceAccounts.actAs`) for GKE to attach the node VM service account |
| GKE Node VM SA | `roles/artifactregistry.reader` | Allows GKE nodes to pull Dimster images from Artifact Registry |
| Human caller (optional) | `roles/iam.serviceAccountTokenCreator` on Deployer SA | Allows `gcloud` impersonation and short-lived Docker tokens |

Run the bundled helper script (from an account with IAM admin perms on the project) to enable the APIs and grant all of the above idempotently:

```bash
./cloud/gke-cli/grant-sa-permissions.sh \
  --project YOUR_PROJECT_ID \
  --sa YOUR_SA@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --impersonator user:you@example.com
```

Pass `--dry-run` first if you want to inspect the exact `gcloud` commands before applying them. Then configure `gcloud` to use the project and impersonation:

```bash
gcloud config set project YOUR_PROJECT_ID
gcloud config set auth/impersonate_service_account YOUR_SA@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

## Build the `gke-setup` binary

From the repository root:

```bash
cd cloud
cargo build --release -p gke-setup
cp target/release/gke-setup ..
cd ..
```

> On macOS Apple Silicon, sign the copied binary: `codesign --force --sign - ./gke-setup`.

You now have `./gke-setup` next to `./dimster` at the project root.

## Run the preflight check

Before creating a cluster, verify your tools, active project, impersonated service account, and IAM permissions:

```bash
./gke-setup check
```

## Run the wizard

```bash
./gke-setup create -y
```

The wizard walks through:

1. **Environment name** — key under `environments:` in `dimster-config.yaml` (default `gke`).
2. **GCP configuration & live quota snapshot** — project ID (auto-detected from `gcloud`), resource name prefix (cluster named `{dimsterName}-benchmark`), region, zone (queried via `gcloud`), a live summary of your project and regional GCP quotas (vCPUs across regions and per machine family, external IPs, Hyperdisk/PD/Local SSD headroom), GKE release channel (`regular`, `stable`, `rapid`, or pinned version), and optional kubelet static CPU Manager (`cpuManagerPolicy: static`).
3. **Deploy mode** — `full` (Dimster manages Kafka in GKE) or `external` (workers only, points at an external Kafka such as Confluent Cloud). For external, see [06 — External Kafka](../../docs/deployment/06-external-kafka.md).
4. **Kafka sizing** (full mode) — Kafka version, machine type (with specs, hourly rate hint, and computed defaults from `cloud/gke-cli/instance-types.yaml`), broker count, optional `TIER_1` networking (when supported by the machine type), JVM heap, CPU/memory requests, broker config, and thread overrides.
5. **Kafka storage** (full mode) — filtered to disk types supported by your chosen Kafka machine type (Hyperdisk Balanced, Hyperdisk Extreme, Hyperdisk Throughput, `pd-ssd`, `pd-balanced`, or Local NVMe SSDs), with live GCP limit/ratio validation and VM ceiling warnings.
6. **Infra + coordinator + monitoring** — small machine type for `default-pool` (default `e2-standard-2`); coordinator, Prometheus, and Grafana sizing.
7. **Worker sizing** — machine type (default `c3-standard-4`), initial count, autoscaling min/max, optional `TIER_1` networking, JVM heap, CPU/memory requests.
8. **Pre-deploy quota check & hourly cost estimate** — before asking you to confirm, `gke-setup` checks your chosen nodes and disks against the queried GCP quotas and displays an itemized on-demand hourly (and daily) USD estimate covering VMs, Kafka storage, boot/infra disks, `TIER_1` networking, and the GKE control plane.

Without `-y`, the wizard asks for confirmation before kicking off setup and before every `gcloud` and `kubectl` command.

### Replay, dry-run, and script generation

When the wizard completes, it writes `gke-answers-{dimsterName}.yaml`. You can replay, preview, or export a bash script at any time:

```bash
# Preview quota checks, hourly cost estimate, gcloud commands, and manifests without running anything
./gke-setup create --answers gke-answers-my-dimster.yaml --dry-run

# Emit a standalone bash script to review or run yourself
./gke-setup create --answers gke-answers-my-dimster.yaml --generate-script > setup-gke.sh

# Recreate the exact same cluster non-interactively
./gke-setup create --answers gke-answers-my-dimster.yaml -y
```

## Build and deploy Dimster

Configure Docker to authenticate against Artifact Registry for your region:

```bash
# One-time setup (uses gcloud credential helper, including global SA impersonation)
gcloud auth configure-docker us-central1-docker.pkg.dev

# Alternative: explicit 1-hour OAuth token login (also works with SA impersonation)
gcloud auth print-access-token | \
  docker login -u oauth2accesstoken --password-stdin https://us-central1-docker.pkg.dev
```

Then build, push, and deploy with Dimster:

```bash
./dimster build gke
./dimster deploy gke
```

> **Architecture note.** If you pick Google Axion Arm64 nodes (`c4a-*`), build images on an `arm64` Docker daemon (e.g. Apple Silicon natively) and keep all pools on `arm64`. For `x86_64` pools (`c3`, `c3d`, `c4`, `c4d`, `n4`, `n2`, `e2`), build on an `amd64` Docker daemon.

## Run a benchmark

```bash
./dimster run gke \
  -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/quick-test/quick-run-test.yaml
```

In a second terminal, open the dashboards:

```bash
./dimster dashboard gke
```

## Picking GCP machine types and disks

The full catalog of curated GCP machine specs and baseline hourly rates lives in `cloud/gke-cli/instance-types.yaml`. Use `./dimster resources` to calculate per-broker network and disk write demand first, then pick a machine + disk whose ceilings comfortably clear that demand.

### GCP disk types compared (throughput, IOPS, latency)

| Disk type | Backing & latency | Provisioning knobs | Machine compatibility | Notes |
|-----------|-------------------|--------------------|-----------------------|-------|
| **`hyperdisk-balanced`** (recommended default) | Next-gen Titanium SSD; sub-ms (~0.5–1 ms) | Size (6 GiB–64 TiB), IOPS (3k–160k, max 500/GiB), Throughput (140–2,400 MiB/s, bounded by `[max(140, IOPS/256), min(2400, IOPS/4)]`) | `c3`, `c3d`, `c4`, `c4a`, `c4d`, `n4` | Best GCP equivalent to AWS `gp3`/`gp3-fast`, with 2.4× the per-volume throughput ceiling. Free baseline: 3,000 IOPS + 140 MiB/s. |
| **`local-ssd`** (Local NVMe SSD) | Directly attached NVMe / Titanium SSD; **lowest latency (~100–200 µs)** | 375 GiB increments; fixed count on `-lssd` shapes, or attach 1/2/4/8 on `n2`/`n2d`. 1 disk ≈ 660R/350W MiB/s; 2 ≈ 1320R/700W; 4 ≈ 2650R/1400W | `*-lssd` (`c3`, `c3d`, `c4`, `c4a`, `c4d`) and attachable `n2`/`n2d` | Ephemeral (lost on node recreate/upgrade). Provisioned via `--local-nvme-ssd-block`, RAID-0 + XFS mounted at `/mnt/disks/nvme0`, exposed as `local-storage` static PVs. |
| **`hyperdisk-extreme`** | Highest-perf durable block storage; lowest durable sub-ms latency | Size (64 GiB–64 TiB), IOPS (2k–350k); throughput auto-couples at 250 MiB/s per 1k IOPS (up to 5,000 MiB/s) | High-vCPU shapes only (`c3` ≥88, `c3d` ≥60, `c4a` ≥64, `c4` ≥96 vCPUs) | For extreme durable IOPS/latency benchmarks on large nodes. |
| **`hyperdisk-throughput`** | HDD-backed sequential storage; higher latency (few ms) | Size (2 TiB–32 TiB), Throughput (20–2,400 MiB/s, `5*TiB..=90*TiB`); IOPS auto-couples at 4 IOPS per MiB/s | `c3`, `c3d`, `c4`, `c4a`, `c4d`, `n4` | Useful for high-capacity sequential-write or HDD-like latency comparisons. |
| **`pd-ssd`** | Legacy SSD Persistent Disk; sub-ms (~0.5–1 ms) | Size only (scales at `3,000 + 30 IOPS/GiB` up to 100k, `0.48 MiB/s/GiB` up to 1,200 MiB/s) | `e2`, `n2`, `n2d`, `c3`, `c3d` (**not** `n4`/`c4`/`c4a`/`c4d`) | Simple size-scaled SSD when running on `n2`/`n2d`. |
| **`pd-balanced`** | Legacy Balanced Persistent Disk; ~1–2 ms | Size only (`3,000 + 6 IOPS/GiB` up to 80k, `0.24 MiB/s/GiB` up to 1,200 MiB/s) | `e2`, `n2`, `n2d`, `c3`, `c3d` (**not** `n4`/`c4`/`c4a`/`c4d`) | Budget SSD PD; GKE's default `standard-rwo` class. |

### Critical GCP rules to watch out for

1. **Effective disk performance = `min(disk provisioned limit, VM ceiling)`.** A `c4-standard-4` caps Hyperdisk Balanced at 400 MiB/s and 50,000 IOPS even if you provision a 2,400 MiB/s volume. `gke-setup` displays each machine type's VM ceiling from `instance-types.yaml` and warns when your StorageClass exceeds it.
2. **4th-gen machines (`n4`, `c4`, `c4a`, `c4d`) are Hyperdisk-only.** They cannot attach `pd-balanced` or `pd-ssd` — not even as a node boot disk. `gke-setup` automatically sets `--disk-type=hyperdisk-balanced` for those node pools and filters out unsupported disk types during storage selection.
3. **Keep the `infra` pool on a PD-capable machine (`e2-standard-2`).** Dimster's `benchmark-results` (`1Gi`) and `prometheus-data` (`50Gi`) PVCs bind to GKE's default `standard-rwo` (`pd-balanced`) StorageClass, which cannot attach to Hyperdisk-only infra nodes (`n4`/`c4`/`c4a`). `e2-standard-2` (2 dedicated vCPUs, 8 GiB RAM) is cheap, supports `standard-rwo`, and avoids shared-core E2 CPU reservations.
4. **Network bandwidth & `TIER_1`.** Unlike EC2's burst-credit cliff, GCP egress bandwidth is sustained per VM (C3/C4/C4A give 23 Gbps even at 4–8 vCPUs, which is ~2,875 MB/s!). Larger shapes (`n2-standard-32`, `c3d-standard-30`, `c4a-standard-32`, `c3-standard-44`, `c4-standard-48`) also unlock `TIER_1` networking (50–200 Gbps) via gVNIC, which `gke-setup` offers interactively.

### Quick-pick by workload shape

| Target throughput (aggregate, RF=3) | Broker pick (3-broker cluster) | Worker pick (2 workers) |
|---|---|---|
| < 150 MB/s ingress | `c3-standard-4` + `hyperdisk-balanced` (3,000 IOPS, 250 MiB/s) | `c3-standard-4` or `n4-standard-4` |
| 150–400 MB/s ingress | `c3-standard-8` or `c4-standard-8` + `hyperdisk-balanced` (10,000 IOPS, 600 MiB/s) | `c3-standard-4` |
| 400–1,000 MB/s ingress | `c4-standard-16` + `hyperdisk-balanced` (25,000 IOPS, 1,200 MiB/s) | `c3-standard-8` or 4× `c3-standard-4` |
| Lowest latency / local NVMe | `c3-standard-8-lssd` (2×375 GiB) or `c4-standard-16-lssd` (2×375 GiB) + `local-storage` | `c3-standard-8` |
| Arm64 price/performance | `c4a-standard-8` (+ `hyperdisk-balanced`) or `c4a-standard-8-lssd` (`local-storage`) | `c4a-standard-4` |

## Scaling node pools

Resize the worker pool at any time:

```bash
gcloud container clusters resize ${DIMSTER_NAME}-benchmark \
  --zone $GCP_ZONE \
  --node-pool workers \
  --num-nodes 4
```

Update `workers.replicas` in `dimster-config.yaml` to match and re-run `./dimster deploy gke`.

## Teardown

**Tear down Dimster only, keep the GKE cluster running:**

```bash
./dimster teardown gke
```

**Delete the entire cluster, orphaned GCE disks, and Artifact Registry repo:**

```bash
./gke-setup delete --answers gke-answers-my-dimster.yaml
```

To keep the Artifact Registry repo so you don't have to re-push images next time:

```bash
./gke-setup delete --answers gke-answers-my-dimster.yaml --keep-registry
```

## Troubleshooting

**`403 Required "iam.serviceAccounts.actAs" permission` during cluster create.** Your deployer service account is missing `roles/iam.serviceAccountUser` on the GKE node VM service account. Run `./cloud/gke-cli/grant-sa-permissions.sh` (or `./gke-setup check` to inspect missing permissions).

**`ErrImagePull` / `ImagePullBackOff` from `pkg.dev`.** Verify the GKE node VM service account has `roles/artifactregistry.reader`, and that you pushed the image with `./dimster build gke` after running `gcloud auth configure-docker <region>-docker.pkg.dev`.

**`gke-gcloud-auth-plugin` not found when running `kubectl` or `dimster`.** Install the plugin (`gcloud components install gke-gcloud-auth-plugin`) and make sure Google Cloud SDK's `bin/` directory is on your `PATH`.

**`Quota 'HDB_TOTAL_GB' exceeded` or `Quota 'SSD_TOTAL_GB' exceeded` when PVCs bind.** New GCP projects often start with tight regional disk quotas (typically `500 GB` for `HDB_TOTAL_GB` and `250 GB` for `SSD_TOTAL_GB`). Every node's boot disk counts toward its disk family's quota (`hyperdisk-balanced` for `n4`/`c4`/`c4a`/`c4d`, `pd-balanced` for `e2`/`n2`/`n2d`/`c3`/`c3d`), and Dimster's `prometheus-data` PVC requests `50Gi` on the default `standard-rwo` (`pd-balanced`) class. `gke-setup` sets `--disk-size 50` on all node pools (instead of GKE's 100 GB default) to halve boot-disk quota consumption and displays your live quotas before you size the cluster; for larger disks or more workers, request a quota increase in Cloud Console → *IAM & Admin → Quotas*.

**Local NVMe SSD pods stuck in `Pending`.** Check `kubectl logs -n kube-system -l app=nvme-format` and `kubectl get pv`. On `dimster teardown` + `dimster deploy`, Dimster automatically resets `Released` PVs belonging to StorageClass `local-storage`.

## See also

- [05 — Cloud Kubernetes](../../docs/deployment/05-cloud-k8s.md) — shared overview across cloud Kubernetes targets.
- [GKE Setup CLI Reference](../../docs/reference/gke-setup-cli.md) — flags, answers YAML schema, and default sizing formulas.
- [06 — External Kafka](../../docs/deployment/06-external-kafka.md) — if you chose `deploy: external` in the wizard.
- [Internals — what `nodeLabel` and `pvcStorageClass` actually do](../../docs/deployment/internals-scheduling-storage.md) — exact Kustomize patches and supported/unsupported features.
