# 09 — Emulated network-attached storage

Local NVMe drives are much faster than the network-attached volumes most cloud deployments use. A Samsung 9100 PRO answers a 4 KiB read in well under 100 µs and has no practical IOPS or throughput limit. An AWS gp3 volume takes roughly 0.5–1 ms per read and is capped at 3,000 IOPS and 125 MiB/s by default. Results that depend on read latency or read volume can rank configurations differently on the two, so it is worth running on something gp3-like without leaving your machine.

`scripts/dm-delay/dm-delay.sh` builds such a volume on any Linux machine, including the Colima VM on macOS. Broker data, or any container's data, can then live on it.

## How it works

A delay volume is a filesystem on a Linux device-mapper `delay` device, stacked on a loop file (or a spare partition):

```
filesystem (your mount point)  ->  /dev/mapper/dimster-NAME (delay)  ->  /dev/loopN (direct I/O)  ->  backing file
```

- **Latency:** the `delay` target holds each read, and separately each write, for a fixed number of milliseconds.
- **Caps:** IOPS and throughput are capped with cgroup v2 `io.max` on that device.

Both sit below the page cache, so cache hits stay fast and only reads and writes that reach the device pay, as on EBS. This is why the scripts don't use the Kubernetes chaos tools (Chaos Mesh IOChaos, Litmus disk latency): they delay file syscalls through FUSE, cache hits included, and FUSE changes caching and readahead.

`io.max` is set on the parent cgroups that containers and pods run under. Because it is per device, it limits the *total* I/O to this volume from everything under that cgroup, and leaves other disks alone. Each volume therefore behaves like one EBS volume with its own caps. By default the caps go on every one of these that exists:

| Cgroup | What runs there |
|--------|-----------------|
| `/sys/fs/cgroup/docker` | Docker with the cgroupfs driver, e.g. Colima |
| `/sys/fs/cgroup/system.slice` | Docker with the systemd driver, and system services |
| `/sys/fs/cgroup/kubepods.slice` / `kubepods` | Bare k3s or kubeadm pods |

k3d and kind nodes are Docker containers, so their pods fall under one of the first two. Pass `--cgroup` to choose the cgroups yourself.

## What you can configure

| Setting | Flag | Mechanism |
|---------|------|-----------|
| Read latency | `--read-ms N` | `delay` target, reads |
| Write latency (flushes too) | `--write-ms N` | `delay` target, writes |
| Read / write IOPS cap | `--riops N` / `--wiops N` | `io.max` |
| Read / write throughput cap | `--rbps SIZE` / `--wbps SIZE` | `io.max` (SIZE like `125M`, binary units) |
| Reads only | `--reads-only` | No write latency and no write caps |

A cap of `0` or `max` means none.

Profiles give starting points, and the flags above override them:

| Profile | Read / write latency | IOPS cap (r / w) | Throughput cap (r / w) | Models |
|---------|----------------------|------------------|------------------------|--------|
| `none` | 0 | none | none | The raw device |
| `gp3` | 1 ms | 3,000 | 125 MiB/s | gp3 defaults |
| `gp3-max` | 1 ms | 16,000 | 1,000 MiB/s | gp3 at its classic provisioned maximum |

The gp3 numbers match [05 — AWS EKS](05-aws-eks.md). AWS changes its limits from time to time, so check current figures for anything you publish.

Other volume types and instance limits are just different flags. For example, an m6i.2xlarge's 312 MB/s EBS bandwidth limit is `--rbps 297M`, since that page's MB/s are decimal and `M` here is binary.

### Limitations

- **Whole milliseconds only.** `delay` takes integer milliseconds, so 1 ms is the smallest non-zero latency. That's at the slow end of gp3's typical latency.
- **Kernel 6.8 or newer is needed for exact delays.** It handles short delays with a kthread. Older kernels round each delay up to the timer tick (`CONFIG_HZ`), and the script warns about this. `check` shows the real latency.
- **Separate read and write budgets.** gp3 shares one IOPS and one throughput budget between reads and writes. `io.max` caps reads and writes separately. During a drain there are few writes, so the read caps are what matter.
- **IOPS are counted per I/O as issued.** EBS counts an I/O larger than 256 KiB as several. With `io.max`, a 1 MiB read counts once, so large reads are slightly favoured compared with gp3.
- **Latency is fixed per I/O.** There is no variance, and no growth with I/O size or load except the queueing that the caps cause.
- **The caps are per cgroup.** Each cgroup in the target list gets its own budget for the volume. With k3d, all broker I/O comes from Docker's cgroup, so that budget is the volume's budget.

## Commands

Run as root on Linux. On macOS, use `scripts/dm-delay/colima.sh` with the same arguments; it runs the script inside the Colima VM.

```bash
dm-delay.sh create NAME --size 400G --mount /mnt/dimster-ebs   # 0 ms, no caps, mounted
dm-delay.sh set NAME --profile gp3                              # live, even while in use
dm-delay.sh set NAME --profile gp3 --rbps 250M                  # profile plus overrides
dm-delay.sh check NAME                                          # measure latency, IOPS, throughput
dm-delay.sh status [NAME]                                       # devices, tables, caps per cgroup
dm-delay.sh down NAME / up NAME                                 # detach / reattach (e.g. after a reboot)
dm-delay.sh pv-yaml NAME --node NODE --size 300Gi [--count N]   # local-storage StorageClass + PVs
dm-delay.sh destroy NAME --yes                                  # delete the volume and its data
dm-delay.sh profiles
```

- **Backing:** the backing file is sparse, in `/var/lib/dimster-dm-delay` by default, so it only uses the space that's written. `create --device /dev/nvme1n1p3 --force` uses a spare partition instead and destroys its contents.
- **TRIM:** discard passes through to the backing file, so `fstrim` on the mount point works as usual.
- **Reboots:** volumes don't survive a reboot until you run `up`, which re-applies the last settings.

`scripts/dm-delay/test.sh` (or `colima.sh test`) is an integration test. It builds a small volume, checks that latency, IOPS and throughput settings take effect, and cleans up.

## Putting brokers on a delay volume

Kafka brokers use the `local-storage` StorageClass: `pv-yaml` prints the StorageClass and one local PersistentVolume per broker for you to `kubectl apply`, and you set `pvcStorageClass: local-storage` in the env block (see [03 — Scheduling and storage](03-scheduling-and-storage.md)). Dimster's existing `local-storage` support releases the PVs between runs.

A PV is a directory on the volume (`pv-0`, `pv-1`, …). The brokers sharing one volume share its caps. For per-broker caps, as with one EBS volume per broker, create one volume per broker and run `pv-yaml --count 1` for each. Give each PV its broker's node with `--node`.

### k3d (Linux, or Colima)

k3d nodes are containers, so the mount point has to be bind-mounted into the node. k3d only accepts volumes when the cluster is created, so the cluster has to be created (or recreated) after the delay volume exists:

```bash
sudo scripts/dm-delay/dm-delay.sh create ebs --size 400G --mount /mnt/dimster-ebs
k3d cluster delete dimster    # if it exists: this deletes its data, including backlogs
k3d cluster create dimster --servers 1 --agents 0 \
  --k3s-arg "--disable=traefik@server:0" \
  --volume /mnt/dimster-ebs:/mnt/dimster-ebs@server:0    # plus your usual flags (kubelet args, ...)
sudo scripts/dm-delay/dm-delay.sh pv-yaml ebs --node k3d-dimster-server-0 --size 300Gi | kubectl apply -f -
```

Then set `pvcStorageClass: local-storage` and `pvcSize: 300Gi` (or less than the PV) on the env's `kafka` block, and deploy.

After a reboot, run `dm-delay.sh up ebs`, then `k3d cluster stop dimster && k3d cluster start dimster`, so the node sees the remounted volume.

### kind

Add the mount point to the node with `extraMounts` in the kind config (`hostPath: /mnt/dimster-ebs`, `containerPath: /mnt/dimster-ebs`). Then create the cluster and continue as for k3d, with `--node dimster-control-plane`.

### k3s or kubeadm on the host

There's no node container, so no bind mount is needed. Run `pv-yaml` with the host's node name.

### minikube

minikube is untested. With the docker driver, the mount point would have to be passed into the node container when it is created.

### Containers and the Kafka storage harness

Any container can use the volume as a bind mount (`docker run -v /mnt/dimster-ebs:/data ...`), and the caps apply to it.

For the Kafka repo's columnar storage harness (`jmh-benchmarks/columnar-harness/run.sh`), set `HARNESS_VOLUME` to the mount point. An absolute path makes Docker bind-mount it in place of the named volume, and the harness reports the delay device's disk stats.

## A gp3 run on a reused backlog

Building a large backlog through gp3 caps is slow: 200 GB at 125 MiB/s is close to half an hour. Latency and caps can be changed while the volume is in use, so build fast and drain slow:

1. `dm-delay.sh set ebs --profile none`.
2. Run the workload with `reuseTopic: true`, so the backlog is built once and kept (see [drain-backlog](../modes/drain-backlog.md#keeping-backlogs-across-workloads-and-runs)).
3. `dm-delay.sh set ebs --profile gp3` (or `--profile gp3 --reads-only`).
4. Run the workload again. It drains the kept backlog under gp3 conditions.

Record the profile as an extra dimension (`dimster run ... -d storage=gp3`, see [08 — Infrastructure dimensions](08-infra-dimensions.md)), so that results on different storage stay apart in the CSVs.

The eviction hook (`run/hooks/evict-topic-cache.sh`) works on local PVs unchanged.

Switching profiles per test point would need a hook that runs on the host as root, which Dimster's coordinator-side hooks can't do. For now, run one profile per `dimster run`.
