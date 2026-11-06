# 09 — Emulated network-attached storage

Local NVMe drives are much faster than the network-attached volumes most cloud deployments use. A fast SSD answers a 4 KiB read in well under 100 µs and has no practical IOPS or throughput limit. An AWS gp3 volume takes roughly 0.5–1 ms per read and is capped at 3,000 IOPS and 125 MiB/s by default. Results that depend on read latency or read volume can rank configurations differently on the two, so it is worth running on something gp3-like without leaving your machine.

`scripts/dm-delay/dm-delay.sh` builds such a volume on any Linux machine, including the Colima VM on macOS. Broker data, or any container's data, can then live on it.

## How it works

A delay volume is a filesystem on a Linux device-mapper `delay` device, stacked on a loop file (or a spare partition). A broker reaches it through an ordinary Kubernetes local PersistentVolume. The picture shows one broker, `kafka-0`, on minikube with volume `ebs0`; the right-hand column says which command or setting makes each piece.

```
  Kafka broker: pod kafka-0
    log.dirs=/var/kafka-data/kafka-logs
        |
        |  the pod mounts its volume kafka-data at /var/kafka-data
        v
  PVC kafka-data-kafka-0                 the StatefulSet's claim template;
    storageClassName: local-storage      class and size from the env's
    request: 7Gi                         pvcStorageClass and pvcSize
        |
        |  binds to an Available PV of that class, at least that size
        v
  PV dimster-ebs0-pv-0                   pv-yaml
    local.path: /mnt/dimster-ebs0/pv-0
    nodeAffinity: minikube               the pod runs on this node
        |
        |  the kubelet bind-mounts local.path into the pod
        v
+-- node: the minikube container ------------------------------------+
|  /mnt/dimster-ebs0                     node-mount                  |
+--------------------------------------------------------------------+
        |
        |  the same filesystem, mounted in both places
        v
+-- host: Linux, or the Colima VM -----------------------------------+
|  /mnt/dimster-ebs0                     create                      |
|      |                                                             |
|      |  ext4 and the page cache: cache hits stop here              |
|      v                                                             |
|  /dev/mapper/dimster-ebs0              delay target: holds each    |
|      |                                 read or write N ms (set)    |
|      |  io.max caps for this device, on the docker cgroup (set)    |
|      v                                                             |
|  /dev/loopN                            direct I/O, so the backing  |
|      |                                 file isn't cached twice     |
|      v                                                             |
|  backing file ebs0.img                 sparse (create)             |
+--------------------------------------------------------------------+
```

From the top:

1. **Broker to claim.** The Kafka StatefulSet has a claim template, `kafka-data`, so each broker gets its own PVC, `kafka-data-kafka-<N>`, mounted where `log.dirs` points. With `pvcStorageClass: local-storage` in the env, the PVC asks for that class.
2. **Claim to PV.** The `local-storage` class has no provisioner, so Kubernetes never creates volumes for it. A PVC can only bind to a PV that already exists, and `pv-yaml` makes those: one `local` PV per `pv-<i>` directory on the volume, pinned to a node. Binding waits until the pod is scheduled (`WaitForFirstConsumer`). Kubernetes chooses which PV each broker gets, so with one volume per broker, `kafka-0` may land on `ebs2`. They're configured alike, so it doesn't matter.
3. **PV to node.** A local PV's path is a path on the node. With minikube, k3d or kind, the node is a container with its own mounts, and it doesn't see the host's. `node-mount` mounts the same device inside that container at the same path. With k3s or kubeadm on the host, the node is the host, and this layer goes away.
4. **Filesystem to device.** Both mounts are one ext4 filesystem in the host kernel, so the broker's file I/O goes through the host's page cache. Only cache misses and writeback reach the delay device.

What's applied at the device:

- **Latency:** the `delay` target holds each read, and separately each write, for a fixed number of milliseconds.
- **Caps:** IOPS and throughput are capped with cgroup v2 `io.max` on that device.

Both sit below the page cache, so cache hits stay fast and only reads and writes that reach the device pay, as on EBS. This is why the scripts don't use the Kubernetes chaos tools (Chaos Mesh IOChaos, Litmus disk latency): they delay file syscalls through FUSE, cache hits included, and FUSE changes caching and readahead.

`io.max` is set on the parent cgroups that containers and pods run under. Because it is per device, it limits the *total* I/O to this volume from everything under that cgroup, and leaves other disks alone. Each volume therefore behaves like one EBS volume with its own caps. By default the caps go on every one of these that exists:

| Cgroup | What runs there |
|--------|-----------------|
| `/sys/fs/cgroup/docker` | Docker with the cgroupfs driver, e.g. Colima |
| `/sys/fs/cgroup/system.slice` | Docker with the systemd driver, and system services |
| `/sys/fs/cgroup/kubepods.slice` / `kubepods` | Bare k3s or kubeadm pods |

`io.max` counts I/O by the cgroup of the process that issues it, and a cap on a cgroup covers every cgroup below it. minikube, k3d and kind nodes are Docker containers, so their pods fall under one of the first two. In Colima, for example, a broker runs in `/docker/<minikube container>/kubepods/burstable/<pod>/<container>`, so the cap on `docker` covers it. Pass `--cgroup` to choose the cgroups yourself.

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

## Prerequisites

To use delay volumes, the following prequisites are needed:

- OS:
    - Linux: Install `dmsetup` (`apt-get install dmsetup`, or `dnf install device-mapper`). Check for kernel 6.8 or newer (`uname -r`) and cgroup v2 (`stat -fc %T /sys/fs/cgroup` prints `cgroup2fs`).
    - MacOS: Use Colima. Start colima with enough disk for your delay volumes.
- Kubernetes distros:
    - Supported: Minikube, k3d, kind
    - Not supported/not tested: Docker Desktop, OrbStack

`check` and the integration test require GNU `dd`, detected as `dd`, `gnudd` or `gdd`. Some uutils `dd` versions fail direct I/O on loop/device-mapper volumes with `IO error: Invalid input`; the scripts select GNU `dd` automatically when it is installed alongside uutils.

## Commands

Run as root on Linux. On macOS, use `scripts/dm-delay/colima.sh` with the same arguments; it runs the script inside the Colima VM.

```bash
dm-delay.sh create NAME --size 400G --mount /mnt/dimster-ebs   # 0 ms, no caps, mounted
dm-delay.sh set NAME --profile gp3                              # live, even while in use
dm-delay.sh set NAME --profile gp3 --rbps 250M                  # profile plus overrides
dm-delay.sh check NAME                                          # measure latency, IOPS, throughput
dm-delay.sh status [NAME]                                       # devices, tables, caps per cgroup
dm-delay.sh down NAME                                           # detach, keeping the data
dm-delay.sh up NAME                                             # reattach (e.g. after a reboot)
dm-delay.sh node-mount NAME --container minikube                # mount inside a Kubernetes node container
dm-delay.sh node-umount NAME --container minikube               # unmount from it
dm-delay.sh pv-yaml NAME --node minikube --size 300Gi [--count N]   # local-storage StorageClass + PVs
dm-delay.sh destroy NAME --yes                                  # delete the volume and its data
dm-delay.sh profiles
```

`--container` is the node's container, and `--node` is the Kubernetes node name. Both are `minikube` on minikube. For k3d and kind, see the [table of node names](#clusters-whose-nodes-are-containers-minikube-k3d-kind). 

- **Backing:** the backing file is sparse, in `/var/lib/dimster-dm-delay` by default (`DM_DELAY_STATE_DIR` changes it), so it only uses the space that's written. `create --device /dev/nvme1n1p3 --force` uses a spare partition instead and destroys its contents.
- **Space:** if the filesystem holding a backing file fills up, the volume fails writes with I/O errors, and Kafka takes the log dir offline. So `create` refuses a size that filesystem can't hold, counting the unwritten part of the other volumes there. `--overcommit` skips the check. The check can't see other users of that filesystem, such as Docker, so leave room for them.
- **Colima:** `colima.sh` puts backing files and state on Colima's data disk (`/mnt/lima-colima/dimster-dm-delay`, the disk `colima start --disk` sizes), not the VM's small root disk.
- **TRIM:** discard passes through to the backing file, so `fstrim` on the mount point works as usual.
- **Reboots:** volumes don't survive a reboot until you run `up`, which re-applies the last settings.

`scripts/dm-delay/test.sh` (or `colima.sh test`) is an integration test. It builds a small volume, checks that latency, IOPS and throughput settings take effect, and cleans up.

## Putting brokers on a delay volume

Kafka brokers use the `local-storage` StorageClass: `pv-yaml` prints the StorageClass and one local PersistentVolume per broker for you to `kubectl apply`, and you set `pvcStorageClass: local-storage` in the env block (see [03 — Scheduling and storage](03-scheduling-and-storage.md)). Dimster's existing `local-storage` support releases the PVs between runs.

A PV is a directory on the volume (`pv-0`, `pv-1`, …). The brokers sharing one volume share its caps. For per-broker caps, as with one EBS volume per broker, create one volume per broker and run `pv-yaml --count 1` for each. Give each PV its broker's node with `--node`.

### Clusters whose nodes are containers (minikube, k3d, kind)

A pod on a local PV sees the PV's path inside the node, and here the node is a container that doesn't see the host's mounts. `node-mount` mounts the volume inside a running node container at the same path. It works because these node containers are privileged, and no cluster has to be recreated.

```bash
sudo scripts/dm-delay/dm-delay.sh create ebs --size 400G --mount /mnt/dimster-ebs
sudo scripts/dm-delay/dm-delay.sh node-mount ebs --container k3d-dimster-server-0   # minikube: --container minikube
sudo scripts/dm-delay/dm-delay.sh pv-yaml ebs --node k3d-dimster-server-0 --size 300Gi | kubectl apply -f -
```

Then set `pvcStorageClass: local-storage` and a `pvcSize` no larger than the PV on the env's `kafka` block, and deploy. Changing `pvcStorageClass` makes the next deploy delete the brokers' existing data PVCs.

The PV must name the node, and the node container must be given as `--container`:

| Distro | Node and container name |
|--------|-------------------------|
| minikube (docker driver) | `minikube` |
| k3d | `k3d-<cluster>-server-0` |
| kind | `<cluster>-control-plane` |

**Restarts:** a node-mount lasts until the node container restarts (`minikube stop`/`start`, `k3d cluster stop`/`start`, a reboot), so run `node-mount` again after one, and `up` first after a reboot. Until then, pods using the PVs fail to start: each PV points at a `pv-<i>` directory that only exists on the volume. They never write to the container's own filesystem by mistake.

**Bind mount alternative:** bind-mount the mount point into the node when the cluster is created. For k3d that's `--volume /mnt/dimster-ebs:/mnt/dimster-ebs@server:0`; for kind, `extraMounts`. That survives node restarts once the volume is `up` before the node starts, but needs a new cluster.

**Removing:** `down` and `destroy` refuse while a container still has the volume mounted; run `node-umount` first, after stopping the pods.

### k3s or kubeadm on the host

There's no node container, so `node-mount` isn't needed. Run `pv-yaml` with the host's node name.

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
