# dm-delay

Builds volumes that behave like network-attached storage, such as AWS gp3, on a local Linux machine or in the Colima VM on macOS:

- **Latency:** from the device-mapper `delay` target.
- **Caps:** IOPS and throughput caps from cgroup v2 `io.max`.

Both apply below the page cache, so only real device I/O pays.

| File | Purpose |
|------|---------|
| `dm-delay.sh` | The tool. Run as root on Linux: `create`, `set`, `check`, `status`, `up`/`down`, `node-mount`/`node-umount`, `pv-yaml`, `destroy`, `profiles`. |
| `colima.sh` | Runs `dm-delay.sh` (or `test.sh`) inside the Colima VM, installing `dmsetup` there if needed. |
| `test.sh` | Integration test: builds a small volume and checks latency, caps, down/up and cleanup. Run as root, or with `colima.sh test`. |

Quick start, on Linux (on macOS, replace `sudo ./dm-delay.sh` with `./colima.sh`):

```bash
sudo ./dm-delay.sh create ebs --size 400G --mount /mnt/dimster-ebs
sudo ./dm-delay.sh set ebs --profile gp3
sudo ./dm-delay.sh check ebs
```

Requirements:

- Linux 6.8 or newer, for exact millisecond delays
- cgroup v2
- `dmsetup`, `losetup` and `mkfs.ext4`

The guide is [docs/deployment/09-emulated-network-storage.md](../../docs/deployment/09-emulated-network-storage.md). It covers the emulation model and its limitations, putting brokers on a volume with minikube, k3d or kind, and a gp3 run on a reused backlog.

## Cleaning up

`./dimster teardown <env>` deletes everything except the actual delay PVs. To delete those you need to delete the PVs then the delay volumes. See the example below.

For brokers on volume `ebs` with minikube:

```bash
kubectl delete pv dimster-ebs-pv-0                           # the PVs pv-yaml made
sudo ./dm-delay.sh node-umount ebs --container minikube
sudo ./dm-delay.sh destroy ebs --yes                         # deletes the data
```

The PVs use `Retain`, so deleting a PVC keeps the data on the volume; only `destroy` deletes it. `down` and `destroy` refuse while a node container still has the volume mounted. `status` lists the volumes that remain.

## When a volume runs out of space

There are two ways to run out, with different symptoms:

- **The volume is full.** `df -h` on the mount shows 100%, and Kafka fails with `No space left on device`. A local PV doesn't enforce `pvcSize`, so a broker writes until the volume is full. Volumes can't be resized: clean up as above and create a bigger one, or make the workload smaller (backlog size, retention).
- **The disk under the backing files is full.** The backing files are sparse, so they grow as the volume fills, and the filesystem holding them can fill first. The volume then fails writes with I/O errors. Kafka logs `SyncFailedException: sync failed` and takes the log dir offline, and `dmesg` shows `critical space allocation error` on a loop device. Check with `df -h` on the backing directory: `/var/lib/dimster-dm-delay` on Linux, or `colima ssh -- df -h /mnt/lima-colima` with Colima. `create` refuses sizes that don't fit, but other users of that disk, such as Docker images and volumes, can still fill it.

In the second case, free space first (`docker system df` shows what Docker holds). The filesystem on the volume may have lost writes, so don't reuse it: clean up as above and recreate the volume, then run `node-mount` and `pv-yaml` again, and redeploy.
