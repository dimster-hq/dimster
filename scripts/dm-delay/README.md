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
