# Scripts

Standalone tools that you run yourself, outside the `dimster` CLI. Each lives in its own directory with a README.

| Directory | What it does |
|-----------|--------------|
| [dm-delay/](dm-delay/) | Emulates network-attached storage such as AWS gp3 (read and write latency, IOPS and throughput caps) on Linux or in the Colima VM. Broker volumes can then be benchmarked against cloud-like disks. |
