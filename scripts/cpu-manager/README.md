# Scripts

Some helper scripts when using static CPU manager policy with k3d. Other distros not yet supported.

## CPU policy

Preview
`./scripts/cpu-manager/k3d-cpu-static.sh --dry-run`

Enable static allocation with complete physical cores
`./scripts/cpu-manager/k3d-cpu-static.sh --delete-emptydir-data`

Reset to shared CPU scheduling
`./scripts/cpu-manager/k3d-cpu-none.sh --delete-emptydir-data`