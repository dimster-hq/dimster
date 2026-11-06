#!/usr/bin/env bash
# Host-side entry point for enabling CPU pinning on an existing k3d cluster.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 "$script_dir/lib/k3d_cpu_manager.py" static "$@"
