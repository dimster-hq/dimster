#!/bin/sh
# Waits a fixed time, e.g. as the last beforeDrain hook, so the cluster can
# quieten down after the hooks before it (cache eviction, broker restarts)
# and before the drain starts. Runs inside the coordinator pod.
#
# Usage: args: ["<seconds>"] (default 60). Keep timeoutSeconds above it.
set -eu

seconds="${1:-60}"
case "$seconds" in
  ''|*[!0-9]*) echo "pause.sh: seconds must be a whole number, got '$seconds'" >&2; exit 1 ;;
esac

echo "Pausing ${seconds}s"
sleep "$seconds"
