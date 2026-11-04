#!/usr/bin/env bash
# Runs dm-delay.sh (or test.sh) inside the Colima VM on macOS, as root, so
# delay volumes can back containers and Kubernetes clusters running in Colima.
# Colima mounts the macOS home directory into the VM at the same path, so the
# scripts run from this checkout; volumes, mounts and their state live in the
# VM. Installs dmsetup in the VM the first time (Colima's Ubuntu lacks it).
#
# Usage: colima.sh <dm-delay.sh command> [args]   e.g. colima.sh status
#        colima.sh test                           runs test.sh in the VM
#
# Optional environment:
#   COLIMA_PROFILE  Colima profile (default: default)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
profile="${COLIMA_PROFILE:-default}"

command -v colima >/dev/null 2>&1 || { echo "colima.sh: colima not found" >&2; exit 1; }
case "$here" in
  "$HOME"/*) ;;
  *) echo "colima.sh: the checkout must be under $HOME, which Colima mounts into the VM" >&2; exit 1 ;;
esac

vm() { colima ssh -p "$profile" -- "$@"; }

if ! vm sh -c 'command -v dmsetup >/dev/null 2>&1'; then
  printf '==> Installing dmsetup in the Colima VM\n'
  vm sudo sh -c 'apt-get update -qq >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq dmsetup >/dev/null'
fi

if [ "${1:-}" = test ]; then
  shift
  vm sudo "$here/test.sh" "$@"
else
  vm sudo "$here/dm-delay.sh" "$@"
fi
