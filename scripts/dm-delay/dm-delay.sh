#!/usr/bin/env bash
# Emulates network-attached storage (such as AWS gp3) on a local Linux machine,
# so brokers can be benchmarked against cloud-like disk latency and caps
# without leaving the laptop or workstation.
#
# A named volume is a filesystem on a device-mapper "delay" device, stacked on
# a loop file (or a spare partition):
#
#   filesystem (mounted at --mount)  ->  /dev/mapper/dimster-NAME (delay)
#                                     ->  /dev/loopN (direct I/O)  ->  backing file
#
# Latency comes from the delay target, which holds each read or write for a
# fixed number of milliseconds. IOPS and throughput caps come from cgroup v2
# io.max on that device. Both sit below the page cache, so cache hits stay
# fast and only real device I/O pays, as on EBS. io.max is set on the parent
# cgroups that containers and pods run under (auto-detected, or --cgroup), so
# a cap is shared by everything that uses the volume: a per-volume cap.
#
# Run as root on Linux (kernel 6.8+ for exact short delays), or on macOS
# through colima.sh, which runs this script inside the Colima VM. See
# docs/deployment/09-emulated-network-storage.md.
#
# Usage: dm-delay.sh <command> [args]   (dm-delay.sh help for the commands)
#
# Optional environment:
#   DM_DELAY_STATE_DIR  where volume state and backing files live (default /var/lib/dimster-dm-delay)
set -euo pipefail

STATE_DIR="${DM_DELAY_STATE_DIR:-/var/lib/dimster-dm-delay}"
CGROUP_ROOT=/sys/fs/cgroup
# Parent cgroups that container runtimes and kubelets put workloads under,
# checked in this order when no --cgroup is given.
AUTO_CGROUPS=(docker system.slice kubepods.slice kubepods)

usage() {
  cat <<'EOF'
Usage: dm-delay.sh <command> [args]

Commands:
  create NAME --size SIZE --mount DIR [--backing FILE | --device DEV --force] [--fs ext4|xfs]
      Create a delay volume (0 ms, no caps) and mount it. SIZE like 400G.
      The backing file is sparse; --device uses a spare partition instead
      (its data is destroyed, so --force is required).
  up NAME
      Reattach and mount a volume after a reboot, re-applying its settings.
  down NAME
      Clear its caps, unmount and detach it. The data is kept.
  set NAME [--profile P] [--read-ms N] [--write-ms N] [--riops N] [--wiops N]
           [--rbps SIZE] [--wbps SIZE] [--reads-only] [--cgroup PATH]...
      Change latency and caps, live (the volume can be mounted and in use).
      Flags override the profile; a cap of 0 or max means none. --reads-only
      leaves writes undelayed and uncapped. --cgroup (repeatable, relative to
      /sys/fs/cgroup or absolute) replaces the auto-detected cgroups.
  status [NAME]
      Show volumes, their delay tables, caps and the cgroups they apply to.
  check NAME
      Measure read latency, IOPS and throughput on the volume, under its caps.
  pv-yaml NAME --node NODE --size SIZE [--count N] [--node-path PATH]
      Create pv-<i> directories on the volume and print a local-storage
      StorageClass and N local PersistentVolumes for kubectl apply.
      --node-path is where the mount appears on the node (default the mount).
  destroy NAME --yes
      Down the volume and delete its backing file and state.
  profiles
      List the built-in profiles.

Profiles: none, gp3, gp3-max (see dm-delay.sh profiles).
EOF
}

die() { echo "dm-delay: $*" >&2; exit 1; }
info() { printf '==> %s\n' "$*"; }

# ---- profiles ----

# Prints "read_ms write_ms riops wiops rbps wbps" for a profile; caps are
# "max" for none. gp3 numbers: AWS defaults and per-volume maximums
# (docs/deployment/05-aws-eks.md); check current AWS limits before relying
# on them.
profile_values() {
  case "$1" in
    none) echo "0 0 max max max max" ;;
    gp3) echo "1 1 3000 3000 $((125 * 1024 * 1024)) $((125 * 1024 * 1024))" ;;
    gp3-max) echo "1 1 16000 16000 $((1000 * 1024 * 1024)) $((1000 * 1024 * 1024))" ;;
    *) die "unknown profile '$1' (none, gp3, gp3-max)" ;;
  esac
}

list_profiles() {
  printf '%-8s %8s %8s %8s %8s %10s %10s\n' profile read_ms write_ms riops wiops rbps wbps
  local p r w ri wi rb wb
  for p in none gp3 gp3-max; do
    read -r r w ri wi rb wb <<<"$(profile_values "$p")"
    printf '%-8s %8s %8s %8s %8s %10s %10s\n' "$p" "$r" "$w" "$ri" "$wi" "$(fmt_cap bps "$rb")" "$(fmt_cap bps "$wb")"
  done
}

# ---- helpers ----

require_root() {
  [ "$(id -u)" -eq 0 ] || die "must run as root (on macOS use colima.sh)"
}

# Checks the tools and kernel features every command needs, with install hints.
check_deps() {
  local missing=() tool
  for tool in dmsetup losetup blockdev mkfs.ext4 findmnt; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  if [ ${#missing[@]} -gt 0 ]; then
    die "missing ${missing[*]}: install with 'apt-get install dmsetup util-linux e2fsprogs' or 'dnf install device-mapper util-linux e2fsprogs'"
  fi
  modprobe dm_delay 2>/dev/null || grep -q '^delay' <(dmsetup targets) || die "kernel has no dm-delay target (CONFIG_DM_DELAY)"
  [ -f "$CGROUP_ROOT/cgroup.controllers" ] || die "cgroup v2 is not mounted at $CGROUP_ROOT"
  grep -qw io "$CGROUP_ROOT/cgroup.controllers" || die "the cgroup v2 io controller is not available"
}

# Warns when the kernel rounds short delays up to the timer tick: dm-delay
# uses a kthread for exact short delays only from Linux 6.8.
warn_kernel() {
  local major minor
  IFS=. read -r major minor _ <<<"$(uname -r)"
  if [ "$major" -lt 6 ] || { [ "$major" -eq 6 ] && [ "$minor" -lt 8 ]; }; then
    echo "dm-delay: warning: kernel $(uname -r) is older than 6.8, so delays round up to the timer tick; use check to see the real latency" >&2
  fi
}

# Parses SIZE (bytes, or with a K/M/G/T suffix, binary) to bytes; 0 and max
# give "max" (no cap).
to_bytes() {
  local v="$1" n unit
  case "$v" in max | 0) echo max; return ;; esac
  [[ "$v" =~ ^([0-9]+)([KkMmGgTt]?)i?[Bb]?$ ]] || die "bad size '$v'"
  n="${BASH_REMATCH[1]}"
  unit="${BASH_REMATCH[2]}"
  case "$unit" in
    K | k) n=$((n * 1024)) ;;
    M | m) n=$((n * 1024 ** 2)) ;;
    G | g) n=$((n * 1024 ** 3)) ;;
    T | t) n=$((n * 1024 ** 4)) ;;
  esac
  echo "$n"
}

to_count() {
  case "$1" in max | 0) echo max ;; *) [[ "$1" =~ ^[0-9]+$ ]] || die "bad count '$1'"; echo "$1" ;; esac
}

valid_name() {
  [[ "$1" =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "name must be lower-case letters, digits and dashes"
}

state_file() { echo "$STATE_DIR/$1.env"; }
dm_name() { echo "dimster-$1"; }

# Loads a volume's state into NAME, KIND, BACKING, MOUNT, FS, LOOP and the
# settings (READ_MS ... WBPS, CGROUPS).
load_state() {
  local f
  f="$(state_file "$1")"
  [ -f "$f" ] || die "no volume '$1' (no $f)"
  # shellcheck disable=SC1090
  . "$f"
}

save_state() {
  mkdir -p "$STATE_DIR"
  cat >"$(state_file "$NAME").tmp" <<EOF
NAME='$NAME'
KIND='$KIND'
BACKING='$BACKING'
MOUNT='$MOUNT'
FS='$FS'
LOOP='${LOOP:-}'
PROFILE='$PROFILE'
READ_MS='$READ_MS'
WRITE_MS='$WRITE_MS'
RIOPS='$RIOPS'
WIOPS='$WIOPS'
RBPS='$RBPS'
WBPS='$WBPS'
CGROUPS='$CGROUPS'
EOF
  mv "$(state_file "$NAME").tmp" "$(state_file "$NAME")"
}

dm_exists() { dmsetup info "$(dm_name "$1")" >/dev/null 2>&1; }

# MAJ:MIN of the volume's dm device.
dm_devno() {
  local dev
  dev="$(readlink -f "/dev/mapper/$(dm_name "$1")")"
  cat "/sys/class/block/$(basename "$dev")/dev"
}

# The device the delay table sits on: the loop device, or the partition.
lower_device() {
  if [ "$KIND" = file ]; then echo "$LOOP"; else echo "$BACKING"; fi
}

# The delay table for the current settings: reads, then writes (flushes
# follow writes).
delay_table() {
  local lower sectors
  lower="$(lower_device)"
  sectors="$(blockdev --getsz "$lower")"
  echo "0 $sectors delay $lower 0 $READ_MS $lower 0 $WRITE_MS"
}

# Resolves the cgroups io.max is set on: the volume's --cgroup list, or every
# auto-detected parent cgroup that exists. Prints absolute paths.
target_cgroups() {
  local c
  if [ -n "$CGROUPS" ]; then
    for c in $CGROUPS; do
      case "$c" in /*) echo "$c" ;; *) echo "$CGROUP_ROOT/$c" ;; esac
    done
  else
    for c in "${AUTO_CGROUPS[@]}"; do
      [ -d "$CGROUP_ROOT/$c" ] && echo "$CGROUP_ROOT/$c"
    done
  fi
  return 0
}

# Sets (or, with all caps max, clears) the volume's io.max on one cgroup.
# Written directly, also for systemd slices: systemctl set-property resolves
# a dm device to the device beneath it, and systemd leaves io.max entries it
# did not configure alone.
apply_io_max() {
  local cg="$1"
  [ -f "$cg/io.max" ] || die "$cg has no io.max (is the io controller enabled in its parent's cgroup.subtree_control?)"
  echo "$(dm_devno "$NAME") rbps=$RBPS wbps=$WBPS riops=$RIOPS wiops=$WIOPS" >"$cg/io.max"
}

apply_caps() {
  local cg applied=0
  while read -r cg; do
    [ -n "$cg" ] || continue
    apply_io_max "$cg"
    applied=1
  done < <(target_cgroups)
  if [ "$applied" -eq 0 ] && [ "$RIOPS$WIOPS$RBPS$WBPS" != maxmaxmaxmax ]; then
    echo "dm-delay: warning: no target cgroups found, so the caps apply nowhere; pass --cgroup" >&2
  fi
}

clear_caps() {
  local saved="$RIOPS $WIOPS $RBPS $WBPS"
  RIOPS=max WIOPS=max RBPS=max WBPS=max
  apply_caps
  read -r RIOPS WIOPS RBPS WBPS <<<"$saved"
}

# Loads the delay table with the current settings, live.
apply_delay() {
  local dm
  dm="$(dm_name "$NAME")"
  dmsetup suspend "$dm"
  dmsetup reload "$dm" --table "$(delay_table)" || { dmsetup resume "$dm"; die "failed to load the delay table"; }
  dmsetup resume "$dm"
}

attach() {
  if [ "$KIND" = file ]; then
    [ -f "$BACKING" ] || die "backing file $BACKING is missing"
    LOOP="$(losetup -j "$BACKING" | cut -d: -f1 | head -1)"
    [ -n "$LOOP" ] || LOOP="$(losetup --direct-io=on --show -f "$BACKING")"
  fi
  dm_exists "$NAME" || dmsetup create "$(dm_name "$NAME")" --table "$(delay_table)"
}

mount_volume() {
  mkdir -p "$MOUNT"
  findmnt -n "$MOUNT" >/dev/null 2>&1 || mount -o noatime "/dev/mapper/$(dm_name "$NAME")" "$MOUNT"
}

fmt_cap() {
  case "$2" in
    max) echo "-" ;;
    *) if [ "$1" = bps ]; then echo "$(($2 / 1024 / 1024))MiB/s"; else echo "$2"; fi ;;
  esac
}

describe() {
  echo "$NAME: profile=$PROFILE read=${READ_MS}ms write=${WRITE_MS}ms riops=$(fmt_cap iops "$RIOPS") wiops=$(fmt_cap iops "$WIOPS") rbps=$(fmt_cap bps "$RBPS") wbps=$(fmt_cap bps "$WBPS")"
}

# ---- commands ----

cmd_create() {
  local size="" fs=ext4 force=0 device=""
  NAME="${1:-}"; [ -n "$NAME" ] || die "create needs a NAME"; shift
  valid_name "$NAME"
  KIND=file BACKING="" MOUNT=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --size) size="$2"; shift 2 ;;
      --mount) MOUNT="$2"; shift 2 ;;
      --backing) BACKING="$2"; shift 2 ;;
      --device) device="$2"; shift 2 ;;
      --fs) fs="$2"; shift 2 ;;
      --force) force=1; shift ;;
      *) die "create: unknown argument $1" ;;
    esac
  done
  [ -n "$MOUNT" ] || die "create needs --mount"
  [ -f "$(state_file "$NAME")" ] && die "volume '$NAME' already exists"
  dm_exists "$NAME" && die "device $(dm_name "$NAME") already exists"
  case "$fs" in ext4) ;; xfs) command -v mkfs.xfs >/dev/null || die "mkfs.xfs not found (install xfsprogs)" ;; *) die "--fs must be ext4 or xfs" ;; esac
  findmnt -n "$MOUNT" >/dev/null 2>&1 && die "$MOUNT is already a mount point"
  if [ -n "$device" ]; then
    [ "$force" -eq 1 ] || die "--device destroys the data on $device; add --force"
    [ -b "$device" ] || die "$device is not a block device"
    KIND=device BACKING="$device"
  else
    [ -n "$size" ] || die "create needs --size (or --device)"
    BACKING="${BACKING:-$STATE_DIR/$NAME.img}"
    [ -e "$BACKING" ] && die "$BACKING already exists"
    mkdir -p "$(dirname "$BACKING")"
    truncate -s "$(to_bytes "$size")" "$BACKING"
  fi
  FS="$fs" PROFILE=none CGROUPS=""
  read -r READ_MS WRITE_MS RIOPS WIOPS RBPS WBPS <<<"$(profile_values none)"
  warn_kernel
  info "Attaching $BACKING as /dev/mapper/$(dm_name "$NAME")"
  attach
  info "Making $FS and mounting at $MOUNT"
  "mkfs.$FS" -q "/dev/mapper/$(dm_name "$NAME")" >/dev/null
  mount_volume
  save_state
  describe
}

cmd_up() {
  load_state "${1:?up needs a NAME}"
  attach
  apply_delay
  mount_volume
  apply_caps
  save_state
  describe
}

cmd_down() {
  load_state "${1:?down needs a NAME}"
  if dm_exists "$NAME"; then
    clear_caps
    if findmnt -n "$MOUNT" >/dev/null 2>&1; then
      umount "$MOUNT" || die "cannot unmount $MOUNT (in use? stop the pods or containers using it first)"
    fi
    dmsetup remove "$(dm_name "$NAME")"
  fi
  if [ "$KIND" = file ]; then
    local loop
    for loop in $(losetup -j "$BACKING" | cut -d: -f1); do losetup -d "$loop"; done
    LOOP=""
  fi
  save_state
  info "$NAME is down (data kept in $BACKING)"
}

cmd_set() {
  local profile="" reads_only=0 cgroups=() overrides=()
  load_state "${1:?set needs a NAME}"; shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --profile) profile="$2"; shift 2 ;;
      --read-ms | --write-ms | --riops | --wiops | --rbps | --wbps) overrides+=("$1" "$2"); shift 2 ;;
      --reads-only) reads_only=1; shift ;;
      --cgroup) cgroups+=("$2"); shift 2 ;;
      *) die "set: unknown argument $1" ;;
    esac
  done
  dm_exists "$NAME" || die "$NAME is down; run up first"
  # Caps move to the new cgroups, so clear them from the old ones first.
  if [ ${#cgroups[@]} -gt 0 ]; then
    clear_caps
    CGROUPS="${cgroups[*]}"
  fi
  if [ -n "$profile" ]; then
    read -r READ_MS WRITE_MS RIOPS WIOPS RBPS WBPS <<<"$(profile_values "$profile")"
    PROFILE="$profile"
  fi
  set -- "${overrides[@]+"${overrides[@]}"}"
  [ $# -gt 0 ] && PROFILE="${PROFILE}+custom"
  while [ $# -gt 0 ]; do
    case "$1" in
      --read-ms) [[ "$2" =~ ^[0-9]+$ ]] || die "bad --read-ms '$2'"; READ_MS="$2" ;;
      --write-ms) [[ "$2" =~ ^[0-9]+$ ]] || die "bad --write-ms '$2'"; WRITE_MS="$2" ;;
      --riops) RIOPS="$(to_count "$2")" ;;
      --wiops) WIOPS="$(to_count "$2")" ;;
      --rbps) RBPS="$(to_bytes "$2")" ;;
      --wbps) WBPS="$(to_bytes "$2")" ;;
    esac
    shift 2
  done
  if [ "$reads_only" -eq 1 ]; then
    WRITE_MS=0 WIOPS=max WBPS=max
    PROFILE="${PROFILE}+reads-only"
  fi
  apply_delay
  apply_caps
  save_state
  describe
}

cmd_status() {
  local f cg any=0
  for f in "$STATE_DIR"/*.env; do
    [ -f "$f" ] || continue
    # shellcheck disable=SC1090
    . "$f"
    [ -z "${1:-}" ] || [ "$NAME" = "$1" ] || continue
    any=1
    describe
    echo "  backing: $BACKING ($KIND, $FS)  mount: $MOUNT"
    if dm_exists "$NAME"; then
      echo "  device:  /dev/mapper/$(dm_name "$NAME") ($(dm_devno "$NAME")) table: $(dmsetup table "$(dm_name "$NAME")")"
      echo "  mounted: $(findmnt -n -o SOURCE,FSTYPE "$MOUNT" 2>/dev/null || echo no)"
      echo "  discard: $(lsblk -n -D -o DISC-GRAN "/dev/mapper/$(dm_name "$NAME")" | tr -d ' ' | sed 's/^0B$/not supported/')"
      while read -r cg; do
        [ -n "$cg" ] || continue
        echo "  io.max in $cg: $(grep "^$(dm_devno "$NAME") " "$cg/io.max" 2>/dev/null || echo none)"
      done < <(target_cgroups)
    else
      echo "  device:  down"
    fi
  done
  [ "$any" -eq 1 ] || echo "no volumes${1:+ named $1}"
  echo "kernel $(uname -r)"
}

# Runs "$@" in the check cgroup, which carries the volume's caps.
in_check_cgroup() {
  # shellcheck disable=SC2016
  bash -c 'echo $$ > "$0/cgroup.procs" && exec "$@"' "$CHECK_CG" "$@"
}

# Prints the mean ops per second of PAR parallel dd readers, each reading
# COUNT blocks of BS from its own region of the scratch file.
parallel_rate() {
  local bs="$1" count="$2" par="$3" scratch="$4" blocks start end i
  blocks="$(($(stat -c %s "$scratch") / $(to_bytes "$bs")))"
  start="$(date +%s%N)"
  for ((i = 0; i < par; i++)); do
    in_check_cgroup dd if="$scratch" of=/dev/null bs="$bs" count="$count" skip=$((i * blocks / par)) iflag=direct status=none &
  done
  wait
  end="$(date +%s%N)"
  echo $((par * count * 1000000000 / (end - start)))
}

cmd_check() {
  local scratch t0 t1 lat iops mbps
  load_state "${1:?check needs a NAME}"
  dm_exists "$NAME" || die "$NAME is down; run up first"
  describe
  scratch="$MOUNT/.dm-delay-check"
  CHECK_CG="$CGROUP_ROOT/dm-delay-check-$$"
  mkdir "$CHECK_CG"
  # expanded now: the locals are gone when the trap runs
  # shellcheck disable=SC2064
  trap "rm -f '$scratch'; rmdir '$CHECK_CG' 2>/dev/null || true" EXIT
  echo "$(dm_devno "$NAME") rbps=$RBPS wbps=$WBPS riops=$RIOPS wiops=$WIOPS" >"$CHECK_CG/io.max"
  info "Writing a 256 MiB scratch file (direct, under the caps)"
  in_check_cgroup dd if=/dev/zero of="$scratch" bs=1M count=256 oflag=direct status=none
  t0="$(date +%s%N)"
  in_check_cgroup dd if="$scratch" of=/dev/null bs=4k count=200 iflag=direct status=none
  t1="$(date +%s%N)"
  lat=$(((t1 - t0) / 200 / 1000))
  iops="$(parallel_rate 4k 500 16 "$scratch")"
  mbps="$(parallel_rate 1M 32 4 "$scratch")"
  printf '  4 KiB read latency: %d us (configured %d ms)\n' "$lat" "$READ_MS"
  printf '  4 KiB read IOPS (16 readers): %d (cap %s)\n' "$iops" "$(fmt_cap iops "$RIOPS")"
  printf '  1 MiB read throughput (4 readers): %d MiB/s (cap %s)\n' "$mbps" "$(fmt_cap bps "$RBPS")"
}

cmd_pv_yaml() {
  local node="" size="" count=1 node_path="" i
  load_state "${1:?pv-yaml needs a NAME}"; shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --node) node="$2"; shift 2 ;;
      --size) size="$2"; shift 2 ;;
      --count) count="$2"; shift 2 ;;
      --node-path) node_path="$2"; shift 2 ;;
      *) die "pv-yaml: unknown argument $1" ;;
    esac
  done
  if [ -z "$node" ] || [ -z "$size" ]; then die "pv-yaml needs --node and --size"; fi
  node_path="${node_path:-$MOUNT}"
  findmnt -n "$MOUNT" >/dev/null 2>&1 || die "$NAME is not mounted"
  cat <<EOF
# local-storage StorageClass and PVs on dm-delay volume '$NAME' ($MOUNT on
# the host, $node_path on node $node). Use with kafka.pvcStorageClass:
# local-storage. One volume's caps are shared by all its PVs.
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-storage
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
EOF
  for ((i = 0; i < count; i++)); do
    # The broker runs as uid 1000 with fsGroup 1000; open the directory so
    # it can write even where fsGroup is not applied.
    mkdir -p "$MOUNT/pv-$i"
    chmod 0777 "$MOUNT/pv-$i"
    cat <<EOF
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: dimster-$NAME-pv-$i
spec:
  capacity:
    storage: $size
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-storage
  local:
    path: $node_path/pv-$i
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["$node"]
EOF
  done
}

cmd_destroy() {
  local name="${1:?destroy needs a NAME}"
  [ "${2:-}" = --yes ] || die "destroy deletes the data of '$name'; add --yes"
  cmd_down "$name" >/dev/null
  load_state "$name"
  [ "$KIND" = file ] && rm -f "$BACKING"
  rm -f "$(state_file "$name")"
  info "$name destroyed"
}

main() {
  local cmd="${1:-help}"
  [ $# -gt 0 ] && shift
  case "$cmd" in
    help | -h | --help) usage; return ;;
    profiles) list_profiles; return ;;
  esac
  require_root
  check_deps
  case "$cmd" in
    create) cmd_create "$@" ;;
    up) cmd_up "$@" ;;
    down) cmd_down "$@" ;;
    set) cmd_set "$@" ;;
    status) cmd_status "$@" ;;
    check) cmd_check "$@" ;;
    pv-yaml) cmd_pv_yaml "$@" ;;
    destroy) cmd_destroy "$@" ;;
    *) usage >&2; exit 1 ;;
  esac
}

main "$@"
