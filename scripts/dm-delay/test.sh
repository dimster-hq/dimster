#!/usr/bin/env bash
# Integration test for dm-delay.sh: builds a small delay volume in a temp
# directory and checks that latency, IOPS and throughput settings take
# effect, that settings survive down/up, and that destroy leaves nothing
# behind. Caps are applied to a private test cgroup (--cgroup), never to the
# cgroups of running containers. Run as root on Linux, or on macOS with
# colima.sh test.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
tmp="$(mktemp -d /var/tmp/dm-delay-test.XXXXXX)"
export DM_DELAY_STATE_DIR="$tmp/state"
name="dmtest$$"
mnt="$tmp/mnt"
cg="/sys/fs/cgroup/dm-delay-test-$$"
failures=0

dmd() { "$here/dm-delay.sh" "$@"; }

cleanup() {
  dmd destroy "$name" --yes >/dev/null 2>&1 || true
  rmdir "$cg" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

pass() { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; failures=$((failures + 1)); }

# Runs "$@" inside the test cgroup, where the volume's caps apply.
in_cg() {
  # shellcheck disable=SC2016
  bash -c 'echo $$ > "$0/cgroup.procs" && exec "$@"' "$cg" "$@"
}

now_ns() { date +%s%N; }

# Mean 4 KiB direct read latency in microseconds, from one reader.
latency_us() {
  local t0 t1
  t0="$(now_ns)"
  in_cg dd if="$mnt/data" of=/dev/null bs=4k count="$1" iflag=direct status=none
  t1="$(now_ns)"
  echo $(((t1 - t0) / $1 / 1000))
}

# Ops per second of PAR parallel direct readers reading COUNT blocks of BS.
rate() {
  local bs="$1" count="$2" par="$3" t0 t1 i
  t0="$(now_ns)"
  for ((i = 0; i < par; i++)); do
    in_cg dd if="$mnt/data" of=/dev/null bs="$bs" count="$count" skip=$((i * count)) iflag=direct status=none &
  done
  wait
  t1="$(now_ns)"
  echo $((par * count * 1000000000 / (t1 - t0)))
}

in_range() { [ "$1" -ge "$2" ] && [ "$1" -le "$3" ]; }

devno() { cat "/sys/class/block/$(basename "$(readlink -f "/dev/mapper/dimster-$name")")/dev"; }

[ "$(id -u)" -eq 0 ] || { echo "test.sh: run as root (or colima.sh test)" >&2; exit 1; }
mkdir "$cg"

echo "== create"
dmd create "$name" --size 512M --mount "$mnt" >/dev/null
if findmnt -n "$mnt" >/dev/null && [ -b "/dev/mapper/dimster-$name" ]; then pass "volume created and mounted"; else fail "volume not mounted"; fi
dd if=/dev/urandom of="$mnt/data" bs=1M count=128 oflag=direct status=none
sha="$(sha256sum "$mnt/data" | cut -d' ' -f1)"

echo "== latency"
dmd set "$name" --read-ms 5 --cgroup "$cg" >/dev/null
lat="$(latency_us 100)"
if in_range "$lat" 4500 8000; then pass "read-ms 5 gives ${lat} us"; else fail "read-ms 5 gave ${lat} us (want 4500-8000)"; fi
dmd set "$name" --profile none >/dev/null
lat="$(latency_us 500)"
if in_range "$lat" 0 999; then pass "profile none gives ${lat} us"; else fail "profile none gave ${lat} us (want < 1000)"; fi

echo "== IOPS cap"
dmd set "$name" --riops 200 >/dev/null
iops="$(rate 4k 60 16)"
if in_range "$iops" 150 230; then pass "riops 200 gives ${iops} IOPS"; else fail "riops 200 gave ${iops} IOPS (want 150-230)"; fi

echo "== throughput cap"
dmd set "$name" --riops 0 --rbps 20M >/dev/null
ops="$(rate 1M 16 4)"
if in_range "$ops" 15 23; then pass "rbps 20M gives ${ops} MiB/s"; else fail "rbps 20M gave ${ops} MiB/s (want 15-23)"; fi

echo "== profile and reads-only"
dmd set "$name" --profile gp3 --reads-only >/dev/null
table="$(dmsetup table "dimster-$name")"
# reads 1 ms, writes 0 ms: "... delay <dev> 0 1 <dev> 0 0"
if [[ "$table" =~ delay\ [0-9:]+\ 0\ 1\ [0-9:]+\ 0\ 0$ ]]; then pass "gp3 --reads-only table: $table"; else fail "gp3 --reads-only table: $table"; fi
if grep -q "^$(devno) rbps=131072000 wbps=max riops=3000 wiops=max" "$cg/io.max"; then pass "gp3 --reads-only caps in io.max"; else fail "io.max: $(cat "$cg/io.max")"; fi
out="$(dmd check "$name")"
if grep -q 'read latency' <<<"$out" && ! ls -d /sys/fs/cgroup/dm-delay-check-* >/dev/null 2>&1; then
  pass "check reports measurements and removes its cgroup"
else
  fail "check: $out"
fi

echo "== down and up"
dmd down "$name" >/dev/null
if ! findmnt -n "$mnt" >/dev/null && ! [ -e "/dev/mapper/dimster-$name" ]; then pass "down unmounts and removes the device"; else fail "down left the volume attached"; fi
if ! grep -q "riops=3000" "$cg/io.max" 2>/dev/null; then pass "down clears the caps"; else fail "down left caps: $(cat "$cg/io.max")"; fi
dmd up "$name" >/dev/null
if [ "$(sha256sum "$mnt/data" | cut -d' ' -f1)" = "$sha" ]; then pass "data survives down/up"; else fail "data changed across down/up"; fi
if grep -q "^$(devno) rbps=131072000 wbps=max riops=3000 wiops=max" "$cg/io.max"; then pass "up re-applies the caps"; else fail "after up io.max: $(cat "$cg/io.max")"; fi

echo "== pv-yaml"
yaml="$(dmd pv-yaml "$name" --node test-node --size 1Gi --count 2)"
if [ "$(grep -c '^kind: PersistentVolume$' <<<"$yaml")" -eq 2 ] && [ -d "$mnt/pv-1" ] && grep -q "path: $mnt/pv-0" <<<"$yaml"; then pass "pv-yaml prints 2 PVs and creates their directories"; else fail "pv-yaml output"; fi

echo "== destroy"
backing="$DM_DELAY_STATE_DIR/$name.img"
dmd destroy "$name" --yes >/dev/null
if ! [ -e "/dev/mapper/dimster-$name" ] && [ -z "$(losetup -j "$backing")" ] && ! [ -e "$backing" ] && ! [ -e "$DM_DELAY_STATE_DIR/$name.env" ]; then
  pass "destroy removes device, loop, backing file and state"
else
  fail "destroy left something behind"
fi

if [ "$failures" -eq 0 ]; then echo "PASS"; else echo "$failures FAILED"; exit 1; fi
