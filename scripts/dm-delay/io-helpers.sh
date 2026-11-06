#!/usr/bin/env bash
# Shared by the volume checker and integration tests so both measure real
# device I/O with the same dd implementation and propagate reader failures.

find_gnu_dd() {
  local candidate version
  # Some uutils dd versions use unaligned O_DIRECT buffers on loop/dm
  # devices. GNU dd aligns them; Ubuntu also installs it as gnudd.
  for candidate in dd gnudd gdd; do
    command -v "$candidate" >/dev/null 2>&1 || continue
    version="$(LC_ALL=C "$candidate" --version 2>/dev/null)" || continue
    case "$version" in
      *'(coreutils)'* | *'(GNU coreutils)'*) command -v "$candidate"; return ;;
    esac
  done
  echo "dm-delay: direct I/O checks require GNU dd; install GNU coreutils and expose dd, gnudd or gdd in PATH" >&2
  return 1
}

wait_for_io() {
  local pid failed=0
  # Bare wait hides failed readers. Reap every child before scratch cleanup,
  # but reject the measurement if any reader failed.
  for pid in "$@"; do
    wait "$pid" || failed=1
  done
  return "$failed"
}
