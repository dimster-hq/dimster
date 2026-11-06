#!/usr/bin/env bash
# Exercises executable selection and child failures without root or devices,
# so regressions in the check's direct-I/O plumbing can be tested anywhere.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=io-helpers.sh
. "$here/io-helpers.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fail() { echo "FAIL  $*" >&2; exit 1; }
pass() { echo "ok    $*"; }

# Each fixture is a separate PATH, independent of the host's dd packages.
mock_dd() {
  local dir="$1" name="$2" banner="$3"
  mkdir -p "$dir"
  printf '#!/bin/bash\n[ "$1" = --version ] || exit 1\nprintf "%%s\\n" %q\n' "$banner" >"$dir/$name"
  chmod +x "$dir/$name"
}

assert_selected() {
  local dir="$1" name="$2" selected
  selected="$(PATH="$dir" find_gnu_dd)"
  [ "$selected" = "$dir/$name" ] || fail "selected '$selected', expected '$dir/$name'"
  pass "$name selected in ${dir##*/}"
}

mock_dd "$tmp/gnu-default" dd 'dd (coreutils) 9.7'
mock_dd "$tmp/gnu-default" gnudd 'dd (coreutils) 9.7'
assert_selected "$tmp/gnu-default" dd

mock_dd "$tmp/uutils-default" dd 'dd (uutils coreutils) 0.8.0'
mock_dd "$tmp/uutils-default" gnudd 'dd (coreutils) 9.7'
assert_selected "$tmp/uutils-default" gnudd

mock_dd "$tmp/gnu-prefixed" dd 'dd (uutils coreutils) 0.8.0'
mock_dd "$tmp/gnu-prefixed" gdd 'dd (GNU coreutils) 9.7'
assert_selected "$tmp/gnu-prefixed" gdd

mock_dd "$tmp/no-default" gnudd 'dd (coreutils) 9.7'
assert_selected "$tmp/no-default" gnudd

mock_dd "$tmp/unsupported" dd 'dd (uutils coreutils) 0.8.0'
mock_dd "$tmp/unsupported" gdd 'BusyBox v1.36.0'
if PATH="$tmp/unsupported" find_gnu_dd >"$tmp/output" 2>"$tmp/error"; then
  fail "unsupported dd accepted"
fi
[ ! -s "$tmp/output" ] || fail "failed selection printed a command"
grep -q 'require GNU dd' "$tmp/error" || fail "missing dependency hint"
pass "unsupported dd rejected with a dependency hint"

# The first reader fails; the second must finish before cleanup can run.
(exit 1) & first=$!
(sleep 0.05; touch "$tmp/finished") & second=$!
if wait_for_io "$first" "$second"; then fail "failed reader ignored"; fi
[ -f "$tmp/finished" ] || fail "returned before all readers finished"
pass "reader failure propagated after all readers finished"

(exit 0) & first=$!
(exit 0) & second=$!
wait_for_io "$first" "$second" || fail "successful readers rejected"
pass "successful readers accepted"

echo PASS
