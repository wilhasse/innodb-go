#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
oss_dir=${OSS_INNODB:-"$HOME/oss-embedded-innodb"}

if [[ ! -d "$oss_dir" ]]; then
  echo "OSS_INNODB not found at $oss_dir" >&2
  exit 1
fi

c_src="$root_dir/tools/c/btr_trace.c"
c_bin="$root_dir/tools/c/btrtrace"
go_trace=$(mktemp)
c_trace=$(mktemp)
run_dir=$(mktemp -d)

cleanup() {
  rm -f "$go_trace" "$c_trace"
  rm -rf "$run_dir"
}
trap cleanup EXIT

cc \
  -I"$oss_dir/include" \
  -I"$oss_dir/tests" \
  "$c_src" \
  "$oss_dir/tests/test0aux.c" \
  -L"$oss_dir/.libs" \
  -Wl,-rpath,"$oss_dir/.libs" \
  -linnodb -lpthread -lm -lz \
  -o "$c_bin"

( cd "$root_dir" && go run ./cmd/btrtrace > "$go_trace" )
( cd "$run_dir" && "$c_bin" > "$c_trace" )

if diff -u "$go_trace" "$c_trace"; then
  echo "trace match"
else
  echo "trace mismatch" >&2
  exit 1
fi
