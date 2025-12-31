#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
oss_dir=${OSS_INNODB:-"$HOME/oss-embedded-innodb"}
verbose=0

usage() {
  cat <<USAGE
Usage: $(basename "$0") [--verbose]

Compares C and Go test suites, producing logs in test-logs/.
USAGE
}

if [[ $# -gt 1 ]]; then
  usage
  exit 1
fi

if [[ $# -eq 1 ]]; then
  case "$1" in
    --verbose)
      verbose=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
fi

if [[ ! -d "$oss_dir" ]]; then
  echo "OSS_INNODB not found at $oss_dir" >&2
  exit 1
fi

log_dir="$root_dir/test-logs"
mkdir -p "$log_dir"

ts=$(date +%Y%m%d_%H%M%S)
c_log="$log_dir/c-tests-$ts.log"
go_log="$log_dir/go-tests-$ts.log"
report="$log_dir/compare-$ts.txt"

c_tests=(
  ib_cfg
  ib_compressed
  ib_cursor
  ib_drop
  ib_ddl
  ib_dict
  ib_index
  ib_logger
  ib_mt_drv
  ib_mt_stress
  ib_recover
  ib_search
  ib_shutdown
  ib_status
  ib_tablename
  ib_test1
  ib_test2
  ib_test3
  ib_test5
  ib_types
  ib_update
  ib_zip
)

run_c_tests() {
  local start end rc
  local total=0
  local failed=0
  local fail_list=()
  local tmp_root

  tmp_root=$(mktemp -d)
  start=$(date +%s)

  for test_name in "${c_tests[@]}"; do
    local bin="$oss_dir/tests/$test_name"
    local test_dir="$tmp_root/$test_name"

    total=$((total + 1))
    mkdir -p "$test_dir"

    if [[ ! -x "$bin" ]]; then
      echo "[C] missing binary: $bin" >> "$c_log"
      failed=$((failed + 1))
      fail_list+=("$test_name (missing)")
      continue
    fi

    if [[ $verbose -eq 1 ]]; then
      (cd "$test_dir" && "$bin") 2>&1 | tee -a "$c_log"
      rc=${PIPESTATUS[0]}
    else
      (cd "$test_dir" && "$bin") >> "$c_log" 2>&1
      rc=$?
    fi

    if [[ $rc -ne 0 ]]; then
      failed=$((failed + 1))
      fail_list+=("$test_name (exit $rc)")
    fi
  done

  end=$(date +%s)
  echo "$total $failed $((end - start)) ${fail_list[*]:-}" > "$log_dir/.c_summary_$ts"
  rm -rf "$tmp_root"
}

run_go_tests() {
  local start end rc
  start=$(date +%s)

  if [[ $verbose -eq 1 ]]; then
    (cd "$root_dir" && go test -v ./...) 2>&1 | tee -a "$go_log"
    rc=${PIPESTATUS[0]}
  else
    (cd "$root_dir" && go test ./...) >> "$go_log" 2>&1
    rc=$?
  fi

  end=$(date +%s)
  echo "$rc $((end - start))" > "$log_dir/.go_summary_$ts"
}

run_c_tests
run_go_tests

read -r c_total c_failed c_time c_fail_list < "$log_dir/.c_summary_$ts"
read -r go_rc go_time < "$log_dir/.go_summary_$ts"

c_status="PASS"
if [[ "$c_failed" -ne 0 ]]; then
  c_status="FAIL"
fi

go_status="PASS"
if [[ "$go_rc" -ne 0 ]]; then
  go_status="FAIL"
fi

c_errors=$(grep -E "(ERROR|Error|FAIL|panic|assert|Assertion)" -n "$c_log" || true)
go_errors=$(grep -E "(ERROR|Error|FAIL|panic|assert|Assertion)" -n "$go_log" || true)

go_packages=$(grep -E "^(ok|\?)\s" "$go_log" | wc -l | tr -d ' ')

echo "C tests: $c_total" >> "$report"
echo "C status: $c_status" >> "$report"
echo "C failed: $c_failed" >> "$report"
echo "C time: ${c_time}s" >> "$report"
echo "C exit: $([[ "$c_failed" -eq 0 ]] && echo 0 || echo 1)" >> "$report"
echo "C log: $c_log" >> "$report"
if [[ -n "$c_fail_list" ]]; then
  echo "C failures: $c_fail_list" >> "$report"
fi

echo "" >> "$report"
echo "Go packages: $go_packages" >> "$report"
echo "Go status: $go_status" >> "$report"
echo "Go time: ${go_time}s" >> "$report"
echo "Go exit: $go_rc" >> "$report"
echo "Go log: $go_log" >> "$report"

echo "" >> "$report"
if [[ "$c_status" == "PASS" && "$go_status" == "PASS" && "$go_time" -gt 0 ]]; then
  speedup=$(awk "BEGIN { printf \"%.2f\", $c_time / $go_time }")
  echo "Speedup ratio (C/Go): $speedup" >> "$report"
fi

if [[ -n "$c_errors" ]]; then
  echo "" >> "$report"
  echo "C error patterns:" >> "$report"
  echo "$c_errors" >> "$report"
fi

if [[ -n "$go_errors" ]]; then
  echo "" >> "$report"
  echo "Go error patterns:" >> "$report"
  echo "$go_errors" >> "$report"
fi

cat "$report"

rm -f "$log_dir/.c_summary_$ts" "$log_dir/.go_summary_$ts"

if [[ "$c_status" == "FAIL" || "$go_status" == "FAIL" ]]; then
  exit 1
fi
