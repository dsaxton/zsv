#!/usr/bin/env bash
set -euo pipefail

exe="${1:?usage: e2e_cli.sh /path/to/zsv}"

CSV_INPUT='name,score,dept
Alice,9,Eng
Bob,8,Sales
Cara,10,Eng
Dan,7,Ops
'

failures=0

run_case() {
  local name="$1"
  local expected="$2"
  shift 2

  local actual
  actual="$(printf '%s' "$CSV_INPUT" | "$exe" "$@")"
  actual+=$'\n'

  if [[ "$actual" != "$expected" ]]; then
    echo "[FAIL] $name"
    echo "  cmd: $exe $*"
    echo "  expected:"
    printf '%s' "$expected" | sed 's/^/    /'
    echo "  actual:"
    printf '%s' "$actual" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "[PASS] $name"
  fi
}

run_case \
  "top+table one header" \
  $'name  | score\n------+------\nCara  | 10   \nAlice | 9    \nBob   | 8    \nDan   | 7    \n' \
  --top score -t -s name,score -n 4

run_case \
  "top+csv one header" \
  $'name,score\nCara,10\nAlice,9\nBob,8\nDan,7\n' \
  --top score -s name,score -n 4

run_case \
  "top+table+no-header" \
  $'Cara  | 10   \nAlice | 9    \nBob   | 8    \nDan   | 7    \n' \
  --top score -t --no-header -s name,score -n 4

run_case \
  "top+csv+no-header" \
  $'Cara,10\nAlice,9\nBob,8\nDan,7\n' \
  --top score --no-header -s name,score -n 4

run_case \
  "top+filter+select" \
  $'name,score\nCara,10\nAlice,9\n' \
  --top score -f dept=Eng -s name,score -n 2

run_case \
  "top+filter+table" \
  $'name  | score\n------+------\nCara  | 10   \nAlice | 9    \n' \
  --top score -f dept=Eng -t -s name,score -n 2

run_case \
  "top+head-default when -n has no value" \
  $'name,score\nCara,10\nAlice,9\nBob,8\nDan,7\n' \
  --top score -n -s name,score

run_case \
  "table without top has single header" \
  $'name  | score\n------+------\nAlice | 9    \nBob   | 8    \nCara  | 10   \nDan   | 7    \n' \
  -t -s name,score

# Test --sample: produces exactly 3 lines (1 header + 2 data rows)
actual="$(printf '%s' "$CSV_INPUT" | "$exe" --sample 2)"
line_count="$(echo "$actual" | grep -c "")"
if [[ "$line_count" -eq 3 ]]; then
  echo "[PASS] sample: 2 rows produces 3 lines"
else
  echo "[FAIL] sample: 2 rows produces 3 lines (got $line_count lines)"
  failures=$((failures + 1))
fi

# Test --sample: header matches original header
actual_header="$(printf '%s' "$CSV_INPUT" | "$exe" --sample 2 | head -1)"
expected_header="name,score,dept"
if [[ "$actual_header" == "$expected_header" ]]; then
  echo "[PASS] sample: header matches original"
else
  echo "[FAIL] sample: header matches original"
  echo "  expected: $expected_header"
  echo "  got: $actual_header"
  failures=$((failures + 1))
fi

# Test --sample with -s: header is name,score
actual_header="$(printf '%s' "$CSV_INPUT" | "$exe" --sample 2 -s name,score | head -1)"
expected_header="name,score"
if [[ "$actual_header" == "$expected_header" ]]; then
  echo "[PASS] sample: selected columns header is correct"
else
  echo "[FAIL] sample: selected columns header is correct"
  echo "  expected: $expected_header"
  echo "  got: $actual_header"
  failures=$((failures + 1))
fi

# Test --sample with -t: output contains table separator
actual="$(printf '%s' "$CSV_INPUT" | "$exe" --sample 2 -t)"
if printf '%s' "$actual" | grep -q -- '--'; then
  echo "[PASS] sample: table output contains separator"
else
  echo "[FAIL] sample: table output contains separator"
  failures=$((failures + 1))
fi

# Test --sample: N > row count returns all rows
actual="$(printf '%s' "$CSV_INPUT" | "$exe" --sample 100)"
actual_line_count="$(echo "$actual" | grep -c "")"
if [[ "$actual_line_count" -eq 5 ]]; then
  echo "[PASS] sample: 100 rows from 4-row data returns 5 lines"
else
  echo "[FAIL] sample: 100 rows from 4-row data returns 5 lines (got $actual_line_count)"
  failures=$((failures + 1))
fi

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "$failures E2E case(s) failed"
  exit 1
fi

echo

echo "All E2E CLI cases passed"
