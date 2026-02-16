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

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "$failures E2E case(s) failed"
  exit 1
fi

echo

echo "All E2E CLI cases passed"
