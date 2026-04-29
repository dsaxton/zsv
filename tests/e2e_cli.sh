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
tmpdir="$(mktemp -d)"

cleanup() {
  rm -rf "$tmpdir"
}

trap cleanup EXIT

printf 'name,score\nAlice,9\n' > "$tmpdir/part1.csv"
printf 'name,score\nBob,8\nCara,10\n' > "$tmpdir/part2.csv"
printf 'name,points\nMismatch,1\n' > "$tmpdir/bad.csv"
{
  printf 'id,value\n'
  for i in $(seq 1 5000); do
    printf '%s,%s\n' "$i" "$i"
  done
} > "$tmpdir/many.csv"

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

TSV_INPUT=$'name\tscore\tdept\nAlice\t9\tEng\nBob\t8\tSales\nCara\t10\tEng\nDan\t7\tOps\n'
actual="$(printf '%s' "$TSV_INPUT" | "$exe" -d tab -s name,score -f 'dept=Eng')"
expected=$'name\tscore\nAlice\t9\nCara\t10'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] delimiter: tab select and filter"
else
  echo "[FAIL] delimiter: tab select and filter"
  echo "  expected:"
  printf '%s\n' "$expected" | sed 's/^/    /'
  echo "  actual:"
  printf '%s\n' "$actual" | sed 's/^/    /'
  failures=$((failures + 1))
fi

HEADERLESS_INPUT=$'Alice,9,Eng\nBob,8,Sales\nCara,10,Eng\nDan,7,Ops\n'
actual="$(printf '%s' "$HEADERLESS_INPUT" | "$exe" --input-no-header -s 1,3 -f '2>8')"
expected=$'Alice,Eng\nCara,Eng'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] input-no-header: first row is data"
else
  echo "[FAIL] input-no-header: first row is data"
  echo "  expected:"
  printf '%s\n' "$expected" | sed 's/^/    /'
  echo "  actual:"
  printf '%s\n' "$actual" | sed 's/^/    /'
  failures=$((failures + 1))
fi

actual="$(printf '%s' "$CSV_INPUT" | "$exe" --group-by dept --agg count:name --agg mean:score)"
expected=$'dept,count(name),mean(score)\nEng,2,9.5\nSales,1,8\nOps,1,7'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] group-by: aggregates by first-seen group order"
else
  echo "[FAIL] group-by: aggregates by first-seen group order"
  echo "  expected:"
  printf '%s\n' "$expected" | sed 's/^/    /'
  echo "  actual:"
  printf '%s\n' "$actual" | sed 's/^/    /'
  failures=$((failures + 1))
fi

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

actual="$($exe "$tmpdir/many.csv" | head -1)"
expected='id,value'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] pipe: downstream head closes cleanly"
else
  echo "[FAIL] pipe: downstream head closes cleanly"
  echo "  expected: $expected"
  echo "  got: $actual"
  failures=$((failures + 1))
fi

actual="$($exe "$tmpdir/part1.csv" "$tmpdir/part2.csv")"
expected=$'name,score\nAlice,9\nBob,8\nCara,10'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] file args: stacked CSVs share one header"
else
  echo "[FAIL] file args: stacked CSVs share one header"
  echo "  expected:"
  printf '%s\n' "$expected" | sed 's/^/    /'
  echo "  actual:"
  printf '%s\n' "$actual" | sed 's/^/    /'
  failures=$((failures + 1))
fi

actual="$(printf 'name,score\nAlice,9\n' | "$exe" - "$tmpdir/part2.csv")"
expected=$'name,score\nAlice,9\nBob,8\nCara,10'
if [[ "$actual" == "$expected" ]]; then
  echo "[PASS] file args: stdin can be mixed with files"
else
  echo "[FAIL] file args: stdin can be mixed with files"
  echo "  expected:"
  printf '%s\n' "$expected" | sed 's/^/    /'
  echo "  actual:"
  printf '%s\n' "$actual" | sed 's/^/    /'
  failures=$((failures + 1))
fi

if "$exe" "$tmpdir/part1.csv" "$tmpdir/bad.csv" >/dev/null 2>&1; then
  echo "[FAIL] file args: mismatched headers should fail"
  failures=$((failures + 1))
else
  echo "[PASS] file args: mismatched headers should fail"
fi

if [[ "$failures" -ne 0 ]]; then
  echo
  echo "$failures E2E case(s) failed"
  exit 1
fi

echo

echo "All E2E CLI cases passed"
