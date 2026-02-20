# zsv

A fast, constant-memory CSV processor. Reads CSV from stdin, applies column selection and row filtering, and writes results to stdout.

Written in Zig.

## Build

Requires [Zig](https://ziglang.org/) 0.14.1.

```
zig build -Doptimize=ReleaseFast
```

The binary is placed at `zig-out/bin/zsv`.

`ReleaseFast` is recommended for production use. `ReleaseSmall` produces a much smaller binary (~56 KB vs ~2.3 MB) but is 5-6x slower. The default (debug) build is significantly slower than either release mode.

Run the test suite:

```
zig build test
```

Run only end-to-end CLI tests:

```
zig build test-e2e
```

If you use `just`, equivalent commands are:

```
just test
just test-unit
just test-e2e
```

## Usage

```
zsv [OPTIONS]
```

All input is read from stdin and output is written to stdout.

### Options

| Flag | Description |
|---|---|
| `-s, --select FIELDS` | Comma-separated column names or 1-based indices |
| `-f, --filter EXPR` | Filter expression (repeatable; multiple filters are ANDed) |
| `-n, --head [N]` | Output first N data rows (after filtering). If N is omitted, defaults to 10 |
| `--top FIELD` | Output top rows by FIELD (descending). Use with `-n` to set count (defaults to 10 when omitted) |
| `--agg FUNC:FIELD` | Aggregate FIELD; FUNC: sum, min, max, count, mean. Repeatable; incompatible with `--top` and `--head` |
| `-t, --table` | Pretty-print output as an aligned table |
| `--no-header` | Suppress header row in output |
| `-h, --help` | Print help message |

### Examples

Pass through a CSV file:

```sh
zsv < data.csv
```

Select specific columns by name:

```sh
zsv -s name,age < data.csv
```

Select by 1-based index:

```sh
zsv -s 1,3 < data.csv
```

Filter rows:

```sh
zsv -f "age>30" < data.csv
```

Head (first N matching rows):

```sh
zsv -n 100 -f "status=active" < data.csv
```

Top N by a column (descending):

```sh
zsv --top salary -n 20 -s name,salary < employees.csv
```

Top 10 by default when `-n` is present without a value:

```sh
zsv --top salary -n -s name,salary < employees.csv
```

Aggregate columns (sum, min, max, count, mean):

```sh
zsv --agg sum:amount --agg count:id < data.csv
```

Spaces around the operator are allowed:

```sh
zsv -f "Total Amount > 0.1" < data.csv
```

Combine select and filter:

```sh
zsv -s name,salary -f "department=Engineering" -f "salary>=100000" < employees.csv
```

Pretty-print as a table:

```sh
zsv -t < data.csv
```

Table with select and filter:

```sh
zsv -t -s name,salary -f "salary>=100000" < employees.csv
```

Column widths are estimated by buffering up to 1 MB of row data. Later values that exceed the estimated width are not truncated but may cause misalignment. Multi-byte UTF-8 characters are measured by display column (codepoint count), not byte length, so non-ASCII text aligns correctly.

Glob filter (prefix match):

```sh
zsv -f "city~New*" < data.csv
```

Glob filter (contains):

```sh
zsv -f "city~*York*" < data.csv
```

Suppress the header row:

```sh
zsv --no-header -s name < data.csv
```

Page through results with `less`:

```sh
zsv -t < data.csv | less -S
```

The `-S` flag disables line wrapping, which keeps table columns aligned.

Pipe with other tools:

```sh
curl -s https://example.com/data.csv | zsv -f "status=active" -s id,name | wc -l
```

### Filter operators

| Operator | Meaning |
|---|---|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |
| `~` | Glob match (`*` matches any sequence of characters) |

The `~` operator is always string-based and supports `*` wildcards: `name~Alice` (exact), `city~New*` (prefix), `city~*York` (suffix), `city~*ew*` (contains). All other filters attempt numeric comparison first. If both sides parse as numbers, the comparison is numeric; otherwise it falls back to lexicographic string comparison.

Column names with spaces work in filter expressions. Whitespace around the operator is trimmed, so `"Total Amount > 100"` correctly references the column `Total Amount`.

## Limitations

- Input must be read from stdin (no filename argument).
- The first row is always treated as a header. There is no headerless-input mode.
- Maximum line length is 1 MB. Lines exceeding this limit produce an error.
- Maximum fields per row is 4096. Rows exceeding this limit produce an error.
- Newlines within quoted fields are not supported (the parser splits on `\n` before parsing fields).
- Empty lines in the input are silently skipped.
- Filter values cannot contain the operator characters (`=`, `<`, `>`, `!`, `~`) since the parser splits on the first operator it finds in the expression.
- In transform modes (`--select`, `--filter`, or `--table`), malformed quoted fields (e.g. unterminated quotes or non-delimiter content after a closing quote) produce an error.

## Error handling

- CSV parse errors are reported to stderr with the line number and a short reason, and the process exits with a non-zero status.
- The no-transform pass-through path (`zsv < file.csv` with no `--select`, `--filter`, or `--table`) streams lines without CSV field parsing, so malformed CSV rows are passed through as-is in that mode.

## Possible future enhancements

- Read from a file argument instead of only stdin.
- Custom field delimiter (`-d '\t'` for TSV, `-d '|'` for pipe-delimited, etc.).
- Case-insensitive filtering.
- Sorting by one or more columns.
- Support newlines within quoted fields (multi-line records).
