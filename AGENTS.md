# Project Guidelines

## Code Philosophy
- Keep things simple. When choosing between approaches, pick the one with fewer moving parts.
- Aim for elegance — code should feel clean and minimal, not bulky or over-built.
- Refactor while you're already in the code. Don't leave messes for a future cleanup pass.
- Maintain documentation (docstrings, docs files) alongside the code so knowledge doesn't evaporate between sessions.
- Let the code speak for itself. Avoid comments when good naming and structure make the intent obvious. When you're tempted to add a comment, see if restructuring the code would make it unnecessary.

## Architecture
- Single-file implementation: `src/main.zig` contains all logic and tests.
- Zero-allocation hot path: CSV parsing uses stack-allocated buffers and returns slices into the line buffer. No heap allocation per row.
- Buffered I/O: stdin is wrapped in a 256 KB buffered reader to minimize syscalls.
- Constants at top of file: `max_line_len` (1 MB), `max_fields` (4096), `read_buf_size` (256 KB), `table_sample_budget` (1 MB), `max_top_rows` (10,000).
- Unit tests live in the same file (idiomatic Zig). Run with `zig build test`.

## Build
- Use `ReleaseFast` for production (`zig build -Doptimize=ReleaseFast`). It is competitive with qsv on throughput.
- `ReleaseSmall` produces a ~56 KB binary but is 5-6x slower — only use if binary size is critical.
- Installed binary: `/usr/local/bin/zsv`

## Key Design Decisions
- `parseRecord` is zero-allocation: fast path returns slices into the line buffer for unquoted/simple-quoted fields; slow path copies into a caller-provided quote buffer only for fields with escaped quotes (`""`).
- `displayWidth` counts UTF-8 codepoints (not bytes) for table column alignment.
- Filter parsing trims whitespace around operators so expressions like `"Total Amount > 100"` work naturally with spaced column names.
- `--top` uses a bounded selection buffer capped at `max_top_rows` (10,000) to prevent unbounded memory growth from large `-n` values. The cap is enforced at argument parse time. `-n` without `--top` is uncapped since it streams without buffering.
