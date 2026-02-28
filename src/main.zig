const std = @import("std");

const max_line_len: usize = 1024 * 1024;
const max_fields: usize = 4096;
const read_buf_size: usize = 256 * 1024;
const table_sample_budget: usize = 1024 * 1024;
const default_head_rows: usize = 10;
const max_top_rows: usize = 10_000;

const AggFunc = enum { sum, min, max, count, mean };

const Agg = struct {
    func: AggFunc,
    field: []const u8,
    col_index: ?usize = null,
    total: f64 = 0,
    extreme: f64 = 0,
    n: usize = 0,
    tainted: bool = false,
};

const FilterOp = enum {
    eq,
    neq,
    lt,
    gt,
    lte,
    gte,
    like,
};

const Filter = struct {
    field: []const u8,
    op: FilterOp,
    value: []const u8,
    col_index: ?usize = null,
    value_num_parsed: bool = false,
    value_num: ?f64 = null,
};

const Config = struct {
    selectors: ?[]const []const u8 = null,
    filters: ?[]Filter = null,
    aggs: ?[]Agg = null,
    head: ?usize = null,
    top_field: ?[]const u8 = null,
    sample: ?usize = null,
    no_header: bool = false,
    table: bool = false,
};

const ParseRecordError = error{
    TooManyFields,
    UnterminatedQuote,
    MalformedQuotedField,
};

fn parseRecordErrorMessage(err: ParseRecordError) []const u8 {
    return switch (err) {
        error.TooManyFields => "too many fields in row",
        error.UnterminatedQuote => "unterminated quoted field",
        error.MalformedQuotedField => "malformed quoted field",
    };
}

fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        \\Usage: zsv [OPTIONS]
        \\
        \\Reads CSV from stdin and writes to stdout.
        \\
        \\Options:
        \\  -s, --select FIELDS   Comma-separated column names or 1-based indices
        \\  -f, --filter EXPR     Filter expression: field op value
        \\                        Operators: =, !=, <, >, <=, >=, ~ (glob)
        \\                        Repeatable (multiple filters = AND)
        \\  -n, --head [N]        Output first N data rows (after filtering; default 10 when omitted)
        \\      --top FIELD       Output top rows by FIELD (desc); use -n for count (max 10000)
        \\      --sample N        Output uniform random sample of N rows (after filtering)
        \\      --agg FUNC:FIELD  Aggregate FIELD; FUNC: sum, min, max, count, mean
        \\                        Repeatable; incompatible with --top and --head
        \\  -t, --table           Pretty-print as aligned table
        \\      --no-header       Suppress header row in output
        \\  -h, --help            Print this help message
        \\
    );
}

fn parseArgsList(args: []const []const u8, allocator: std.mem.Allocator) !?Config {
    var selectors = std.ArrayList([]const u8).init(allocator);
    var filters = std.ArrayList(Filter).init(allocator);
    var aggs = std.ArrayList(Agg).init(allocator);
    var head: ?usize = null;
    var top_field: ?[]const u8 = null;
    var sample: ?usize = null;
    var no_header = false;
    var table = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            const stdout = std.io.getStdOut().writer();
            try printUsage(stdout);
            return null;
        } else if (std.mem.eql(u8, arg, "-t") or std.mem.eql(u8, arg, "--table")) {
            table = true;
        } else if (std.mem.eql(u8, arg, "--no-header")) {
            no_header = true;
        } else if (std.mem.eql(u8, arg, "-s") or std.mem.eql(u8, arg, "--select")) {
            if (i + 1 >= args.len) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --select requires an argument\n");
                return null;
            }
            i += 1;
            const value = args[i];
            var it = std.mem.splitScalar(u8, value, ',');
            while (it.next()) |field| {
                if (field.len > 0) {
                    try selectors.append(try allocator.dupe(u8, field));
                }
            }
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--filter")) {
            if (i + 1 >= args.len) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --filter requires an argument\n");
                return null;
            }
            i += 1;
            const value = args[i];
            const parsed_filter = parseFilter(value) orelse {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error: invalid filter expression: {s}\n", .{value});
                return null;
            };
            var filter = parsed_filter;
            filter.field = try allocator.dupe(u8, parsed_filter.field);
            filter.value = try allocator.dupe(u8, parsed_filter.value);
            try filters.append(filter);
        } else if (std.mem.eql(u8, arg, "-n") or std.mem.eql(u8, arg, "--head")) {
            if (i + 1 >= args.len) {
                head = default_head_rows;
            } else {
                const value = args[i + 1];
                head = std.fmt.parseInt(usize, value, 10) catch {
                    if (value.len > 0 and value[0] == '-') {
                        head = default_head_rows;
                        continue;
                    }
                    const stderr = std.io.getStdErr().writer();
                    try stderr.print("Error: invalid --head value: {s}\n", .{value});
                    return null;
                };
                i += 1;
            }
        } else if (std.mem.eql(u8, arg, "--agg")) {
            if (i + 1 >= args.len) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --agg requires an argument\n");
                return null;
            }
            i += 1;
            const value = args[i];
            const parsed_agg = parseAgg(value) orelse {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error: invalid --agg expression: {s}\n", .{value});
                return null;
            };
            var agg = parsed_agg;
            agg.field = try allocator.dupe(u8, parsed_agg.field);
            try aggs.append(agg);
        } else if (std.mem.eql(u8, arg, "--top")) {
            if (i + 1 >= args.len) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --top requires an argument\n");
                return null;
            }
            i += 1;
            const value = args[i];
            top_field = try allocator.dupe(u8, value);
        } else if (std.mem.eql(u8, arg, "--sample")) {
            if (i + 1 >= args.len) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --sample requires an argument\n");
                return null;
            }
            i += 1;
            const n = std.fmt.parseInt(usize, args[i], 10) catch {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error: invalid --sample value: {s}\n", .{args[i]});
                return null;
            };
            if (n == 0) {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --sample must be >= 1\n");
                return null;
            }
            sample = n;
        } else {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("Error: unknown argument: {s}\n", .{arg});
            try printUsage(stderr);
            return null;
        }
    }

    if (top_field != null) {
        const limit = head orelse default_head_rows;
        if (limit > max_top_rows) {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("Error: --top -n {d} exceeds maximum of {d}\n", .{ limit, max_top_rows });
            return null;
        }
    }

    if (aggs.items.len > 0) {
        if (top_field != null) {
            const stderr = std.io.getStdErr().writer();
            try stderr.writeAll("Error: --agg cannot be combined with --top\n");
            return null;
        }
        if (head != null) {
            const stderr = std.io.getStdErr().writer();
            try stderr.writeAll("Error: --agg cannot be combined with --head\n");
            return null;
        }
    }

    if (sample) |_| {
        if (top_field != null) {
            const stderr = std.io.getStdErr().writer();
            try stderr.writeAll("Error: --sample cannot be combined with --top\n");
            return null;
        }
        if (aggs.items.len > 0) {
            const stderr = std.io.getStdErr().writer();
            try stderr.writeAll("Error: --sample cannot be combined with --agg\n");
            return null;
        }
        if (head != null) {
            const stderr = std.io.getStdErr().writer();
            try stderr.writeAll("Error: --sample cannot be combined with --head\n");
            return null;
        }
    }

    return Config{
        .selectors = if (selectors.items.len > 0) selectors.items else null,
        .filters = if (filters.items.len > 0) filters.items else null,
        .aggs = if (aggs.items.len > 0) aggs.items else null,
        .head = head,
        .top_field = top_field,
        .sample = sample,
        .no_header = no_header,
        .table = table,
    };
}

fn parseArgs(allocator: std.mem.Allocator) !?Config {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    if (args.len == 0) return null;
    return parseArgsList(args, allocator);
}

fn parseFilter(expr: []const u8) ?Filter {
    const ops = [_]struct { text: []const u8, op: FilterOp }{
        .{ .text = "!=", .op = .neq },
        .{ .text = "<=", .op = .lte },
        .{ .text = ">=", .op = .gte },
        .{ .text = "=", .op = .eq },
        .{ .text = "~", .op = .like },
        .{ .text = "<", .op = .lt },
        .{ .text = ">", .op = .gt },
    };

    for (ops) |entry| {
        if (std.mem.indexOf(u8, expr, entry.text)) |pos| {
            if (pos == 0) continue; // No field name.
            const field = std.mem.trim(u8, expr[0..pos], " ");
            const value = std.mem.trim(u8, expr[pos + entry.text.len ..], " ");
            const value_num = std.fmt.parseFloat(f64, value) catch null;
            return Filter{
                .field = field,
                .op = entry.op,
                .value = value,
                .value_num_parsed = true,
                .value_num = value_num,
            };
        }
    }
    return null;
}

fn parseAgg(expr: []const u8) ?Agg {
    const colon = std.mem.indexOfScalar(u8, expr, ':') orelse return null;
    if (colon == 0 or colon + 1 >= expr.len) return null;
    const func_str = expr[0..colon];
    const field = expr[colon + 1 ..];
    const func: AggFunc = if (std.mem.eql(u8, func_str, "sum")) .sum else if (std.mem.eql(u8, func_str, "min")) .min else if (std.mem.eql(u8, func_str, "max")) .max else if (std.mem.eql(u8, func_str, "count")) .count else if (std.mem.eql(u8, func_str, "mean")) .mean else return null;
    return Agg{ .func = func, .field = field };
}

/// Parses a CSV line into fields. Returns slices into `line` for unquoted fields
/// and slices into `quote_buf` for quoted fields containing escaped quotes.
fn parseRecord(line: []const u8, out: [][]const u8, quoted: []bool, quote_buf: []u8) ParseRecordError!struct { fields: [][]const u8, quoted: []const bool, quote_buf_used: usize } {
    var n: usize = 0;
    var qb_used: usize = 0;
    var i: usize = 0;

    while (i <= line.len) {
        if (i == line.len) {
            if (line.len > 0 and line[line.len - 1] == ',') {
                if (n >= out.len) return error.TooManyFields;
                out[n] = "";
                quoted[n] = false;
                n += 1;
            }
            break;
        }

        if (n >= out.len) return error.TooManyFields;

        if (line[i] == '"') {
            i += 1;
            // Check if the quoted field contains escaped quotes.
            var has_escapes = false;
            var scan = i;
            while (scan < line.len) {
                if (line[scan] == '"') {
                    if (scan + 1 < line.len and line[scan + 1] == '"') {
                        has_escapes = true;
                        scan += 2;
                    } else {
                        break;
                    }
                } else {
                    scan += 1;
                }
            }
            if (scan >= line.len) return error.UnterminatedQuote;

            if (!has_escapes) {
                // Fast path: slice directly into line.
                out[n] = line[i..scan];
                quoted[n] = true;
                n += 1;
                i = scan + 1; // skip closing quote
                if (i == line.len) {
                    break;
                } else if (line[i] == ',') {
                    i += 1;
                } else {
                    return error.MalformedQuotedField;
                }
            } else {
                // Slow path: copy unescaped content into quote_buf.
                const start = qb_used;
                var closed = false;
                while (i < line.len) {
                    if (line[i] == '"') {
                        if (i + 1 < line.len and line[i + 1] == '"') {
                            if (qb_used < quote_buf.len) {
                                quote_buf[qb_used] = '"';
                                qb_used += 1;
                            }
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        if (qb_used < quote_buf.len) {
                            quote_buf[qb_used] = line[i];
                            qb_used += 1;
                        }
                        i += 1;
                    }
                }
                if (!closed) return error.UnterminatedQuote;
                out[n] = quote_buf[start..qb_used];
                quoted[n] = true;
                n += 1;
                if (i == line.len) {
                    break;
                } else if (line[i] == ',') {
                    i += 1;
                } else {
                    return error.MalformedQuotedField;
                }
            }
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') : (i += 1) {}
            out[n] = line[start..i];
            quoted[n] = false;
            n += 1;
            if (i < line.len) {
                i += 1;
            } else {
                break;
            }
        }
    }

    return .{ .fields = out[0..n], .quoted = quoted[0..n], .quote_buf_used = qb_used };
}

fn resolveColumnIndex(header: []const []const u8, selector: []const u8) !usize {
    if (std.fmt.parseInt(usize, selector, 10)) |idx| {
        if (idx >= 1 and idx <= header.len) {
            return idx - 1;
        }
        const stderr = std.io.getStdErr().writer();
        try stderr.print("Error: column index {d} out of range (1-{d})\n", .{ idx, header.len });
        std.process.exit(1);
    } else |_| {}

    for (header, 0..) |col, i| {
        if (std.mem.eql(u8, col, selector)) {
            return i;
        }
    }

    const stderr = std.io.getStdErr().writer();
    try stderr.print("Error: unknown column: {s}\n", .{selector});
    std.process.exit(1);
}

fn globMatch(pattern: []const u8, text: []const u8) bool {
    var px: usize = 0;
    var tx: usize = 0;
    var star_px: ?usize = null;
    var star_tx: usize = 0;

    while (tx < text.len or px < pattern.len) {
        if (px < pattern.len and pattern[px] == '*') {
            star_px = px;
            star_tx = tx;
            px += 1;
        } else if (px < pattern.len and tx < text.len and pattern[px] == text[tx]) {
            px += 1;
            tx += 1;
        } else if (star_px) |sp| {
            star_tx += 1;
            if (star_tx > text.len) return false;
            tx = star_tx;
            px = sp + 1;
        } else {
            return false;
        }
    }
    return true;
}

fn evaluateFilter(filter: *const Filter, fields: []const []const u8) bool {
    const col_index = filter.col_index orelse return true;
    if (col_index >= fields.len) return false;

    const field_val = fields[col_index];
    const filter_val = filter.value;

    if (filter.op == .like) {
        return globMatch(filter_val, field_val);
    }

    if (filter.value_num) |b| {
        const field_num = std.fmt.parseFloat(f64, field_val) catch return false;
        return switch (filter.op) {
            .eq => field_num == b,
            .neq => field_num != b,
            .lt => field_num < b,
            .gt => field_num > b,
            .lte => field_num <= b,
            .gte => field_num >= b,
            .like => unreachable,
        };
    }

    const order = std.mem.order(u8, field_val, filter_val);
    return switch (filter.op) {
        .eq => order == .eq,
        .neq => order != .eq,
        .lt => order == .lt,
        .gt => order == .gt,
        .lte => order == .lt or order == .eq,
        .gte => order == .gt or order == .eq,
        .like => unreachable,
    };
}

fn writeField(writer: anytype, field: []const u8) !void {
    const needs_quoting = std.mem.indexOfAny(u8, field, ",\"\n\r") != null;
    if (needs_quoting) {
        try writer.writeByte('"');
        for (field) |c| {
            if (c == '"') {
                try writer.writeAll("\"\"");
            } else {
                try writer.writeByte(c);
            }
        }
        try writer.writeByte('"');
    } else {
        try writer.writeAll(field);
    }
}

fn writeRecord(writer: anytype, fields: []const []const u8, col_indices: ?[]const usize) !void {
    if (col_indices) |indices| {
        for (indices, 0..) |ci, i| {
            if (i > 0) try writer.writeByte(',');
            if (ci < fields.len) {
                try writeField(writer, fields[ci]);
            }
        }
    } else {
        for (fields, 0..) |field, i| {
            if (i > 0) try writer.writeByte(',');
            try writeField(writer, field);
        }
    }
    try writer.writeByte('\n');
}

fn writeRecordWithQuotedMask(writer: anytype, fields: []const []const u8, quoted: []const bool, col_indices: ?[]const usize) !void {
    if (col_indices) |indices| {
        for (indices, 0..) |ci, i| {
            if (i > 0) try writer.writeByte(',');
            if (ci < fields.len) {
                if (quoted[ci]) {
                    try writeField(writer, fields[ci]);
                } else {
                    try writer.writeAll(fields[ci]);
                }
            }
        }
    } else {
        for (fields, 0..) |field, i| {
            if (i > 0) try writer.writeByte(',');
            if (quoted[i]) {
                try writeField(writer, field);
            } else {
                try writer.writeAll(field);
            }
        }
    }
    try writer.writeByte('\n');
}

const TopRow = struct {
    fields: []const []const u8,
    key: []const u8,
    key_num: ?f64,
};

const SampleRow = struct {
    fields: []const []const u8,
};

fn compareTopKeys(a_num: ?f64, a: []const u8, b_num: ?f64, b: []const u8) std.math.Order {
    if (a_num != null and b_num != null) {
        return std.math.order(a_num.?, b_num.?);
    }
    return std.mem.order(u8, a, b);
}

fn cloneFields(allocator: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
    const duped = try allocator.alloc([]const u8, fields.len);
    errdefer allocator.free(duped);

    var i: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < i) : (j += 1) {
            allocator.free(duped[j]);
        }
    }
    while (i < fields.len) : (i += 1) {
        const field = fields[i];
        duped[i] = try allocator.dupe(u8, field);
    }
    return duped;
}

fn freeClonedFields(allocator: std.mem.Allocator, fields: []const []const u8) void {
    for (fields) |field| {
        allocator.free(field);
    }
    allocator.free(fields);
}

fn worstTopIndex(rows: []const TopRow) usize {
    std.debug.assert(rows.len > 0);
    var worst: usize = 0;
    var i: usize = 1;
    while (i < rows.len) : (i += 1) {
        if (compareTopKeys(rows[i].key_num, rows[i].key, rows[worst].key_num, rows[worst].key) == .lt) {
            worst = i;
        }
    }
    return worst;
}

fn displayWidth(s: []const u8) usize {
    var width: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        const byte = s[i];
        if (byte < 0x80) {
            width += 1;
            i += 1;
        } else if (byte < 0xC0) {
            // Stray continuation byte; count as 1.
            width += 1;
            i += 1;
        } else if (byte < 0xE0) {
            width += 1;
            i += 2;
        } else if (byte < 0xF0) {
            width += 1;
            i += 3;
        } else {
            width += 1;
            i += 4;
        }
    }
    return width;
}

fn writeTableSeparator(writer: anytype, widths: []const usize) !void {
    for (widths, 0..) |w, i| {
        if (i > 0) try writer.writeAll("-+-");
        try writer.writeByteNTimes('-', w);
    }
    try writer.writeByte('\n');
}

fn writeTableRow(writer: anytype, fields: []const []const u8, col_indices: ?[]const usize, widths: []const usize) !void {
    if (col_indices) |indices| {
        for (indices, 0..) |ci, i| {
            if (i > 0) try writer.writeAll(" | ");
            const val = if (ci < fields.len) fields[ci] else "";
            try writer.writeAll(val);
            const dw = displayWidth(val);
            if (dw < widths[i]) {
                try writer.writeByteNTimes(' ', widths[i] - dw);
            }
        }
    } else {
        for (widths, 0..) |w, i| {
            if (i > 0) try writer.writeAll(" | ");
            const val = if (i < fields.len) fields[i] else "";
            try writer.writeAll(val);
            const dw = displayWidth(val);
            if (dw < w) {
                try writer.writeByteNTimes(' ', w - dw);
            }
        }
    }
    try writer.writeByte('\n');
}

fn writeTopRows(
    allocator: std.mem.Allocator,
    writer: anytype,
    header: []const []const u8,
    col_indices: ?[]const usize,
    top_rows: []const TopRow,
    table: bool,
    no_header: bool,
) !void {
    if (table) {
        const num_output_cols = if (col_indices) |ci| ci.len else header.len;
        const widths = try allocator.alloc(usize, num_output_cols);
        defer allocator.free(widths);

        if (col_indices) |ci| {
            for (ci, 0..) |c, i| {
                widths[i] = if (c < header.len) displayWidth(header[c]) else 0;
            }
        } else {
            for (header, 0..) |h, i| {
                widths[i] = displayWidth(h);
            }
        }

        for (top_rows) |row| {
            if (col_indices) |ci| {
                for (ci, 0..) |c, j| {
                    if (c < row.fields.len) {
                        const dw = displayWidth(row.fields[c]);
                        if (dw > widths[j]) widths[j] = dw;
                    }
                }
            } else {
                for (row.fields, 0..) |val, j| {
                    if (j < widths.len) {
                        const dw = displayWidth(val);
                        if (dw > widths[j]) widths[j] = dw;
                    }
                }
            }
        }

        if (!no_header) {
            try writeTableRow(writer, header, col_indices, widths);
            try writeTableSeparator(writer, widths);
        }

        for (top_rows) |row| {
            try writeTableRow(writer, row.fields, col_indices, widths);
        }
        return;
    }

    if (!no_header) {
        try writeRecord(writer, header, col_indices);
    }
    for (top_rows) |row| {
        try writeRecord(writer, row.fields, col_indices);
    }
}

fn writeSampleRows(
    allocator: std.mem.Allocator,
    writer: anytype,
    header: []const []const u8,
    col_indices: ?[]const usize,
    rows: []const SampleRow,
    table: bool,
    no_header: bool,
) !void {
    if (table) {
        const num_output_cols = if (col_indices) |ci| ci.len else header.len;
        const widths = try allocator.alloc(usize, num_output_cols);
        defer allocator.free(widths);

        if (col_indices) |ci| {
            for (ci, 0..) |c, i| {
                widths[i] = if (c < header.len) displayWidth(header[c]) else 0;
            }
        } else {
            for (header, 0..) |h, i| {
                widths[i] = displayWidth(h);
            }
        }

        for (rows) |row| {
            if (col_indices) |ci| {
                for (ci, 0..) |c, j| {
                    if (c < row.fields.len) {
                        const dw = displayWidth(row.fields[c]);
                        if (dw > widths[j]) widths[j] = dw;
                    }
                }
            } else {
                for (row.fields, 0..) |val, j| {
                    if (j < widths.len) {
                        const dw = displayWidth(val);
                        if (dw > widths[j]) widths[j] = dw;
                    }
                }
            }
        }

        if (!no_header) {
            try writeTableRow(writer, header, col_indices, widths);
            try writeTableSeparator(writer, widths);
        }

        for (rows) |row| {
            try writeTableRow(writer, row.fields, col_indices, widths);
        }
        return;
    }

    if (!no_header) {
        try writeRecord(writer, header, col_indices);
    }
    for (rows) |row| {
        try writeRecord(writer, row.fields, col_indices);
    }
}

fn updateAgg(agg: *Agg, field_val: []const u8) void {
    switch (agg.func) {
        .count => {
            if (field_val.len > 0) agg.n += 1;
        },
        .sum, .mean => {
            const v = std.fmt.parseFloat(f64, field_val) catch {
                agg.tainted = true;
                return;
            };
            agg.total += v;
            agg.n += 1;
        },
        .min => {
            const v = std.fmt.parseFloat(f64, field_val) catch {
                agg.tainted = true;
                return;
            };
            if (agg.n == 0 or v < agg.extreme) agg.extreme = v;
            agg.n += 1;
        },
        .max => {
            const v = std.fmt.parseFloat(f64, field_val) catch {
                agg.tainted = true;
                return;
            };
            if (agg.n == 0 or v > agg.extreme) agg.extreme = v;
            agg.n += 1;
        },
    }
}

fn aggResult(agg: *const Agg) f64 {
    return switch (agg.func) {
        .sum => agg.total,
        .mean => if (agg.n > 0) agg.total / @as(f64, @floatFromInt(agg.n)) else 0,
        .min, .max => agg.extreme,
        .count => @as(f64, @floatFromInt(agg.n)),
    };
}

fn passesFilters(filters: ?[]Filter, fields: []const []const u8) bool {
    const ff = filters orelse return true;
    for (ff) |*f| {
        if (!evaluateFilter(f, fields)) return false;
    }
    return true;
}

fn readNextLine(reader: anytype, buf: []u8) !?[]const u8 {
    while (true) {
        const line = reader.readUntilDelimiterOrEof(buf, '\n') catch |err| {
            if (err == error.StreamTooLong) return error.LineTooLong;
            return err;
        } orelse return null;

        const clean = if (line.len > 0 and line[line.len - 1] == '\r')
            line[0 .. line.len - 1]
        else
            line;

        if (clean.len == 0) continue;
        return clean;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const TestParseResult = struct {
    fields: [][]const u8,
    quote_buf_used: usize,
    out: [max_fields][]const u8,
    qbuf: [4096]u8,
};

fn testParseRecord(line: []const u8) TestParseResult {
    var result: TestParseResult = undefined;
    result.out = undefined;
    result.qbuf = undefined;
    var quoted: [max_fields]bool = undefined;
    const r = parseRecord(line, &result.out, &quoted, &result.qbuf) catch unreachable;
    result.fields = r.fields;
    result.quote_buf_used = r.quote_buf_used;
    return result;
}

test "parseRecord: simple unquoted fields" {
    const r = testParseRecord("alice,30,Engineering");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 3), f.len);
    try std.testing.expectEqualStrings("alice", f[0]);
    try std.testing.expectEqualStrings("30", f[1]);
    try std.testing.expectEqualStrings("Engineering", f[2]);
}

test "parseRecord: single field" {
    const r = testParseRecord("hello");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 1), f.len);
    try std.testing.expectEqualStrings("hello", f[0]);
}

test "parseRecord: empty fields" {
    const r = testParseRecord(",a,,b,");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 5), f.len);
    try std.testing.expectEqualStrings("", f[0]);
    try std.testing.expectEqualStrings("a", f[1]);
    try std.testing.expectEqualStrings("", f[2]);
    try std.testing.expectEqualStrings("b", f[3]);
    try std.testing.expectEqualStrings("", f[4]);
}

test "parseRecord: quoted field without escapes" {
    const r = testParseRecord("\"hello world\",42");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 2), f.len);
    try std.testing.expectEqualStrings("hello world", f[0]);
    try std.testing.expectEqualStrings("42", f[1]);
}

test "parseRecord: quoted field with comma inside" {
    const r = testParseRecord("\"last, first\",age");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 2), f.len);
    try std.testing.expectEqualStrings("last, first", f[0]);
    try std.testing.expectEqualStrings("age", f[1]);
}

test "parseRecord: quoted field with escaped quotes" {
    const r = testParseRecord("\"she said \"\"hi\"\"\",ok");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 2), f.len);
    try std.testing.expectEqualStrings("she said \"hi\"", f[0]);
    try std.testing.expectEqualStrings("ok", f[1]);
}

test "parseRecord: fields with spaces" {
    const r = testParseRecord("Total Amount,First Name,Last Name");
    const f = r.fields;
    try std.testing.expectEqual(@as(usize, 3), f.len);
    try std.testing.expectEqualStrings("Total Amount", f[0]);
    try std.testing.expectEqualStrings("First Name", f[1]);
    try std.testing.expectEqualStrings("Last Name", f[2]);
}

test "parseFilter: simple operators" {
    const eq = parseFilter("name=Alice").?;
    try std.testing.expectEqualStrings("name", eq.field);
    try std.testing.expectEqual(FilterOp.eq, eq.op);
    try std.testing.expectEqualStrings("Alice", eq.value);

    const neq = parseFilter("age!=30").?;
    try std.testing.expectEqualStrings("age", neq.field);
    try std.testing.expectEqual(FilterOp.neq, neq.op);
    try std.testing.expectEqualStrings("30", neq.value);

    const lt = parseFilter("score<50").?;
    try std.testing.expectEqual(FilterOp.lt, lt.op);

    const gt = parseFilter("score>50").?;
    try std.testing.expectEqual(FilterOp.gt, gt.op);

    const lte = parseFilter("score<=50").?;
    try std.testing.expectEqual(FilterOp.lte, lte.op);

    const gte = parseFilter("score>=50").?;
    try std.testing.expectEqual(FilterOp.gte, gte.op);

    const like = parseFilter("name~A*").?;
    try std.testing.expectEqual(FilterOp.like, like.op);
    try std.testing.expectEqualStrings("A*", like.value);
}

test "parseFilter: whitespace around operator" {
    const f = parseFilter("Total Amount > 0.1").?;
    try std.testing.expectEqualStrings("Total Amount", f.field);
    try std.testing.expectEqual(FilterOp.gt, f.op);
    try std.testing.expectEqualStrings("0.1", f.value);
}

test "parseFilter: whitespace around multi-char operator" {
    const f = parseFilter("age != 30").?;
    try std.testing.expectEqualStrings("age", f.field);
    try std.testing.expectEqual(FilterOp.neq, f.op);
    try std.testing.expectEqualStrings("30", f.value);
}

test "parseFilter: field name with spaces no surrounding whitespace" {
    const f = parseFilter("Total Amount>0.1").?;
    try std.testing.expectEqualStrings("Total Amount", f.field);
    try std.testing.expectEqualStrings("0.1", f.value);
}

test "parseFilter: returns null for empty string" {
    try std.testing.expect(parseFilter("") == null);
}

test "parseFilter: returns null for no operator" {
    try std.testing.expect(parseFilter("justtext") == null);
}

test "parseFilter: returns null when operator is at start" {
    try std.testing.expect(parseFilter("=value") == null);
    try std.testing.expect(parseFilter(">10") == null);
}

test "parseArgsList: --head defaults to 10 when value omitted" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "-n" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expectEqual(@as(?usize, default_head_rows), cfg.head);
}

test "parseArgsList: --head uses explicit value when provided" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--head", "25" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expectEqual(@as(?usize, 25), cfg.head);
}

test "parseArgsList: --head defaults to 10 when followed by flag" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "-n", "-t" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expectEqual(@as(?usize, default_head_rows), cfg.head);
    try std.testing.expect(cfg.table);
}

test "parseArgsList: --table can be combined with --top" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--table", "--top", "salary" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expect(cfg.table);
    try std.testing.expectEqualStrings("salary", cfg.top_field.?);
}

test "parseArgsList: --top rejects -n exceeding max_top_rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--top", "salary", "-n", "10001" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --top accepts -n at max_top_rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--top", "salary", "-n", "10000" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expectEqual(@as(?usize, 10000), cfg.head);
}

test "parseArgsList: -n without --top has no cap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "-n", "999999" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    try std.testing.expectEqual(@as(?usize, 999999), cfg.head);
}

test "globMatch: exact match" {
    try std.testing.expect(globMatch("hello", "hello"));
    try std.testing.expect(!globMatch("hello", "world"));
}

test "globMatch: prefix wildcard" {
    try std.testing.expect(globMatch("*world", "hello world"));
    try std.testing.expect(!globMatch("*world", "world!"));
}

test "globMatch: suffix wildcard" {
    try std.testing.expect(globMatch("hello*", "hello world"));
    try std.testing.expect(!globMatch("hello*", "hi"));
}

test "globMatch: contains wildcard" {
    try std.testing.expect(globMatch("*ell*", "hello"));
    try std.testing.expect(!globMatch("*xyz*", "hello"));
}

test "globMatch: multiple wildcards" {
    try std.testing.expect(globMatch("h*l*o", "hello"));
    try std.testing.expect(globMatch("*a*b*c*", "aXbYc"));
    try std.testing.expect(!globMatch("*a*b*c*", "a only"));
}

test "globMatch: star matches empty string" {
    try std.testing.expect(globMatch("*", ""));
    try std.testing.expect(globMatch("*", "anything"));
    try std.testing.expect(globMatch("hello*", "hello"));
}

test "globMatch: empty pattern" {
    try std.testing.expect(globMatch("", ""));
    try std.testing.expect(!globMatch("", "notempty"));
}

test "evaluateFilter: numeric comparison" {
    const fields = [_][]const u8{ "Alice", "150000" };
    var f = Filter{ .field = "salary", .op = .gt, .value = "100000", .col_index = 1 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "200000";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: numeric equality" {
    const fields = [_][]const u8{"42"};
    var f = Filter{ .field = "x", .op = .eq, .value = "42", .col_index = 0 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "43";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: string comparison fallback" {
    const fields = [_][]const u8{"banana"};
    var f = Filter{ .field = "fruit", .op = .gt, .value = "apple", .col_index = 0 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "cherry";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: glob operator" {
    const fields = [_][]const u8{"bc-west"};
    var f = Filter{ .field = "dest", .op = .like, .value = "bc*", .col_index = 0 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "xy*";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: col_index out of range returns false" {
    const fields = [_][]const u8{"only"};
    const f = Filter{ .field = "x", .op = .eq, .value = "only", .col_index = 5 };
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: null col_index returns true" {
    const fields = [_][]const u8{"a"};
    const f = Filter{ .field = "x", .op = .eq, .value = "a", .col_index = null };
    try std.testing.expect(evaluateFilter(&f, &fields));
}

test "passesFilters: null filters always passes" {
    const fields = [_][]const u8{"a"};
    try std.testing.expect(passesFilters(null, &fields));
}

test "passesFilters: multiple filters ANDed" {
    const fields = [_][]const u8{ "Alice", "35", "Engineering" };
    var filters = [_]Filter{
        .{ .field = "age", .op = .gt, .value = "30", .col_index = 1 },
        .{ .field = "dept", .op = .eq, .value = "Engineering", .col_index = 2 },
    };
    try std.testing.expect(passesFilters(&filters, &fields));

    filters[1].value = "Marketing";
    try std.testing.expect(!passesFilters(&filters, &fields));
}

test "compareTopKeys: numeric and string ordering" {
    try std.testing.expect(compareTopKeys(5, "5", 3, "3") == .gt);
    try std.testing.expect(compareTopKeys(null, "b", null, "a") == .gt);
    try std.testing.expect(compareTopKeys(10, "10", null, "9") == .lt);
}

test "worstTopIndex: returns minimum key row index" {
    const f1 = [_][]const u8{"x"};
    const f2 = [_][]const u8{"y"};
    const f3 = [_][]const u8{"z"};
    const rows = [_]TopRow{
        .{ .fields = &f1, .key = "10", .key_num = 10 },
        .{ .fields = &f2, .key = "3", .key_num = 3 },
        .{ .fields = &f3, .key = "7", .key_num = 7 },
    };
    try std.testing.expectEqual(@as(usize, 1), worstTopIndex(&rows));
}

test "displayWidth: ASCII string" {
    try std.testing.expectEqual(@as(usize, 5), displayWidth("hello"));
}

test "displayWidth: empty string" {
    try std.testing.expectEqual(@as(usize, 0), displayWidth(""));
}

test "displayWidth: two-byte UTF-8 (middle dot U+00B7)" {
    // "··" = 4 bytes, 2 display columns
    try std.testing.expectEqual(@as(usize, 2), displayWidth("\xc2\xb7\xc2\xb7"));
}

test "displayWidth: three-byte UTF-8 (euro sign U+20AC)" {
    // "€" = 3 bytes, 1 display column
    try std.testing.expectEqual(@as(usize, 1), displayWidth("\xe2\x82\xac"));
}

test "displayWidth: mixed ASCII and multi-byte" {
    // "360 Checking ··4926" = 19 display columns
    try std.testing.expectEqual(@as(usize, 19), displayWidth("360 Checking \xc2\xb7\xc2\xb74926"));
}

test "displayWidth: four-byte UTF-8 (emoji)" {
    // Single emoji = 4 bytes, counted as 1 column
    try std.testing.expectEqual(@as(usize, 1), displayWidth("\xf0\x9f\x98\x80"));
}

test "writeField: plain field unchanged" {
    var buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeField(fbs.writer(), "hello");
    try std.testing.expectEqualStrings("hello", fbs.getWritten());
}

test "writeField: field with comma gets quoted" {
    var buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeField(fbs.writer(), "a,b");
    try std.testing.expectEqualStrings("\"a,b\"", fbs.getWritten());
}

test "writeField: field with quote gets escaped" {
    var buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeField(fbs.writer(), "she said \"hi\"");
    try std.testing.expectEqualStrings("\"she said \"\"hi\"\"\"", fbs.getWritten());
}

test "writeRecord: all fields, no column selection" {
    var buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fields = [_][]const u8{ "a", "b", "c" };
    try writeRecord(fbs.writer(), &fields, null);
    try std.testing.expectEqualStrings("a,b,c\n", fbs.getWritten());
}

test "writeRecord: with column selection" {
    var buf: [256]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const fields = [_][]const u8{ "a", "b", "c", "d" };
    const indices = [_]usize{ 2, 0 };
    try writeRecord(fbs.writer(), &fields, &indices);
    try std.testing.expectEqualStrings("c,a\n", fbs.getWritten());
}

test "writeTopRows: table mode emits one header row" {
    var buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const header = [_][]const u8{ "name", "score" };
    const row1_fields = [_][]const u8{ "Alice", "9" };
    const row2_fields = [_][]const u8{ "Bob", "8" };
    const rows = [_]TopRow{
        .{ .fields = &row1_fields, .key = row1_fields[1], .key_num = 9.0 },
        .{ .fields = &row2_fields, .key = row2_fields[1], .key_num = 8.0 },
    };

    try writeTopRows(std.testing.allocator, fbs.writer(), &header, null, &rows, true, false);
    try std.testing.expectEqualStrings(
        "name  | score\n" ++
            "------+------\n" ++
            "Alice | 9    \n" ++
            "Bob   | 8    \n",
        fbs.getWritten(),
    );
}

test "writeTopRows: csv mode emits one header row" {
    var buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const header = [_][]const u8{ "name", "score" };
    const row1_fields = [_][]const u8{ "Alice", "9" };
    const row2_fields = [_][]const u8{ "Bob", "8" };
    const rows = [_]TopRow{
        .{ .fields = &row1_fields, .key = row1_fields[1], .key_num = 9.0 },
        .{ .fields = &row2_fields, .key = row2_fields[1], .key_num = 8.0 },
    };

    try writeTopRows(std.testing.allocator, fbs.writer(), &header, null, &rows, false, false);
    try std.testing.expectEqualStrings(
        "name,score\n" ++
            "Alice,9\n" ++
            "Bob,8\n",
        fbs.getWritten(),
    );
}

test "readNextLine: basic line" {
    var buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream("hello\nworld\n");
    var reader = stream.reader();
    const line1 = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("hello", line1);
    const line2 = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("world", line2);
    try std.testing.expect(try readNextLine(&reader, &buf) == null);
}

test "readNextLine: strips carriage return" {
    var buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream("hello\r\nworld\r\n");
    var reader = stream.reader();
    const line1 = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("hello", line1);
    const line2 = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("world", line2);
}

test "readNextLine: skips empty lines" {
    var buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream("\n\nhello\n\n");
    var reader = stream.reader();
    const line = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("hello", line);
}

test "readNextLine: returns null on empty input" {
    var buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream("");
    var reader = stream.reader();
    try std.testing.expect(try readNextLine(&reader, &buf) == null);
}

test "parseRecord: malformed quoted field returns error" {
    var out: [max_fields][]const u8 = undefined;
    var quoted: [max_fields]bool = undefined;
    var qbuf: [4096]u8 = undefined;
    try std.testing.expectError(error.MalformedQuotedField, parseRecord("\"x\"oops,2,3", &out, &quoted, &qbuf));
}

test "parseRecord: unterminated quoted field returns error" {
    var out: [max_fields][]const u8 = undefined;
    var quoted: [max_fields]bool = undefined;
    var qbuf: [4096]u8 = undefined;
    try std.testing.expectError(error.UnterminatedQuote, parseRecord("\"abc,def", &out, &quoted, &qbuf));
}

test "parseRecord: too many fields returns error" {
    var line_buf: [max_fields * 2 + 16]u8 = undefined;
    var pos: usize = 0;
    for (0..(max_fields + 1)) |i| {
        line_buf[pos] = 'x';
        pos += 1;
        if (i != max_fields) {
            line_buf[pos] = ',';
            pos += 1;
        }
    }

    var out: [max_fields][]const u8 = undefined;
    var quoted: [max_fields]bool = undefined;
    var qbuf: [4096]u8 = undefined;
    try std.testing.expectError(error.TooManyFields, parseRecord(line_buf[0..pos], &out, &quoted, &qbuf));
}

test "parseAgg: valid expressions" {
    const s = parseAgg("sum:salary").?;
    try std.testing.expectEqual(AggFunc.sum, s.func);
    try std.testing.expectEqualStrings("salary", s.field);

    const mn = parseAgg("min:age").?;
    try std.testing.expectEqual(AggFunc.min, mn.func);
    try std.testing.expectEqualStrings("age", mn.field);

    const mx = parseAgg("max:Total Amount").?;
    try std.testing.expectEqual(AggFunc.max, mx.func);
    try std.testing.expectEqualStrings("Total Amount", mx.field);

    const c = parseAgg("count:name").?;
    try std.testing.expectEqual(AggFunc.count, c.func);
    try std.testing.expectEqualStrings("name", c.field);

    const me = parseAgg("mean:score").?;
    try std.testing.expectEqual(AggFunc.mean, me.func);
    try std.testing.expectEqualStrings("score", me.field);
}

test "parseAgg: field with colon uses everything after first colon" {
    const a = parseAgg("sum:Rate:2024").?;
    try std.testing.expectEqual(AggFunc.sum, a.func);
    try std.testing.expectEqualStrings("Rate:2024", a.field);
}

test "parseAgg: invalid expressions" {
    try std.testing.expect(parseAgg("avg:salary") == null); // unknown func
    try std.testing.expect(parseAgg("sum:") == null); // empty field
    try std.testing.expect(parseAgg("sum") == null); // no colon
    try std.testing.expect(parseAgg(":salary") == null); // empty func
    try std.testing.expect(parseAgg("") == null);
}

test "updateAgg: sum accumulates" {
    var agg = Agg{ .func = .sum, .field = "x" };
    updateAgg(&agg, "10");
    updateAgg(&agg, "20.5");
    try std.testing.expectEqual(@as(usize, 2), agg.n);
    try std.testing.expectApproxEqAbs(@as(f64, 30.5), agg.total, 1e-9);
    try std.testing.expect(!agg.tainted);
}

test "updateAgg: non-numeric value sets tainted" {
    var agg = Agg{ .func = .sum, .field = "x" };
    updateAgg(&agg, "10");
    updateAgg(&agg, "not_a_number");
    try std.testing.expect(agg.tainted);
    try std.testing.expectEqual(@as(usize, 1), agg.n);
}

test "updateAgg: min tracks minimum" {
    var agg = Agg{ .func = .min, .field = "x" };
    updateAgg(&agg, "5");
    updateAgg(&agg, "2");
    updateAgg(&agg, "8");
    try std.testing.expectApproxEqAbs(@as(f64, 2), agg.extreme, 1e-9);
}

test "updateAgg: max tracks maximum" {
    var agg = Agg{ .func = .max, .field = "x" };
    updateAgg(&agg, "5");
    updateAgg(&agg, "2");
    updateAgg(&agg, "8");
    try std.testing.expectApproxEqAbs(@as(f64, 8), agg.extreme, 1e-9);
}

test "updateAgg: count increments for non-empty values" {
    var agg = Agg{ .func = .count, .field = "x" };
    updateAgg(&agg, "hello");
    updateAgg(&agg, ""); // skipped
    updateAgg(&agg, "world");
    try std.testing.expectEqual(@as(usize, 2), agg.n);
}

test "updateAgg: mean accumulates total and count" {
    var agg = Agg{ .func = .mean, .field = "x" };
    updateAgg(&agg, "10");
    updateAgg(&agg, "20");
    updateAgg(&agg, "30");
    try std.testing.expectApproxEqAbs(@as(f64, 60), agg.total, 1e-9);
    try std.testing.expectEqual(@as(usize, 3), agg.n);
}

test "aggResult: mean divides total by n" {
    const agg = Agg{ .func = .mean, .field = "x", .total = 60, .n = 3 };
    try std.testing.expectApproxEqAbs(@as(f64, 20), aggResult(&agg), 1e-9);
}

test "aggResult: mean with zero rows returns 0" {
    const agg = Agg{ .func = .mean, .field = "x" };
    try std.testing.expectApproxEqAbs(@as(f64, 0), aggResult(&agg), 1e-9);
}

test "parseArgsList: --agg parses correctly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--agg", "sum:salary", "--agg", "min:age" };
    const cfg = (try parseArgsList(&argv, allocator)).?;
    const aggs = cfg.aggs.?;
    try std.testing.expectEqual(@as(usize, 2), aggs.len);
    try std.testing.expectEqual(AggFunc.sum, aggs[0].func);
    try std.testing.expectEqualStrings("salary", aggs[0].field);
    try std.testing.expectEqual(AggFunc.min, aggs[1].func);
    try std.testing.expectEqualStrings("age", aggs[1].field);
}

test "parseArgsList: --agg combined with --top errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--agg", "sum:salary", "--top", "salary" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --agg combined with --head errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--agg", "sum:salary", "--head", "5" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --sample parses N" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--sample", "50" };
    const cfg = (try parseArgsList(&argv, allocator)) orelse {
        try std.testing.expect(false);
        return;
    };
    try std.testing.expectEqual(cfg.sample, 50);
}

test "parseArgsList: --sample + --top errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--sample", "50", "--top", "salary" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --sample + --agg errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--sample", "50", "--agg", "sum:salary" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --sample + --head errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--sample", "50", "--head", "5" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "parseArgsList: --sample 0 errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const argv = [_][]const u8{ "zsv", "--sample", "0" };
    try std.testing.expect((try parseArgsList(&argv, allocator)) == null);
}

test "header with escaped quotes is not corrupted by subsequent data rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var field_buf: [max_fields][]const u8 = undefined;
    var quoted_buf: [max_fields]bool = undefined;
    var quote_buf: [max_line_len]u8 = undefined;

    const header_line = "\"col\"\"name\",value";
    const header_result = try parseRecord(header_line, &field_buf, &quoted_buf, &quote_buf);

    const header_shallow = try alloc.dupe([]const u8, header_result.fields);
    try std.testing.expectEqualStrings("col\"name", header_shallow[0]);

    const data_line = "\"data\"\"1\",world";
    _ = try parseRecord(data_line, &field_buf, &quoted_buf, &quote_buf);

    const header_deep = try alloc.alloc([]const u8, header_result.fields.len);
    const header_result2 = try parseRecord(header_line, &field_buf, &quoted_buf, &quote_buf);
    for (header_result2.fields, 0..) |field, i| {
        header_deep[i] = try alloc.dupe(u8, field);
    }

    const data_line2 = "\"overwrite\"\"me\",x";
    _ = try parseRecord(data_line2, &field_buf, &quoted_buf, &quote_buf);

    try std.testing.expectEqualStrings("col\"name", header_deep[0]);
    try std.testing.expectEqualStrings("value", header_deep[1]);
}

test "evaluateFilter: numeric filter does not match non-numeric field values" {
    const f = parseFilter("price>100").?;
    var filter = Filter{
        .field = f.field,
        .op = f.op,
        .value = f.value,
        .col_index = 0,
        .value_num_parsed = f.value_num_parsed,
        .value_num = f.value_num,
    };

    const fields_na = [_][]const u8{"N/A"};
    try std.testing.expect(!evaluateFilter(&filter, &fields_na));

    const fields_abc = [_][]const u8{"abc"};
    try std.testing.expect(!evaluateFilter(&filter, &fields_abc));

    const fields_200 = [_][]const u8{"200"};
    try std.testing.expect(evaluateFilter(&filter, &fields_200));

    const fields_50 = [_][]const u8{"50"};
    try std.testing.expect(!evaluateFilter(&filter, &fields_50));
}

test "evaluateFilter: numeric equality filter does not match non-numeric field" {
    const f = parseFilter("x=42").?;
    const filter = Filter{
        .field = f.field,
        .op = f.op,
        .value = f.value,
        .col_index = 0,
        .value_num_parsed = f.value_num_parsed,
        .value_num = f.value_num,
    };

    const fields_text = [_][]const u8{"N/A"};
    try std.testing.expect(!evaluateFilter(&filter, &fields_text));

    const fields_num = [_][]const u8{"42"};
    try std.testing.expect(evaluateFilter(&filter, &fields_num));
}

test "parseFilter always sets value_num_parsed = true" {
    const f_num = parseFilter("score>50").?;
    try std.testing.expect(f_num.value_num_parsed);
    try std.testing.expect(f_num.value_num != null);
    try std.testing.expectApproxEqAbs(@as(f64, 50), f_num.value_num.?, 1e-9);

    const f_text = parseFilter("name=Alice").?;
    try std.testing.expect(f_text.value_num_parsed);
    try std.testing.expect(f_text.value_num == null);
}

test "readNextLine: StreamTooLong is reported as LineTooLong" {
    var buf: [4]u8 = undefined;
    var stream = std.io.fixedBufferStream("hello\n");
    var reader = stream.reader();
    try std.testing.expectError(error.LineTooLong, readNextLine(&reader, &buf));
}

test "readNextLine: skips empty lines without exposing count" {
    var buf: [256]u8 = undefined;
    var stream = std.io.fixedBufferStream("\n\n\ndata\n");
    var reader = stream.reader();
    const line = (try readNextLine(&reader, &buf)).?;
    try std.testing.expectEqualStrings("data", line);
}

test "worstTopIndex: single-element slice returns index 0" {
    const f1 = [_][]const u8{"x"};
    const rows = [_]TopRow{
        .{ .fields = &f1, .key = "42", .key_num = 42 },
    };
    try std.testing.expectEqual(@as(usize, 0), worstTopIndex(&rows));
}

test "worstTopIndex: returns index of smallest key among multiple rows" {
    const f1 = [_][]const u8{"a"};
    const f2 = [_][]const u8{"b"};
    const f3 = [_][]const u8{"c"};
    const rows = [_]TopRow{
        .{ .fields = &f1, .key = "5", .key_num = 5 },
        .{ .fields = &f2, .key = "1", .key_num = 1 },
        .{ .fields = &f3, .key = "9", .key_num = 9 },
    };
    try std.testing.expectEqual(@as(usize, 1), worstTopIndex(&rows));
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var prog_arena = std.heap.ArenaAllocator.init(allocator);
    defer prog_arena.deinit();
    const prog_alloc = prog_arena.allocator();

    const config = try parseArgs(prog_alloc) orelse return;

    const stdin = std.io.getStdIn();
    var br = std.io.bufferedReaderSize(read_buf_size, stdin.reader());
    var reader = br.reader();

    const stdout = std.io.getStdOut();
    var bw = std.io.bufferedWriter(stdout.writer());
    const writer = bw.writer();

    var line_buf: [max_line_len]u8 = undefined;
    var field_buf: [max_fields][]const u8 = undefined;
    var quoted_buf: [max_fields]bool = undefined;
    var quote_buf: [max_line_len]u8 = undefined;

    const header_line = readNextLine(&reader, &line_buf) catch {
        const stderr = std.io.getStdErr().writer();
        try stderr.writeAll("Error reading header: line too long\n");
        return;
    } orelse return;
    var rows_written: usize = 0;

    if (!config.table and config.selectors == null and config.filters == null and config.top_field == null and config.aggs == null and config.sample == null) {
        if (!config.no_header) {
            try writer.writeAll(header_line);
            try writer.writeByte('\n');
        }

        while (true) {
            if (config.head) |limit| {
                if (rows_written >= limit) break;
            }
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            try writer.writeAll(line);
            try writer.writeByte('\n');
            rows_written += 1;
        }

        try bw.flush();
        return;
    }

    // Header must be heap-duped since line_buf will be reused.
    const header_line_copy = try prog_alloc.dupe(u8, header_line);
    const header_result = parseRecord(header_line_copy, &field_buf, &quoted_buf, &quote_buf) catch |err| {
        const stderr = std.io.getStdErr().writer();
        try stderr.print("Error parsing CSV header: {s}\n", .{parseRecordErrorMessage(err)});
        std.process.exit(1);
    };
    const header = try prog_alloc.alloc([]const u8, header_result.fields.len);
    for (header_result.fields, 0..) |field, i| {
        header[i] = try prog_alloc.dupe(u8, field);
    }
    var line_no: usize = 1;

    var col_indices: ?[]usize = null;
    if (config.selectors) |selectors| {
        const indices = try prog_alloc.alloc(usize, selectors.len);
        for (selectors, 0..) |sel, i| {
            indices[i] = try resolveColumnIndex(header, sel);
        }
        col_indices = indices;
    }

    if (config.filters) |filters| {
        for (filters) |*f| {
            f.col_index = try resolveColumnIndex(header, f.field);
        }
    }

    const top_col_index: ?usize = if (config.top_field) |name| try resolveColumnIndex(header, name) else null;

    if (top_col_index) |top_idx| {
        const limit = config.head orelse default_head_rows;
        if (limit == 0) {
            try bw.flush();
            return;
        }

        var top_rows = std.ArrayList(TopRow).init(allocator);
        defer {
            for (top_rows.items) |row| {
                freeClonedFields(allocator, row.fields);
            }
            top_rows.deinit();
        }

        while (true) {
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };

            if (!passesFilters(config.filters, result.fields)) continue;

            const key = if (top_idx < result.fields.len) result.fields[top_idx] else "";
            const key_num = std.fmt.parseFloat(f64, key) catch null;

            if (top_rows.items.len < limit) {
                const duped = try cloneFields(allocator, result.fields);
                const duped_key = if (top_idx < duped.len) duped[top_idx] else "";
                try top_rows.append(.{ .fields = duped, .key = duped_key, .key_num = key_num });
            } else {
                const wi = worstTopIndex(top_rows.items);
                const worst = top_rows.items[wi];
                if (compareTopKeys(key_num, key, worst.key_num, worst.key) == .gt) {
                    const duped = try cloneFields(allocator, result.fields);
                    const duped_key = if (top_idx < duped.len) duped[top_idx] else "";
                    freeClonedFields(allocator, worst.fields);
                    top_rows.items[wi] = .{ .fields = duped, .key = duped_key, .key_num = key_num };
                }
            }
        }

        std.sort.pdq(TopRow, top_rows.items, {}, struct {
            fn lessThan(_: void, a: TopRow, b: TopRow) bool {
                return compareTopKeys(a.key_num, a.key, b.key_num, b.key) == .gt;
            }
        }.lessThan);

        try writeTopRows(prog_alloc, writer, header, col_indices, top_rows.items, config.table, config.no_header);

        try bw.flush();
        return;
    }

    if (config.aggs) |aggs| {
        for (aggs) |*agg| {
            agg.col_index = try resolveColumnIndex(header, agg.field);
        }

        while (true) {
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };
            if (!passesFilters(config.filters, result.fields)) continue;
            for (aggs) |*agg| {
                const col = agg.col_index orelse continue;
                if (col < result.fields.len) updateAgg(agg, result.fields[col]);
            }
        }

        const agg_headers = try prog_alloc.alloc([]const u8, aggs.len);
        const agg_values = try prog_alloc.alloc([]const u8, aggs.len);
        const stderr = std.io.getStdErr().writer();
        for (aggs, 0..) |*agg, i| {
            agg_headers[i] = try std.fmt.allocPrint(prog_alloc, "{s}({s})", .{ @tagName(agg.func), agg.field });
            if (agg.func == .count) {
                agg_values[i] = try std.fmt.allocPrint(prog_alloc, "{d}", .{agg.n});
            } else if (agg.tainted) {
                try stderr.print("Warning: {s}({s}): non-numeric values encountered\n", .{ @tagName(agg.func), agg.field });
                agg_values[i] = "";
            } else {
                agg_values[i] = try std.fmt.allocPrint(prog_alloc, "{d}", .{aggResult(agg)});
            }
        }

        if (config.table) {
            const widths = try prog_alloc.alloc(usize, aggs.len);
            for (aggs, 0..) |_, i| {
                widths[i] = @max(displayWidth(agg_headers[i]), displayWidth(agg_values[i]));
            }
            if (!config.no_header) {
                try writeTableRow(writer, agg_headers, null, widths);
                try writeTableSeparator(writer, widths);
            }
            try writeTableRow(writer, agg_values, null, widths);
        } else {
            if (!config.no_header) try writeRecord(writer, agg_headers, null);
            try writeRecord(writer, agg_values, null);
        }

        try bw.flush();
        return;
    }

    if (config.sample) |sample_n| {
        var reservoir = std.ArrayList(SampleRow).init(allocator);
        defer {
            for (reservoir.items) |row| {
                freeClonedFields(allocator, row.fields);
            }
            reservoir.deinit();
        }

        var rows_seen: usize = 0;
        while (true) {
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };
            if (!passesFilters(config.filters, result.fields)) continue;

            if (reservoir.items.len < sample_n) {
                // Fill phase
                const duped = try cloneFields(allocator, result.fields);
                try reservoir.append(.{ .fields = duped });
            } else {
                // Replace phase: j uniform in [0, rows_seen]
                const j = std.crypto.random.intRangeLessThan(usize, 0, rows_seen + 1);
                if (j < sample_n) {
                    const duped = try cloneFields(allocator, result.fields);
                    freeClonedFields(allocator, reservoir.items[j].fields);
                    reservoir.items[j] = .{ .fields = duped };
                }
            }
            rows_seen += 1;
        }

        try writeSampleRows(prog_alloc, writer, header, col_indices, reservoir.items, config.table, config.no_header);
        try bw.flush();
        return;
    }

    if (config.table) {
        const num_output_cols = if (col_indices) |ci| ci.len else header.len;

        const widths = try prog_alloc.alloc(usize, num_output_cols);
        if (col_indices) |ci| {
            for (ci, 0..) |c, i| {
                widths[i] = if (c < header.len) displayWidth(header[c]) else 0;
            }
        } else {
            for (header, 0..) |h, i| {
                widths[i] = displayWidth(h);
            }
        }

        var buffered_rows = std.ArrayList([]const []const u8).init(prog_alloc);
        var sample_bytes: usize = 0;

        while (sample_bytes < table_sample_budget) {
            if (config.head) |limit| {
                if (buffered_rows.items.len >= limit) break;
            }
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };

            if (passesFilters(config.filters, result.fields)) {
                const duped = try prog_alloc.alloc([]const u8, result.fields.len);
                for (result.fields, 0..) |field, i| {
                    duped[i] = try prog_alloc.dupe(u8, field);
                    sample_bytes += field.len;
                }
                try buffered_rows.append(duped);

                if (col_indices) |ci| {
                    for (ci, 0..) |c, j| {
                        if (c < duped.len) {
                            const dw = displayWidth(duped[c]);
                            if (dw > widths[j]) widths[j] = dw;
                        }
                    }
                } else {
                    for (duped, 0..) |val, j| {
                        if (j < widths.len) {
                            const dw = displayWidth(val);
                            if (dw > widths[j]) widths[j] = dw;
                        }
                    }
                }
            }
        }

        if (!config.no_header) {
            try writeTableRow(writer, header, col_indices, widths);
            try writeTableSeparator(writer, widths);
        }

        for (buffered_rows.items) |row| {
            if (config.head) |limit| {
                if (rows_written >= limit) break;
            }
            try writeTableRow(writer, row, col_indices, widths);
            rows_written += 1;
        }

        while (true) {
            if (config.head) |limit| {
                if (rows_written >= limit) break;
            }
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };

            if (passesFilters(config.filters, result.fields)) {
                try writeTableRow(writer, result.fields, col_indices, widths);
                rows_written += 1;
            }
        }
    } else {
        if (!config.no_header) {
            try writeRecord(writer, header, col_indices);
        }

        while (true) {
            if (config.head) |limit| {
                if (rows_written >= limit) break;
            }
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            line_no += 1;
            const result = parseRecord(line, &field_buf, &quoted_buf, &quote_buf) catch |err| {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error parsing CSV on line {d}: {s}\n", .{ line_no, parseRecordErrorMessage(err) });
                std.process.exit(1);
            };

            if (passesFilters(config.filters, result.fields)) {
                try writeRecordWithQuotedMask(writer, result.fields, result.quoted, col_indices);
                rows_written += 1;
            }
        }
    }

    try bw.flush();
}
