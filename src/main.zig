const std = @import("std");

const max_line_len: usize = 1024 * 1024;
const max_fields: usize = 4096;
const read_buf_size: usize = 256 * 1024;
const table_sample_budget: usize = 1024 * 1024;

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
};

const Config = struct {
    selectors: ?[]const []const u8 = null,
    filters: ?[]Filter = null,
    no_header: bool = false,
    table: bool = false,
};

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
        \\  -t, --table           Pretty-print as aligned table
        \\      --no-header       Suppress header row in output
        \\  -h, --help            Print this help message
        \\
    );
}

fn parseArgs(allocator: std.mem.Allocator) !?Config {
    var args_iter = try std.process.argsWithAllocator(allocator);
    defer args_iter.deinit();

    _ = args_iter.next();

    var selectors = std.ArrayList([]const u8).init(allocator);
    var filters = std.ArrayList(Filter).init(allocator);
    var no_header = false;
    var table = false;

    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            const stdout = std.io.getStdOut().writer();
            try printUsage(stdout);
            return null;
        } else if (std.mem.eql(u8, arg, "-t") or std.mem.eql(u8, arg, "--table")) {
            table = true;
        } else if (std.mem.eql(u8, arg, "--no-header")) {
            no_header = true;
        } else if (std.mem.eql(u8, arg, "-s") or std.mem.eql(u8, arg, "--select")) {
            const value = args_iter.next() orelse {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --select requires an argument\n");
                return null;
            };
            var it = std.mem.splitScalar(u8, value, ',');
            while (it.next()) |field| {
                if (field.len > 0) {
                    try selectors.append(field);
                }
            }
        } else if (std.mem.eql(u8, arg, "-f") or std.mem.eql(u8, arg, "--filter")) {
            const value = args_iter.next() orelse {
                const stderr = std.io.getStdErr().writer();
                try stderr.writeAll("Error: --filter requires an argument\n");
                return null;
            };
            const filter = parseFilter(value) orelse {
                const stderr = std.io.getStdErr().writer();
                try stderr.print("Error: invalid filter expression: {s}\n", .{value});
                return null;
            };
            try filters.append(filter);
        } else {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("Error: unknown argument: {s}\n", .{arg});
            try printUsage(stderr);
            return null;
        }
    }

    return Config{
        .selectors = if (selectors.items.len > 0) selectors.items else null,
        .filters = if (filters.items.len > 0) filters.items else null,
        .no_header = no_header,
        .table = table,
    };
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
            return Filter{
                .field = field,
                .op = entry.op,
                .value = value,
            };
        }
    }
    return null;
}

/// Parses a CSV line into fields. Returns slices into `line` for unquoted fields
/// and slices into `quote_buf` for quoted fields containing escaped quotes.
fn parseRecord(line: []const u8, out: [][]const u8, quote_buf: []u8) struct { fields: [][]const u8, quote_buf_used: usize } {
    var n: usize = 0;
    var qb_used: usize = 0;
    var i: usize = 0;

    while (i <= line.len and n < out.len) {
        if (i == line.len) {
            if (line.len > 0 and line[line.len - 1] == ',') {
                out[n] = "";
                n += 1;
            }
            break;
        }

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
            if (!has_escapes) {
                // Fast path: slice directly into line.
                out[n] = line[i..scan];
                n += 1;
                i = if (scan < line.len) scan + 1 else scan; // skip closing quote
                if (i < line.len and line[i] == ',') {
                    i += 1;
                } else {
                    break;
                }
            } else {
                // Slow path: copy unescaped content into quote_buf.
                const start = qb_used;
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
                out[n] = quote_buf[start..qb_used];
                n += 1;
                if (i < line.len and line[i] == ',') {
                    i += 1;
                } else {
                    break;
                }
            }
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') : (i += 1) {}
            out[n] = line[start..i];
            n += 1;
            if (i < line.len) {
                i += 1;
            } else {
                break;
            }
        }
    }

    return .{ .fields = out[0..n], .quote_buf_used = qb_used };
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

    const field_num = std.fmt.parseFloat(f64, field_val) catch null;
    const filter_num = std.fmt.parseFloat(f64, filter_val) catch null;

    if (field_num != null and filter_num != null) {
        const a = field_num.?;
        const b = filter_num.?;
        return switch (filter.op) {
            .eq => a == b,
            .neq => a != b,
            .lt => a < b,
            .gt => a > b,
            .lte => a <= b,
            .gte => a >= b,
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

fn passesFilters(filters: ?[]Filter, fields: []const []const u8) bool {
    const ff = filters orelse return true;
    for (ff) |*f| {
        if (!evaluateFilter(f, fields)) return false;
    }
    return true;
}

fn readNextLine(reader: anytype, buf: []u8) !?[]const u8 {
    while (true) {
        const line = reader.readUntilDelimiterOrEof(buf, '\n') catch {
            return error.LineTooLong;
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
    const r = parseRecord(line, &result.out, &result.qbuf);
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
    const fields = [_][]const u8{ "42" };
    var f = Filter{ .field = "x", .op = .eq, .value = "42", .col_index = 0 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "43";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: string comparison fallback" {
    const fields = [_][]const u8{ "banana" };
    var f = Filter{ .field = "fruit", .op = .gt, .value = "apple", .col_index = 0 };
    try std.testing.expect(evaluateFilter(&f, &fields));

    f.value = "cherry";
    try std.testing.expect(!evaluateFilter(&f, &fields));
}

test "evaluateFilter: glob operator" {
    const fields = [_][]const u8{ "bc-west" };
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

    // Stack-allocated line buffer and field/quote scratch space.
    var line_buf: [max_line_len]u8 = undefined;
    var field_buf: [max_fields][]const u8 = undefined;
    var quote_buf: [max_line_len]u8 = undefined;

    const header_line = readNextLine(&reader, &line_buf) catch {
        const stderr = std.io.getStdErr().writer();
        try stderr.writeAll("Error reading header: line too long\n");
        return;
    } orelse return;

    // Header must be heap-duped since line_buf will be reused.
    const header_line_copy = try prog_alloc.dupe(u8, header_line);
    const header_result = parseRecord(header_line_copy, &field_buf, &quote_buf);
    const header = try prog_alloc.dupe([]const u8, header_result.fields);

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
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            const result = parseRecord(line, &field_buf, &quote_buf);

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
            try writeTableRow(writer, row, col_indices, widths);
        }

        while (true) {
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            const result = parseRecord(line, &field_buf, &quote_buf);

            if (passesFilters(config.filters, result.fields)) {
                try writeTableRow(writer, result.fields, col_indices, widths);
            }
        }
    } else {
        if (!config.no_header) {
            try writeRecord(writer, header, col_indices);
        }

        while (true) {
            const line = readNextLine(&reader, &line_buf) catch break orelse break;
            const result = parseRecord(line, &field_buf, &quote_buf);

            if (passesFilters(config.filters, result.fields)) {
                try writeRecord(writer, result.fields, col_indices);
            }
        }
    }

    try bw.flush();
}
