const std = @import("std");

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
            const field = expr[0..pos];
            const value = expr[pos + entry.text.len ..];
            return Filter{
                .field = field,
                .op = entry.op,
                .value = value,
            };
        }
    }
    return null;
}

fn parseRecord(allocator: std.mem.Allocator, line: []const u8) ![][]const u8 {
    var fields = std.ArrayList([]const u8).init(allocator);
    var i: usize = 0;

    while (i <= line.len) {
        if (i == line.len) {
            if (line.len > 0 and line[line.len - 1] == ',') {
                try fields.append("");
            }
            break;
        }

        if (line[i] == '"') {
            i += 1;
            var unquoted = std.ArrayList(u8).init(allocator);
            while (i < line.len) {
                if (line[i] == '"') {
                    if (i + 1 < line.len and line[i + 1] == '"') {
                        try unquoted.append('"');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    try unquoted.append(line[i]);
                    i += 1;
                }
            }
            try fields.append(unquoted.items);
            if (i < line.len and line[i] == ',') {
                i += 1;
            } else {
                break;
            }
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') : (i += 1) {}
            try fields.append(line[start..i]);
            if (i < line.len) {
                i += 1;
            } else {
                break;
            }
        }
    }

    return fields.items;
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

const max_line_len: usize = 1024 * 1024;
const table_sample_budget: usize = 1024 * 1024;

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
            if (val.len < widths[i]) {
                try writer.writeByteNTimes(' ', widths[i] - val.len);
            }
        }
    } else {
        for (widths, 0..) |w, i| {
            if (i > 0) try writer.writeAll(" | ");
            const val = if (i < fields.len) fields[i] else "";
            try writer.writeAll(val);
            if (val.len < w) {
                try writer.writeByteNTimes(' ', w - val.len);
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

fn readNextRecord(reader: anytype, alloc: std.mem.Allocator) !?[][]const u8 {
    while (true) {
        const line = reader.readUntilDelimiterOrEofAlloc(alloc, '\n', max_line_len) catch |err| {
            const stderr = std.io.getStdErr().writer();
            try stderr.print("Error reading line: {}\n", .{err});
            return null;
        } orelse return null;

        const clean = if (line.len > 0 and line[line.len - 1] == '\r')
            line[0 .. line.len - 1]
        else
            line;

        if (clean.len == 0) continue;

        return try parseRecord(alloc, clean);
    }
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
    var reader = stdin.reader();

    const stdout = std.io.getStdOut();
    var bw = std.io.bufferedWriter(stdout.writer());
    const writer = bw.writer();

    const header_line = reader.readUntilDelimiterOrEofAlloc(prog_alloc, '\n', max_line_len) catch |err| {
        const stderr = std.io.getStdErr().writer();
        try stderr.print("Error reading header: {}\n", .{err});
        return;
    } orelse return;

    const clean_header_line = if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line[0 .. header_line.len - 1]
    else
        header_line;

    const header = try parseRecord(prog_alloc, clean_header_line);

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

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    if (config.table) {
        const num_output_cols = if (col_indices) |ci| ci.len else header.len;

        const widths = try prog_alloc.alloc(usize, num_output_cols);
        if (col_indices) |ci| {
            for (ci, 0..) |c, i| {
                widths[i] = if (c < header.len) header[c].len else 0;
            }
        } else {
            for (header, 0..) |h, i| {
                widths[i] = h.len;
            }
        }

        var buffered_rows = std.ArrayList([]const []const u8).init(prog_alloc);
        var sample_bytes: usize = 0;

        while (sample_bytes < table_sample_budget) {
            _ = arena.reset(.retain_capacity);
            const fields = try readNextRecord(&reader, arena.allocator()) orelse break;

            if (passesFilters(config.filters, fields)) {
                const duped = try prog_alloc.alloc([]const u8, fields.len);
                for (fields, 0..) |field, i| {
                    duped[i] = try prog_alloc.dupe(u8, field);
                    sample_bytes += field.len;
                }
                try buffered_rows.append(duped);

                if (col_indices) |ci| {
                    for (ci, 0..) |c, j| {
                        if (c < duped.len and duped[c].len > widths[j]) {
                            widths[j] = duped[c].len;
                        }
                    }
                } else {
                    for (duped, 0..) |val, j| {
                        if (j < widths.len and val.len > widths[j]) {
                            widths[j] = val.len;
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
            _ = arena.reset(.retain_capacity);
            const fields = try readNextRecord(&reader, arena.allocator()) orelse break;

            if (passesFilters(config.filters, fields)) {
                try writeTableRow(writer, fields, col_indices, widths);
            }
        }
    } else {
        if (!config.no_header) {
            try writeRecord(writer, header, col_indices);
        }

        while (true) {
            _ = arena.reset(.retain_capacity);
            const fields = try readNextRecord(&reader, arena.allocator()) orelse break;

            if (passesFilters(config.filters, fields)) {
                try writeRecord(writer, fields, col_indices);
            }
        }
    }

    try bw.flush();
}
