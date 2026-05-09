const std = @import("std");
const bench = @import("bench.zig");
const db_mod = @import("db.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const io = std.Options.debug_io;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var stdout_buffer: [4096]u8 = undefined;
    var stderr_buffer: [1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(io, &stdout_buffer);
    var stderr_writer = std.Io.File.stderr().writerStreaming(io, &stderr_buffer);

    var args_it = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args_it.deinit();

    var args = std.ArrayList([]const u8).empty;
    defer args.deinit(allocator);
    while (args_it.next()) |arg| {
        try args.append(allocator, arg);
    }

    if (args.items.len < 2) {
        try printUsage(&stderr_writer.interface);
        try stderr_writer.flush();
        return error.InvalidArguments;
    }

    const command = args.items[1];
    const wal_path = resolveWalPath(init);

    if (std.mem.eql(u8, command, "bench")) {
        const config = try bench.parseArgs(args.items[2..]);
        try bench.run(allocator, wal_path, config, &stdout_writer.interface);
        try stdout_writer.flush();
        return;
    }

    var db = try db_mod.KvDb.openPath(allocator, wal_path);
    defer db.close();

    if (std.mem.eql(u8, command, "repl")) {
        try runRepl(db, allocator, &stdout_writer.interface, &stderr_writer.interface);
    } else if (std.mem.eql(u8, command, "put")) {
        if (args.items.len != 4) return error.InvalidArguments;
        try db.put(args.items[2], args.items[3]);
        try stdout_writer.interface.writeAll("ok\n");
    } else if (std.mem.eql(u8, command, "get")) {
        if (args.items.len != 3) return error.InvalidArguments;
        if (try db.get(allocator, args.items[2])) |value| {
            defer allocator.free(value);
            try stdout_writer.interface.print("{s}\n", .{value});
        } else {
            try stdout_writer.interface.writeAll("(nil)\n");
        }
    } else if (std.mem.eql(u8, command, "delete")) {
        if (args.items.len != 3) return error.InvalidArguments;
        try db.delete(args.items[2]);
        try stdout_writer.interface.writeAll("ok\n");
    } else if (std.mem.eql(u8, command, "list")) {
        try db.dump(&stdout_writer.interface);
    } else {
        try printUsage(&stderr_writer.interface);
        try stderr_writer.flush();
        return error.InvalidArguments;
    }

    try stdout_writer.flush();
}

fn resolveWalPath(init: std.process.Init) []const u8 {
    return init.environ_map.get("MUND_DB_WAL") orelse "mund_db.wal";
}

fn printUsage(writer: *Io.Writer) !void {
    try writer.writeAll(
        \\usage:
        \\  MUND_DB_WAL=data.wal mund_db repl
        \\  MUND_DB_WAL=data.wal mund_db put <key> <value>
        \\  MUND_DB_WAL=data.wal mund_db get <key>
        \\  MUND_DB_WAL=data.wal mund_db delete <key>
        \\  MUND_DB_WAL=data.wal mund_db list
        \\  MUND_DB_WAL=data.wal mund_db bench [seconds] [readers] [keyspace]
        \\
    );
}

fn runRepl(db: *db_mod.KvDb, allocator: Allocator, stdout: *Io.Writer, stderr: *Io.Writer) !void {
    var stdin_buffer: [4096]u8 = undefined;
    var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buffer);

    while (true) {
        try stdout.writeAll("kv> ");

        const raw_line = stdin_reader.interface.takeDelimiterInclusive('\n') catch |err| switch (err) {
            error.EndOfStream => break,
            else => |e| return e,
        };
        const line = std.mem.trim(u8, raw_line, " \r\n\t");
        if (line.len == 0) continue;
        if (std.mem.eql(u8, line, "quit") or std.mem.eql(u8, line, "exit")) break;

        var parts = std.mem.tokenizeScalar(u8, line, ' ');
        const op = parts.next() orelse continue;

        if (std.mem.eql(u8, op, "put")) {
            const key = parts.next() orelse {
                try stderr.writeAll("usage: put <key> <value>\n");
                continue;
            };
            const value = parts.next() orelse {
                try stderr.writeAll("usage: put <key> <value>\n");
                continue;
            };
            if (parts.next() != null) {
                try stderr.writeAll("values with spaces are not supported in repl mode\n");
                continue;
            }
            try db.put(key, value);
            try stdout.writeAll("ok\n");
        } else if (std.mem.eql(u8, op, "get")) {
            const key = parts.next() orelse {
                try stderr.writeAll("usage: get <key>\n");
                continue;
            };
            if (try db.get(allocator, key)) |value| {
                defer allocator.free(value);
                try stdout.print("{s}\n", .{value});
            } else {
                try stdout.writeAll("(nil)\n");
            }
        } else if (std.mem.eql(u8, op, "delete")) {
            const key = parts.next() orelse {
                try stderr.writeAll("usage: delete <key>\n");
                continue;
            };
            try db.delete(key);
            try stdout.writeAll("ok\n");
        } else if (std.mem.eql(u8, op, "list")) {
            try db.dump(stdout);
        } else if (std.mem.eql(u8, op, "help")) {
            try stdout.writeAll("put/get/delete/list/quit\n");
        } else {
            try stderr.writeAll("unknown command\n");
        }
    }
}
