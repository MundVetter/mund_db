const std = @import("std");
const db_mod = @import("db.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const io = std.Options.debug_io;

pub const Config = struct {
    seconds: usize = 3,
    readers: usize = 4,
    keyspace: usize = 1024,
};

pub const Mode = enum {
    mixed,
    read_only,
};

pub const Result = struct {
    backend: db_mod.Backend,
    mode: Mode,
    seconds: usize,
    readers: usize,
    keyspace: usize,
    reads: u64,
    writes: u64,
    reads_per_sec: f64,
    writes_per_sec: f64,
};

pub const ScaleConfig = struct {
    seconds: usize = 3,
    keyspace: usize = 1024,
    readers: []const usize,
};

pub const MedianConfig = struct {
    seconds: usize = 3,
    readers: usize = 4,
    keyspace: usize = 1024,
    repeats: usize = 5,
};

pub const MedianScaleConfig = struct {
    seconds: usize = 3,
    keyspace: usize = 1024,
    repeats: usize = 5,
    readers: []const usize,
};

pub const Summary = struct {
    backend: db_mod.Backend,
    mode: Mode,
    seconds: usize,
    readers: usize,
    keyspace: usize,
    repeats: usize,
    reads_per_sec_p50: f64,
    reads_per_sec_min: f64,
    reads_per_sec_max: f64,
    writes_per_sec_p50: f64,
    writes_per_sec_min: f64,
    writes_per_sec_max: f64,
};

const WriterCtx = struct {
    db: *db_mod.KvDb,
    stop: *std.atomic.Value(bool),
    keys: []const []const u8,
    writes: u64 = 0,
};

const ReaderCtx = struct {
    db: *db_mod.KvDb,
    stop: *std.atomic.Value(bool),
    keys: []const []const u8,
    seed: u64,
    reads: u64 = 0,
};

pub fn parseArgs(args: []const []const u8) !Config {
    var config = Config{};
    if (args.len > 0) config.seconds = try std.fmt.parseInt(usize, args[0], 10);
    if (args.len > 1) config.readers = try std.fmt.parseInt(usize, args[1], 10);
    if (args.len > 2) config.keyspace = try std.fmt.parseInt(usize, args[2], 10);
    if (config.seconds == 0 or config.readers == 0 or config.keyspace == 0) return error.InvalidArguments;
    return config;
}

pub fn parseMedianArgs(args: []const []const u8) !MedianConfig {
    var config = MedianConfig{};
    if (args.len > 0) config.seconds = try std.fmt.parseInt(usize, args[0], 10);
    if (args.len > 1) config.readers = try std.fmt.parseInt(usize, args[1], 10);
    if (args.len > 2) config.keyspace = try std.fmt.parseInt(usize, args[2], 10);
    if (args.len > 3) config.repeats = try std.fmt.parseInt(usize, args[3], 10);
    if (config.seconds == 0 or config.readers == 0 or config.keyspace == 0 or config.repeats == 0) return error.InvalidArguments;
    return config;
}

pub fn parseScaleArgs(allocator: Allocator, args: []const []const u8) !ScaleConfig {
    var seconds: usize = 3;
    var keyspace: usize = 1024;

    if (args.len > 0) seconds = try std.fmt.parseInt(usize, args[0], 10);
    if (args.len > 1) keyspace = try std.fmt.parseInt(usize, args[1], 10);
    if (seconds == 0 or keyspace == 0) return error.InvalidArguments;

    const counts = if (args.len > 2) blk: {
        const parsed = try allocator.alloc(usize, args.len - 2);
        errdefer allocator.free(parsed);
        for (args[2..], 0..) |arg, idx| {
            parsed[idx] = try std.fmt.parseInt(usize, arg, 10);
            if (parsed[idx] == 0) return error.InvalidArguments;
        }
        break :blk parsed;
    } else try allocator.dupe(usize, &.{ 1, 2, 4, 8, 16 });

    return .{
        .seconds = seconds,
        .keyspace = keyspace,
        .readers = counts,
    };
}

pub fn parseMedianScaleArgs(allocator: Allocator, args: []const []const u8) !MedianScaleConfig {
    var seconds: usize = 3;
    var keyspace: usize = 1024;
    var repeats: usize = 5;

    if (args.len > 0) seconds = try std.fmt.parseInt(usize, args[0], 10);
    if (args.len > 1) keyspace = try std.fmt.parseInt(usize, args[1], 10);
    if (args.len > 2) repeats = try std.fmt.parseInt(usize, args[2], 10);
    if (seconds == 0 or keyspace == 0 or repeats == 0) return error.InvalidArguments;

    const counts = if (args.len > 3) blk: {
        const parsed = try allocator.alloc(usize, args.len - 3);
        errdefer allocator.free(parsed);
        for (args[3..], 0..) |arg, idx| {
            parsed[idx] = try std.fmt.parseInt(usize, arg, 10);
            if (parsed[idx] == 0) return error.InvalidArguments;
        }
        break :blk parsed;
    } else try allocator.dupe(usize, &.{ 1, 2, 4, 8, 16 });

    return .{
        .seconds = seconds,
        .keyspace = keyspace,
        .repeats = repeats,
        .readers = counts,
    };
}

pub fn run(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: Config, writer: *Io.Writer) !void {
    const result = try runOneMode(allocator, wal_path, backend, .mixed, config);
    try printResult(writer, result);
}

pub fn runReadOnly(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: Config, writer: *Io.Writer) !void {
    const result = try runOneMode(allocator, wal_path, backend, .read_only, config);
    try printResult(writer, result);
}

pub fn runMedian(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: MedianConfig, writer: *Io.Writer) !void {
    const summary = try runMedianMode(allocator, wal_path, backend, .mixed, config);
    try printSummary(writer, summary);
}

pub fn runReadOnlyMedian(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: MedianConfig, writer: *Io.Writer) !void {
    const summary = try runMedianMode(allocator, wal_path, backend, .read_only, config);
    try printSummary(writer, summary);
}

pub fn runScale(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: ScaleConfig, writer: *Io.Writer) !void {
    try runScaleMode(allocator, wal_path, backend, .mixed, config, writer);
}

pub fn runReadOnlyScale(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: ScaleConfig, writer: *Io.Writer) !void {
    try runScaleMode(allocator, wal_path, backend, .read_only, config, writer);
}

pub fn runScaleMedian(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: MedianScaleConfig, writer: *Io.Writer) !void {
    try runScaleMedianMode(allocator, wal_path, backend, .mixed, config, writer);
}

pub fn runReadOnlyScaleMedian(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: MedianScaleConfig, writer: *Io.Writer) !void {
    try runScaleMedianMode(allocator, wal_path, backend, .read_only, config, writer);
}

fn runScaleMode(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, mode: Mode, config: ScaleConfig, writer: *Io.Writer) !void {
    defer allocator.free(config.readers);

    const results = try allocator.alloc(Result, config.readers.len);
    defer allocator.free(results);

    for (config.readers, 0..) |reader_count, idx| {
        var path_buf: [256]u8 = undefined;
        const sample_path = try std.fmt.bufPrint(&path_buf, "{s}.r{d}.wal", .{ wal_path, reader_count });
        results[idx] = try runOneMode(allocator, sample_path, backend, mode, .{
            .seconds = config.seconds,
            .readers = reader_count,
            .keyspace = config.keyspace,
        });
    }

    try writer.print(
        "reader scaling: backend={s} mode={s} seconds={d} keyspace={d}\n",
        .{ db_mod.backendName(backend), modeName(mode), config.seconds, config.keyspace },
    );
    try writer.writeAll("readers | reads/sec   | writes/sec  | read scale\n");
    try writer.writeAll("--------+-------------+-------------+--------------------------------\n");

    const base_reads = results[0].reads_per_sec;
    var max_reads = results[0].reads_per_sec;
    for (results[1..]) |result| {
        max_reads = @max(max_reads, result.reads_per_sec);
    }

    for (results) |result| {
        const bar_len = @max(@as(usize, 1), @as(usize, @intFromFloat((result.reads_per_sec / max_reads) * 32.0)));
        var bar_buf: [32]u8 = undefined;
        @memset(bar_buf[0..bar_len], '#');
        try writer.print(
            "{d: >7} | {d: >11.0} | {d: >11.0} | {s} {d:.2}x\n",
            .{
                result.readers,
                result.reads_per_sec,
                result.writes_per_sec,
                bar_buf[0..bar_len],
                result.reads_per_sec / base_reads,
            },
        );
    }
}

fn runScaleMedianMode(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, mode: Mode, config: MedianScaleConfig, writer: *Io.Writer) !void {
    defer allocator.free(config.readers);

    const summaries = try allocator.alloc(Summary, config.readers.len);
    defer allocator.free(summaries);

    for (config.readers, 0..) |reader_count, idx| {
        var path_buf: [256]u8 = undefined;
        const sample_path = try std.fmt.bufPrint(&path_buf, "{s}.r{d}", .{ wal_path, reader_count });
        summaries[idx] = try runMedianMode(allocator, sample_path, backend, mode, .{
            .seconds = config.seconds,
            .readers = reader_count,
            .keyspace = config.keyspace,
            .repeats = config.repeats,
        });
    }

    try writer.print(
        "reader scaling median: backend={s} mode={s} repeats={d} seconds={d} keyspace={d}\n",
        .{ db_mod.backendName(backend), modeName(mode), config.repeats, config.seconds, config.keyspace },
    );
    try writer.writeAll("readers | reads/sec p50 | writes/sec p50 | read scale | graph\n");
    try writer.writeAll("--------+---------------+----------------+------------+--------------------------------\n");

    const base_reads = summaries[0].reads_per_sec_p50;
    var max_reads = summaries[0].reads_per_sec_p50;
    for (summaries[1..]) |summary| {
        max_reads = @max(max_reads, summary.reads_per_sec_p50);
    }

    for (summaries) |summary| {
        const bar_len = @max(@as(usize, 1), @as(usize, @intFromFloat((summary.reads_per_sec_p50 / max_reads) * 32.0)));
        var bar_buf: [32]u8 = undefined;
        @memset(bar_buf[0..bar_len], '#');
        try writer.print(
            "{d: >7} | {d: >13.0} | {d: >14.0} | {d: >8.2}x | {s}\n",
            .{
                summary.readers,
                summary.reads_per_sec_p50,
                summary.writes_per_sec_p50,
                summary.reads_per_sec_p50 / base_reads,
                bar_buf[0..bar_len],
            },
        );
        try writer.print(
            "        | read {d:.0}..{d:.0} | write {d:.0}..{d:.0}\n",
            .{
                summary.reads_per_sec_min,
                summary.reads_per_sec_max,
                summary.writes_per_sec_min,
                summary.writes_per_sec_max,
            },
        );
    }
}

pub fn runOne(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, config: Config) !Result {
    return runOneMode(allocator, wal_path, backend, .mixed, config);
}

pub fn runMedianMode(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, mode: Mode, config: MedianConfig) !Summary {
    const results = try allocator.alloc(Result, config.repeats);
    defer allocator.free(results);

    for (0..config.repeats) |idx| {
        var path_buf: [320]u8 = undefined;
        const sample_path = try std.fmt.bufPrint(&path_buf, "{s}.rep{d}.wal", .{ wal_path, idx });
        results[idx] = try runOneMode(allocator, sample_path, backend, mode, .{
            .seconds = config.seconds,
            .readers = config.readers,
            .keyspace = config.keyspace,
        });
    }

    return summarizeResults(allocator, results, config.repeats);
}

pub fn runOneMode(allocator: Allocator, wal_path: []const u8, backend: db_mod.Backend, mode: Mode, config: Config) !Result {
    const db = try db_mod.KvDb.openPath(allocator, wal_path, backend);
    defer db.close();

    const keys = try prepareKeys(allocator, config.keyspace);
    defer freeKeys(allocator, keys);

    for (keys, 0..) |key, idx| {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "seed-{d}", .{idx});
        try db.put(key, value);
    }

    var stop = std.atomic.Value(bool).init(false);

    const reader_threads = try allocator.alloc(std.Thread, config.readers);
    defer allocator.free(reader_threads);
    const reader_contexts = try allocator.alloc(ReaderCtx, config.readers);
    defer allocator.free(reader_contexts);
    const writer_ctx = if (mode == .mixed) try allocator.create(WriterCtx) else null;
    defer if (writer_ctx) |ctx| allocator.destroy(ctx);
    if (writer_ctx) |ctx| {
        ctx.* = .{
            .db = db,
            .stop = &stop,
            .keys = keys,
        };
    }
    const writer_thread = if (writer_ctx) |ctx| try std.Thread.spawn(.{}, writerLoop, .{ctx}) else null;

    for (reader_threads, 0..) |*thread, idx| {
        reader_contexts[idx] = .{
            .db = db,
            .stop = &stop,
            .keys = keys,
            .seed = @intCast(idx + 1),
        };
        thread.* = try std.Thread.spawn(.{}, readerLoop, .{&reader_contexts[idx]});
    }

    _ = try io.sleep(.fromNanoseconds(config.seconds * std.time.ns_per_s), .awake);
    stop.store(true, .release);

    if (writer_thread) |thread| thread.join();
    for (reader_threads) |thread| thread.join();

    var total_reads: u64 = 0;
    for (reader_contexts) |ctx| total_reads += ctx.reads;
    const total_writes = if (writer_ctx) |ctx| ctx.writes else 0;
    const seconds_f = @as(f64, @floatFromInt(config.seconds));

    return .{
        .backend = backend,
        .mode = mode,
        .seconds = config.seconds,
        .readers = config.readers,
        .keyspace = config.keyspace,
        .reads = total_reads,
        .writes = total_writes,
        .reads_per_sec = @as(f64, @floatFromInt(total_reads)) / seconds_f,
        .writes_per_sec = @as(f64, @floatFromInt(total_writes)) / seconds_f,
    };
}

pub fn printResult(writer: *Io.Writer, result: Result) !void {
    try writer.print(
        "backend={s} mode={s} seconds={d} readers={d} keyspace={d} reads={d} writes={d} reads_per_sec={d:.2} writes_per_sec={d:.2}\n",
        .{
            db_mod.backendName(result.backend),
            modeName(result.mode),
            result.seconds,
            result.readers,
            result.keyspace,
            result.reads,
            result.writes,
            result.reads_per_sec,
            result.writes_per_sec,
        },
    );
}

pub fn printSummary(writer: *Io.Writer, summary: Summary) !void {
    try writer.print(
        "backend={s} mode={s} repeats={d} seconds={d} readers={d} keyspace={d} reads_per_sec_p50={d:.2} reads_per_sec_min={d:.2} reads_per_sec_max={d:.2} writes_per_sec_p50={d:.2} writes_per_sec_min={d:.2} writes_per_sec_max={d:.2}\n",
        .{
            db_mod.backendName(summary.backend),
            modeName(summary.mode),
            summary.repeats,
            summary.seconds,
            summary.readers,
            summary.keyspace,
            summary.reads_per_sec_p50,
            summary.reads_per_sec_min,
            summary.reads_per_sec_max,
            summary.writes_per_sec_p50,
            summary.writes_per_sec_min,
            summary.writes_per_sec_max,
        },
    );
}

fn writerLoop(ctx: *WriterCtx) void {
    var prng = std.Random.DefaultPrng.init(0xdecafbad);
    const random = prng.random();
    var counter: u64 = 0;

    while (!ctx.stop.load(.acquire)) {
        var value_buf: [64]u8 = undefined;
        const key = ctx.keys[random.uintLessThan(usize, ctx.keys.len)];
        const value = std.fmt.bufPrint(&value_buf, "value-{d}", .{counter}) catch return;
        ctx.db.put(key, value) catch return;
        ctx.writes += 1;
        counter += 1;
    }
}

fn readerLoop(ctx: *ReaderCtx) void {
    var prng = std.Random.DefaultPrng.init(ctx.seed);
    const random = prng.random();
    var value_buf: [128]u8 = undefined;

    while (!ctx.stop.load(.acquire)) {
        const key = ctx.keys[random.uintLessThan(usize, ctx.keys.len)];
        _ = ctx.db.getInto(key, &value_buf) catch return;
        ctx.reads += 1;
    }
}

fn modeName(mode: Mode) []const u8 {
    return switch (mode) {
        .mixed => "mixed",
        .read_only => "read_only",
    };
}

fn prepareKeys(allocator: Allocator, keyspace: usize) ![][]const u8 {
    const keys = try allocator.alloc([]const u8, keyspace);
    errdefer allocator.free(keys);

    var built: usize = 0;
    errdefer {
        for (keys[0..built]) |key| allocator.free(key);
    }

    for (0..keyspace) |idx| {
        var key_buf: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{idx});
        keys[idx] = try allocator.dupe(u8, key);
        built += 1;
    }
    return keys;
}

fn freeKeys(allocator: Allocator, keys: [][]const u8) void {
    for (keys) |key| allocator.free(key);
    allocator.free(keys);
}

fn summarizeResults(allocator: Allocator, results: []const Result, repeats: usize) !Summary {
    const reads = try allocator.alloc(f64, results.len);
    defer allocator.free(reads);
    const writes = try allocator.alloc(f64, results.len);
    defer allocator.free(writes);

    var read_min = results[0].reads_per_sec;
    var read_max = results[0].reads_per_sec;
    var write_min = results[0].writes_per_sec;
    var write_max = results[0].writes_per_sec;

    for (results, 0..) |result, idx| {
        reads[idx] = result.reads_per_sec;
        writes[idx] = result.writes_per_sec;
        read_min = @min(read_min, result.reads_per_sec);
        read_max = @max(read_max, result.reads_per_sec);
        write_min = @min(write_min, result.writes_per_sec);
        write_max = @max(write_max, result.writes_per_sec);
    }

    std.mem.sort(f64, reads, {}, lessThanF64);
    std.mem.sort(f64, writes, {}, lessThanF64);

    return .{
        .backend = results[0].backend,
        .mode = results[0].mode,
        .seconds = results[0].seconds,
        .readers = results[0].readers,
        .keyspace = results[0].keyspace,
        .repeats = repeats,
        .reads_per_sec_p50 = medianOfSorted(reads),
        .reads_per_sec_min = read_min,
        .reads_per_sec_max = read_max,
        .writes_per_sec_p50 = medianOfSorted(writes),
        .writes_per_sec_min = write_min,
        .writes_per_sec_max = write_max,
    };
}

fn lessThanF64(_: void, lhs: f64, rhs: f64) bool {
    return lhs < rhs;
}

fn medianOfSorted(values: []const f64) f64 {
    const mid = values.len / 2;
    if ((values.len & 1) == 1) return values[mid];
    return (values[mid - 1] + values[mid]) / 2.0;
}
