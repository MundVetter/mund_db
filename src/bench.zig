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

const WriterCtx = struct {
    db: *db_mod.KvDb,
    stop: *std.atomic.Value(bool),
    writes: *std.atomic.Value(u64),
    keyspace: usize,
};

const ReaderCtx = struct {
    db: *db_mod.KvDb,
    stop: *std.atomic.Value(bool),
    reads: *std.atomic.Value(u64),
    keyspace: usize,
    seed: u64,
};

pub fn parseArgs(args: []const []const u8) !Config {
    var config = Config{};
    if (args.len > 0) config.seconds = try std.fmt.parseInt(usize, args[0], 10);
    if (args.len > 1) config.readers = try std.fmt.parseInt(usize, args[1], 10);
    if (args.len > 2) config.keyspace = try std.fmt.parseInt(usize, args[2], 10);
    if (config.seconds == 0 or config.readers == 0 or config.keyspace == 0) return error.InvalidArguments;
    return config;
}

pub fn run(allocator: Allocator, wal_path: []const u8, config: Config, writer: *Io.Writer) !void {
    const db = try db_mod.KvDb.openPath(allocator, wal_path);
    defer db.close();

    for (0..config.keyspace) |idx| {
        var key_buf: [64]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{idx});
        const value = try std.fmt.bufPrint(&value_buf, "seed-{d}", .{idx});
        try db.put(key, value);
    }

    var stop = std.atomic.Value(bool).init(false);
    var reads = std.atomic.Value(u64).init(0);
    var writes = std.atomic.Value(u64).init(0);

    const reader_threads = try allocator.alloc(std.Thread, config.readers);
    defer allocator.free(reader_threads);
    const reader_contexts = try allocator.alloc(ReaderCtx, config.readers);
    defer allocator.free(reader_contexts);

    const writer_thread = try std.Thread.spawn(.{}, writerLoop, .{WriterCtx{
        .db = db,
        .stop = &stop,
        .writes = &writes,
        .keyspace = config.keyspace,
    }});

    for (reader_threads, 0..) |*thread, idx| {
        reader_contexts[idx] = .{
            .db = db,
            .stop = &stop,
            .reads = &reads,
            .keyspace = config.keyspace,
            .seed = @intCast(idx + 1),
        };
        thread.* = try std.Thread.spawn(.{}, readerLoop, .{reader_contexts[idx]});
    }

    _ = try io.sleep(.fromNanoseconds(config.seconds * std.time.ns_per_s), .awake);
    stop.store(true, .release);

    writer_thread.join();
    for (reader_threads) |thread| thread.join();

    const total_reads = reads.load(.acquire);
    const total_writes = writes.load(.acquire);
    const seconds_f = @as(f64, @floatFromInt(config.seconds));

    try writer.print(
        "seconds={d} readers={d} keyspace={d} reads={d} writes={d} reads_per_sec={d:.2} writes_per_sec={d:.2}\n",
        .{
            config.seconds,
            config.readers,
            config.keyspace,
            total_reads,
            total_writes,
            @as(f64, @floatFromInt(total_reads)) / seconds_f,
            @as(f64, @floatFromInt(total_writes)) / seconds_f,
        },
    );
}

fn writerLoop(ctx: WriterCtx) void {
    var prng = std.Random.DefaultPrng.init(0xdecafbad);
    const random = prng.random();
    var counter: u64 = 0;

    while (!ctx.stop.load(.acquire)) {
        var key_buf: [64]u8 = undefined;
        var value_buf: [64]u8 = undefined;
        const idx = random.uintLessThan(usize, ctx.keyspace);
        const key = std.fmt.bufPrint(&key_buf, "key-{d}", .{idx}) catch return;
        const value = std.fmt.bufPrint(&value_buf, "value-{d}", .{counter}) catch return;
        ctx.db.put(key, value) catch return;
        _ = ctx.writes.fetchAdd(1, .monotonic);
        counter += 1;
    }
}

fn readerLoop(ctx: ReaderCtx) void {
    var prng = std.Random.DefaultPrng.init(ctx.seed);
    const random = prng.random();
    var value_buf: [128]u8 = undefined;

    while (!ctx.stop.load(.acquire)) {
        var key_buf: [64]u8 = undefined;
        const idx = random.uintLessThan(usize, ctx.keyspace);
        const key = std.fmt.bufPrint(&key_buf, "key-{d}", .{idx}) catch return;
        _ = ctx.db.getInto(key, &value_buf) catch return;
        _ = ctx.reads.fetchAdd(1, .monotonic);
    }
}
