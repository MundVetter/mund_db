const std = @import("std");
const db_mod = @import("db.zig");

const io = std.Options.debug_io;

test "put get delete and recover from wal" {
    var db_allocator: std.heap.DebugAllocator(.{}) = .{};
    defer _ = db_allocator.deinit();
    const allocator = db_allocator.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const db = try db_mod.KvDb.openInDir(allocator, tmp.dir, "test.wal");
        defer db.close();

        try db.put("alpha", "1");
        try db.put("beta", "2");
        try db.delete("alpha");

        const beta = (try db.get(allocator, "beta")).?;
        defer allocator.free(beta);
        try std.testing.expectEqualStrings("2", beta);
    }

    {
        const reopened = try db_mod.KvDb.openInDir(allocator, tmp.dir, "test.wal");
        defer reopened.close();

        try std.testing.expect((try reopened.get(allocator, "alpha")) == null);

        const beta = (try reopened.get(allocator, "beta")).?;
        defer allocator.free(beta);
        try std.testing.expectEqualStrings("2", beta);
    }
}

test "concurrent readers see whole values while one writer updates" {
    var db_allocator: std.heap.DebugAllocator(.{}) = .{};
    defer _ = db_allocator.deinit();
    const allocator = db_allocator.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db = try db_mod.KvDb.openInDir(allocator, tmp.dir, "concurrent.wal");
    defer db.close();

    const value_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const value_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    try db.put("shared", value_a);

    const ReaderCtx = struct {
        db: *db_mod.KvDb,
        stop: *std.atomic.Value(bool),
        failures: *std.atomic.Value(u64),

        fn run(ctx: @This()) void {
            var buffer: [64]u8 = undefined;
            while (!ctx.stop.load(.acquire)) {
                const len = ctx.db.getInto("shared", &buffer) catch {
                    _ = ctx.failures.fetchAdd(1, .monotonic);
                    return;
                };
                const slice = buffer[0 .. len orelse {
                    _ = ctx.failures.fetchAdd(1, .monotonic);
                    return;
                }];
                if (!std.mem.eql(u8, slice, value_a) and !std.mem.eql(u8, slice, value_b)) {
                    _ = ctx.failures.fetchAdd(1, .monotonic);
                    return;
                }
            }
        }
    };

    const WriterCtx = struct {
        db: *db_mod.KvDb,
        stop: *std.atomic.Value(bool),

        fn run(ctx: @This()) void {
            var toggle = false;
            while (!ctx.stop.load(.acquire)) {
                ctx.db.put("shared", if (toggle) value_a else value_b) catch return;
                toggle = !toggle;
            }
        }
    };

    var stop = std.atomic.Value(bool).init(false);
    var failures = std.atomic.Value(u64).init(0);

    var readers: [4]std.Thread = undefined;
    for (&readers) |*thread| {
        thread.* = try std.Thread.spawn(.{}, ReaderCtx.run, .{ReaderCtx{
            .db = db,
            .stop = &stop,
            .failures = &failures,
        }});
    }

    const writer_thread = try std.Thread.spawn(.{}, WriterCtx.run, .{WriterCtx{
        .db = db,
        .stop = &stop,
    }});

    _ = try io.sleep(.fromNanoseconds(250 * std.time.ns_per_ms), .awake);
    stop.store(true, .release);

    writer_thread.join();
    for (readers) |thread| thread.join();

    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
}
