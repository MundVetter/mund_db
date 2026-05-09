const std = @import("std");
const types = @import("types.zig");
const wal = @import("wal.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const io = std.Options.debug_io;

const WriteRequest = struct {
    next: ?*WriteRequest = null,
    mutex: Io.Mutex = .init,
    condition: Io.Condition = .init,
    completed: bool = false,
    result: ?anyerror = null,
    op: types.Op,
    key: []u8,
    value: []u8,

    fn wait(self: *WriteRequest) anyerror!void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        while (!self.completed) {
            self.condition.waitUncancelable(io, &self.mutex);
        }
        if (self.result) |err| return err;
    }

    fn finish(self: *WriteRequest, result: ?anyerror) void {
        self.mutex.lockUncancelable(io);
        self.completed = true;
        self.result = result;
        self.condition.signal(io);
        self.mutex.unlock(io);
    }
};

const WriteQueue = struct {
    mutex: Io.Mutex = .init,
    condition: Io.Condition = .init,
    head: ?*WriteRequest = null,
    tail: ?*WriteRequest = null,
    closed: bool = false,

    fn enqueue(self: *WriteQueue, request: *WriteRequest) !void {
        request.next = null;

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.closed) return error.DatabaseClosed;

        if (self.tail) |tail| {
            tail.next = request;
        } else {
            self.head = request;
        }
        self.tail = request;
        self.condition.signal(io);
    }

    fn close(self: *WriteQueue) void {
        self.mutex.lockUncancelable(io);
        self.closed = true;
        self.condition.broadcast(io);
        self.mutex.unlock(io);
    }

    fn dequeue(self: *WriteQueue) ?*WriteRequest {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        while (self.head == null and !self.closed) {
            self.condition.waitUncancelable(io, &self.mutex);
        }

        const request = self.head orelse return null;
        self.head = request.next;
        if (self.head == null) self.tail = null;
        request.next = null;
        return request;
    }
};

const Shard = struct {
    lock: Io.RwLock = .init,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *Shard, allocator: Allocator) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.map.deinit(allocator);
    }
};

pub const Entry = struct {
    key: []u8,
    value: []u8,
};

pub const KvDb = struct {
    allocator: Allocator,
    wal_file: Io.File,
    queue: WriteQueue = .{},
    writer_thread: ?std.Thread = null,
    shards: [types.shard_count]Shard = undefined,

    pub fn openPath(allocator: Allocator, wal_path: []const u8) !*KvDb {
        return openInDir(allocator, Io.Dir.cwd(), wal_path);
    }

    pub fn openInDir(allocator: Allocator, dir: Io.Dir, wal_path: []const u8) !*KvDb {
        var wal_file = try dir.createFile(io, wal_path, .{
            .read = true,
            .truncate = false,
            .lock = .exclusive,
        });
        errdefer wal_file.close(io);

        const db = try allocator.create(KvDb);
        errdefer allocator.destroy(db);

        db.* = .{
            .allocator = allocator,
            .wal_file = wal_file,
        };
        errdefer db.deinitShards();

        for (&db.shards) |*shard| shard.* = .{};

        try db.replayWal();
        db.writer_thread = try std.Thread.spawn(.{}, writerMain, .{db});
        return db;
    }

    pub fn close(self: *KvDb) void {
        self.queue.close();
        if (self.writer_thread) |thread| thread.join();
        self.wal_file.close(io);
        self.deinitShards();
        self.allocator.destroy(self);
    }

    pub fn put(self: *KvDb, key: []const u8, value: []const u8) !void {
        if (key.len == 0) return error.EmptyKey;
        if (key.len > types.max_record_len or value.len > types.max_record_len) return error.RecordTooLarge;

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        var request: WriteRequest = .{
            .op = .put,
            .key = owned_key,
            .value = owned_value,
        };
        try self.queue.enqueue(&request);
        try request.wait();
    }

    pub fn delete(self: *KvDb, key: []const u8) !void {
        if (key.len == 0) return error.EmptyKey;
        if (key.len > types.max_record_len) return error.RecordTooLarge;

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        var request: WriteRequest = .{
            .op = .delete,
            .key = owned_key,
            .value = &.{},
        };
        try self.queue.enqueue(&request);
        try request.wait();
    }

    pub fn get(self: *KvDb, allocator: Allocator, key: []const u8) !?[]u8 {
        const shard = self.shardFor(key);
        shard.lock.lockSharedUncancelable(io);
        defer shard.lock.unlockShared(io);

        const value = shard.map.get(key) orelse return null;
        return try allocator.dupe(u8, value);
    }

    pub fn getInto(self: *KvDb, key: []const u8, buffer: []u8) !?usize {
        const shard = self.shardFor(key);
        shard.lock.lockSharedUncancelable(io);
        defer shard.lock.unlockShared(io);

        const value = shard.map.get(key) orelse return null;
        if (value.len > buffer.len) return error.BufferTooSmall;
        @memcpy(buffer[0..value.len], value);
        return value.len;
    }

    pub fn dump(self: *KvDb, writer: *Io.Writer) !void {
        var entries = std.ArrayList(Entry).empty;
        defer {
            for (entries.items) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
            entries.deinit(self.allocator);
        }

        for (&self.shards) |*shard| {
            shard.lock.lockSharedUncancelable(io);
            var it = shard.map.iterator();
            while (it.next()) |entry| {
                try entries.append(self.allocator, .{
                    .key = try self.allocator.dupe(u8, entry.key_ptr.*),
                    .value = try self.allocator.dupe(u8, entry.value_ptr.*),
                });
            }
            shard.lock.unlockShared(io);
        }

        std.mem.sort(Entry, entries.items, {}, struct {
            fn lessThan(_: void, lhs: Entry, rhs: Entry) bool {
                return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            }
        }.lessThan);

        for (entries.items) |entry| {
            try writer.print("{s}={s}\n", .{ entry.key, entry.value });
        }
    }

    fn replayWal(self: *KvDb) !void {
        var reader_buffer: [4096]u8 = undefined;
        var reader = self.wal_file.readerStreaming(io, &reader_buffer);

        while (true) {
            const maybe_record = try wal.readRecord(self.allocator, &reader.interface);
            const record = maybe_record orelse break;
            defer {
                self.allocator.free(record.key);
                self.allocator.free(record.value);
            }
            try self.applyRecoveredRecord(record);
        }
    }

    fn applyRecoveredRecord(self: *KvDb, record: types.WalRecord) !void {
        const shard = self.shardFor(record.key);
        shard.lock.lockUncancelable(io);
        defer shard.lock.unlock(io);

        switch (record.op) {
            .put => try putOwnedIntoShard(
                self.allocator,
                shard,
                try self.allocator.dupe(u8, record.key),
                try self.allocator.dupe(u8, record.value),
            ),
            .delete => removeFromShard(self.allocator, shard, record.key),
        }
    }

    fn writeLoop(self: *KvDb) void {
        while (true) {
            const request = self.queue.dequeue() orelse break;
            switch (request.op) {
                .put => {
                    const result = self.applyWrite(request.key, request.value);
                    if (result) |_| {
                        request.finish(null);
                    } else |err| {
                        self.allocator.free(request.key);
                        self.allocator.free(request.value);
                        request.finish(err);
                    }
                },
                .delete => {
                    const result = self.applyDelete(request.key);
                    self.allocator.free(request.key);
                    if (result) |_| {
                        request.finish(null);
                    } else |err| {
                        request.finish(err);
                    }
                },
            }
        }
    }

    fn applyWrite(self: *KvDb, key: []u8, value: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .put,
            .key = key,
            .value = value,
        });

        const shard = self.shardFor(key);
        shard.lock.lockUncancelable(io);
        defer shard.lock.unlock(io);
        try putOwnedIntoShard(self.allocator, shard, key, value);
    }

    fn applyDelete(self: *KvDb, key: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .delete,
            .key = key,
            .value = &.{},
        });

        const shard = self.shardFor(key);
        shard.lock.lockUncancelable(io);
        defer shard.lock.unlock(io);
        removeFromShard(self.allocator, shard, key);
    }

    fn shardFor(self: *KvDb, key: []const u8) *Shard {
        const hash = std.hash.Wyhash.hash(0, key);
        return &self.shards[hash % types.shard_count];
    }

    fn deinitShards(self: *KvDb) void {
        for (&self.shards) |*shard| shard.deinit(self.allocator);
    }
};

fn writerMain(db: *KvDb) void {
    db.writeLoop();
}

fn putOwnedIntoShard(allocator: Allocator, shard: *Shard, owned_key: []u8, owned_value: []u8) !void {
    const gop = try shard.map.getOrPut(allocator, owned_key);
    if (gop.found_existing) {
        allocator.free(owned_key);
        allocator.free(gop.value_ptr.*);
    } else {
        gop.key_ptr.* = owned_key;
    }
    gop.value_ptr.* = owned_value;
}

fn removeFromShard(allocator: Allocator, shard: *Shard, key: []const u8) void {
    if (shard.map.fetchRemove(key)) |entry| {
        allocator.free(entry.key);
        allocator.free(entry.value);
    }
}
