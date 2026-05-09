const std = @import("std");
const types = @import("types.zig");
const wal = @import("wal.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const io = std.Options.debug_io;

const bucket_count = 64;
const reader_slot_count = 128;
const snapshot_slots = 64;
const max_write_batch = 32;
const no_announcement: u64 = 0;

comptime {
    if (types.shard_count >= 256) @compileError("reader announcement encoding assumes shard_count < 256");
}

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

    fn tryDequeue(self: *WriteQueue) ?*WriteRequest {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        const request = self.head orelse return null;
        self.head = request.next;
        if (self.head == null) self.tail = null;
        request.next = null;
        return request;
    }
};

const Node = struct {
    ref_count: std.atomic.Value(usize) = .init(1),
    key: []u8,
    value: []u8,
    next: ?*Node,

    fn createOwned(allocator: Allocator, key: []u8, value: []u8, next: ?*Node) !*Node {
        const node = try allocator.create(Node);
        node.* = .{ .key = key, .value = value, .next = next };
        return node;
    }

    fn retain(self: *Node) void {
        _ = self.ref_count.fetchAdd(1, .acq_rel);
    }

    fn release(self: *Node, allocator: Allocator) void {
        const previous = self.ref_count.fetchSub(1, .acq_rel);
        if (previous == 1) {
            if (self.next) |next| next.release(allocator);
            allocator.free(self.key);
            allocator.free(self.value);
            allocator.destroy(self);
        }
    }
};

const Snapshot = struct {
    ref_count: std.atomic.Value(usize) = .init(1),
    buckets: [bucket_count]?*Node = [_]?*Node{null} ** bucket_count,

    fn createEmpty(allocator: Allocator) !*Snapshot {
        const snapshot = try allocator.create(Snapshot);
        snapshot.* = .{};
        return snapshot;
    }

    fn release(self: *Snapshot, allocator: Allocator) void {
        const previous = self.ref_count.fetchSub(1, .acq_rel);
        if (previous == 1) {
            for (self.buckets) |head| {
                if (head) |node| node.release(allocator);
            }
            allocator.destroy(self);
        }
    }

    fn get(self: *const Snapshot, key: []const u8) ?[]const u8 {
        var node = self.buckets[bucketIndex(key)];
        while (node) |current| : (node = current.next) {
            if (std.mem.eql(u8, current.key, key)) return current.value;
        }
        return null;
    }

    fn cloneWithPut(self: *const Snapshot, allocator: Allocator, key: []u8, value: []u8) !*Snapshot {
        const snapshot = try allocator.create(Snapshot);
        errdefer allocator.destroy(snapshot);
        snapshot.* = .{};

        const target_idx = bucketIndex(key);
        for (0..bucket_count) |idx| {
            if (idx == target_idx) continue;
            snapshot.buckets[idx] = self.buckets[idx];
            if (snapshot.buckets[idx]) |head| head.retain();
        }

        snapshot.buckets[target_idx] = try cloneBucketForPut(allocator, self.buckets[target_idx], key, value);
        return snapshot;
    }

    fn cloneWithDelete(self: *const Snapshot, allocator: Allocator, key: []const u8) !*Snapshot {
        const snapshot = try allocator.create(Snapshot);
        errdefer allocator.destroy(snapshot);
        snapshot.* = .{};

        const target_idx = bucketIndex(key);
        for (0..bucket_count) |idx| {
            if (idx == target_idx) continue;
            snapshot.buckets[idx] = self.buckets[idx];
            if (snapshot.buckets[idx]) |head| head.retain();
        }

        snapshot.buckets[target_idx] = try cloneBucketForDelete(allocator, self.buckets[target_idx], key);
        return snapshot;
    }
};

const PinnedSnapshot = struct {
    reader_slot: *ReaderSlot,
    snapshot: *Snapshot,
};

const ReaderSlot = struct {
    owner: std.atomic.Value(u64) = .init(0),
    announcement: std.atomic.Value(u64) = .init(no_announcement),
    padding: [48]u8 = [_]u8{0} ** 48,
};

const ReaderRegistry = struct {
    slots: [reader_slot_count]ReaderSlot = [_]ReaderSlot{ReaderSlot{}} ** reader_slot_count,

    fn acquire(self: *ReaderRegistry, db: *KvDb) *ReaderSlot {
        const owner = threadOwnerId();
        if (registered_reader) |registration| {
            if (registration.db == db) {
                return &self.slots[registration.slot];
            }

            const previous_slot = &registration.db.reader_registry.slots[registration.slot];
            previous_slot.announcement.store(no_announcement, .release);
            previous_slot.owner.store(0, .release);
            registered_reader = null;
        }

        var empty_slot: ?usize = null;
        for (&self.slots, 0..) |*slot, idx| {
            const slot_owner = slot.owner.load(.acquire);
            if (slot_owner == owner) {
                registered_reader = .{ .db = db, .slot = idx };
                return slot;
            }
            if (slot_owner == 0 and empty_slot == null) empty_slot = idx;
        }

        const idx = empty_slot orelse @panic("reader slot registry exhausted");
        const slot = &self.slots[idx];
        const claimed = slot.owner.cmpxchgStrong(0, owner, .acq_rel, .acquire);
        if (claimed != null) return self.acquire(db);

        registered_reader = .{ .db = db, .slot = idx };
        return slot;
    }

    fn isQuiescent(self: *const ReaderRegistry, announcement: u64) bool {
        for (self.slots) |slot| {
            if (slot.announcement.load(.acquire) == announcement) return false;
        }
        return true;
    }
};

const RegisteredReader = struct {
    db: *KvDb,
    slot: usize,
};

threadlocal var registered_reader: ?RegisteredReader = null;

const Shard = struct {
    active_slot: std.atomic.Value(usize) = .init(0),
    active_generation: std.atomic.Value(u64) = .init(1),
    next_generation: std.atomic.Value(u64) = .init(2),
    slot_generations: [snapshot_slots]u64 = [_]u64{0} ** snapshot_slots,
    snapshots: [snapshot_slots]?*Snapshot = [_]?*Snapshot{null} ** snapshot_slots,

    fn init(allocator: Allocator) !Shard {
        var shard: Shard = .{};
        shard.snapshots[0] = try Snapshot.createEmpty(allocator);
        shard.slot_generations[0] = 1;
        return shard;
    }

    fn deinit(self: *Shard, allocator: Allocator) void {
        for (self.snapshots) |snapshot| {
            if (snapshot) |ptr| ptr.release(allocator);
        }
        self.* = undefined;
    }

    fn pinCurrent(self: *Shard, db: *KvDb, shard_index: usize) PinnedSnapshot {
        const reader_slot = db.reader_registry.acquire(db);
        while (true) {
            const slot = self.active_slot.load(.acquire);
            const generation = self.active_generation.load(.acquire);
            reader_slot.announcement.store(encodeAnnouncement(shard_index, generation), .release);
            if (self.active_slot.load(.acquire) == slot and self.active_generation.load(.acquire) == generation) {
                return .{ .reader_slot = reader_slot, .snapshot = self.snapshots[slot].? };
            }
            reader_slot.announcement.store(no_announcement, .release);
        }
    }

    fn unpin(self: *Shard, pinned: PinnedSnapshot) void {
        _ = self;
        pinned.reader_slot.announcement.store(no_announcement, .release);
    }

    fn currentSnapshot(self: *Shard) *Snapshot {
        return self.snapshots[self.active_slot.load(.acquire)].?;
    }

    fn acquirePublishSlot(self: *Shard, db: *KvDb, shard_index: usize, allocator: Allocator) usize {
        while (true) {
            const active = self.active_slot.load(.acquire);
            for (0..snapshot_slots) |slot| {
                if (slot == active) continue;
                const generation = self.slot_generations[slot];
                if (generation != 0 and !db.reader_registry.isQuiescent(encodeAnnouncement(shard_index, generation))) {
                    continue;
                }
                if (self.snapshots[slot]) |snapshot| {
                    self.snapshots[slot] = null;
                    self.slot_generations[slot] = 0;
                    snapshot.release(allocator);
                }
                return slot;
            }
            std.Thread.yield() catch {};
        }
    }

    fn publish(self: *Shard, slot: usize, next: *Snapshot) void {
        const generation = self.next_generation.fetchAdd(1, .acq_rel);
        self.snapshots[slot] = next;
        self.slot_generations[slot] = generation;
        self.active_slot.store(slot, .release);
        self.active_generation.store(generation, .release);
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
    reader_registry: ReaderRegistry = .{},
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

        for (&db.shards) |*shard| shard.* = try Shard.init(allocator);

        try db.replayWal();
        db.writer_thread = try std.Thread.spawn(.{}, writerMain, .{db});
        return db;
    }

    pub fn close(self: *KvDb) void {
        self.queue.close();
        if (self.writer_thread) |thread| thread.join();
        if (registered_reader) |registration| {
            if (registration.db == self) {
                const slot = &self.reader_registry.slots[registration.slot];
                slot.announcement.store(no_announcement, .release);
                slot.owner.store(0, .release);
                registered_reader = null;
            }
        }
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
        const shard_idx = self.shardIndexFor(key);
        const shard = &self.shards[shard_idx];
        const pinned = shard.pinCurrent(self, shard_idx);
        defer shard.unpin(pinned);

        const value = pinned.snapshot.get(key) orelse return null;
        return try allocator.dupe(u8, value);
    }

    pub fn getInto(self: *KvDb, key: []const u8, buffer: []u8) !?usize {
        const shard_idx = self.shardIndexFor(key);
        const shard = &self.shards[shard_idx];
        const pinned = shard.pinCurrent(self, shard_idx);
        defer shard.unpin(pinned);

        const value = pinned.snapshot.get(key) orelse return null;
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

        for (&self.shards, 0..) |*shard, idx| {
            const pinned = shard.pinCurrent(self, idx);
            defer shard.unpin(pinned);

            for (pinned.snapshot.buckets) |head| {
                var node = head;
                while (node) |current| : (node = current.next) {
                    try entries.append(self.allocator, .{
                        .key = try self.allocator.dupe(u8, current.key),
                        .value = try self.allocator.dupe(u8, current.value),
                    });
                }
            }
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
        const shard_idx = self.shardIndexFor(record.key);
        const shard = &self.shards[shard_idx];
        const current = shard.currentSnapshot();
        const next = switch (record.op) {
            .put => try current.cloneWithPut(
                self.allocator,
                try self.allocator.dupe(u8, record.key),
                try self.allocator.dupe(u8, record.value),
            ),
            .delete => try current.cloneWithDelete(self.allocator, record.key),
        };
        const slot = shard.acquirePublishSlot(self, shard_idx, self.allocator);
        shard.publish(slot, next);
    }

    fn writeLoop(self: *KvDb) void {
        var batch: [max_write_batch]*WriteRequest = undefined;

        while (true) {
            const first = self.queue.dequeue() orelse break;
            batch[0] = first;

            var batch_len: usize = 1;
            while (batch_len < max_write_batch) {
                const next = self.queue.tryDequeue() orelse break;
                batch[batch_len] = next;
                batch_len += 1;
            }

            self.applyBatch(batch[0..batch_len]);
        }
    }

    fn applyBatch(self: *KvDb, requests: []const *WriteRequest) void {
        var pending: [types.shard_count]?*Snapshot = [_]?*Snapshot{null} ** types.shard_count;
        var prepared_count: usize = 0;
        var failure: ?anyerror = null;

        defer {
            for (pending) |snapshot| {
                if (snapshot) |ptr| ptr.release(self.allocator);
            }
        }

        for (requests) |request| {
            const result = switch (request.op) {
                .put => self.prepareBatchPut(&pending, request.key, request.value),
                .delete => self.prepareBatchDelete(&pending, request.key),
            };

            if (result) |_| {
                prepared_count += 1;
            } else |err| {
                failure = err;
                break;
            }
        }

        for (&pending, 0..) |*maybe_snapshot, shard_idx| {
            const snapshot = maybe_snapshot.* orelse continue;
            const slot = self.shards[shard_idx].acquirePublishSlot(self, shard_idx, self.allocator);
            self.shards[shard_idx].publish(slot, snapshot);
            maybe_snapshot.* = null;
        }

        for (requests[0..prepared_count]) |request| {
            self.finishSuccessfulRequest(request);
        }

        if (failure) |err| {
            for (requests[prepared_count..]) |request| {
                self.finishFailedRequest(request, err);
            }
        }
    }

    fn prepareBatchPut(self: *KvDb, pending: *[types.shard_count]?*Snapshot, key: []u8, value: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .put,
            .key = key,
            .value = value,
        });

        const shard_idx = self.shardIndexFor(key);
        const base = pending[shard_idx] orelse self.shards[shard_idx].currentSnapshot();
        const next = try base.cloneWithPut(self.allocator, key, value);
        if (pending[shard_idx]) |previous| previous.release(self.allocator);
        pending[shard_idx] = next;
    }

    fn prepareBatchDelete(self: *KvDb, pending: *[types.shard_count]?*Snapshot, key: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .delete,
            .key = key,
            .value = &.{},
        });

        const shard_idx = self.shardIndexFor(key);
        const base = pending[shard_idx] orelse self.shards[shard_idx].currentSnapshot();
        const next = try base.cloneWithDelete(self.allocator, key);
        if (pending[shard_idx]) |previous| previous.release(self.allocator);
        pending[shard_idx] = next;
    }

    fn finishSuccessfulRequest(self: *KvDb, request: *WriteRequest) void {
        switch (request.op) {
            .put => {},
            .delete => self.allocator.free(request.key),
        }
        request.finish(null);
    }

    fn finishFailedRequest(self: *KvDb, request: *WriteRequest, err: anyerror) void {
        self.allocator.free(request.key);
        if (request.op == .put) self.allocator.free(request.value);
        request.finish(err);
    }

    fn applyWrite(self: *KvDb, key: []u8, value: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .put,
            .key = key,
            .value = value,
        });

        const shard_idx = self.shardIndexFor(key);
        const shard = &self.shards[shard_idx];
        const current = shard.currentSnapshot();
        const next = try current.cloneWithPut(self.allocator, key, value);
        const slot = shard.acquirePublishSlot(self, shard_idx, self.allocator);
        shard.publish(slot, next);
    }

    fn applyDelete(self: *KvDb, key: []u8) !void {
        try wal.append(&self.wal_file, .{
            .op = .delete,
            .key = key,
            .value = &.{},
        });

        const shard_idx = self.shardIndexFor(key);
        const shard = &self.shards[shard_idx];
        const current = shard.currentSnapshot();
        const next = try current.cloneWithDelete(self.allocator, key);
        const slot = shard.acquirePublishSlot(self, shard_idx, self.allocator);
        shard.publish(slot, next);
    }

    fn shardIndexFor(self: *KvDb, key: []const u8) usize {
        _ = self;
        const hash = std.hash.Wyhash.hash(0, key);
        return hash % types.shard_count;
    }

    fn deinitShards(self: *KvDb) void {
        for (&self.shards) |*shard| shard.deinit(self.allocator);
    }
};

fn writerMain(db: *KvDb) void {
    db.writeLoop();
}

fn bucketIndex(key: []const u8) usize {
    return std.hash.Wyhash.hash(1, key) % bucket_count;
}

fn encodeAnnouncement(shard_index: usize, generation: u64) u64 {
    return (generation << 8) | @as(u64, @intCast(shard_index + 1));
}

fn threadOwnerId() u64 {
    return std.hash.Wyhash.hash(3, std.mem.asBytes(&std.Thread.getCurrentId())) | 1;
}

fn cloneBucketForPut(allocator: Allocator, head: ?*Node, key: []u8, value: []u8) !?*Node {
    if (head == null) return try Node.createOwned(allocator, key, value, null);

    const current = head.?;
    if (std.mem.eql(u8, current.key, key)) {
        if (current.next) |next| next.retain();
        return try Node.createOwned(allocator, key, value, current.next);
    }

    const next = try cloneBucketForPut(allocator, current.next, key, value);
    const cloned_key = try allocator.dupe(u8, current.key);
    errdefer allocator.free(cloned_key);
    const cloned_value = try allocator.dupe(u8, current.value);
    errdefer allocator.free(cloned_value);
    return try Node.createOwned(allocator, cloned_key, cloned_value, next);
}

fn cloneBucketForDelete(allocator: Allocator, head: ?*Node, key: []const u8) !?*Node {
    const current = head orelse return null;

    if (std.mem.eql(u8, current.key, key)) {
        if (current.next) |next| next.retain();
        return current.next;
    }

    const next = try cloneBucketForDelete(allocator, current.next, key);
    const cloned_key = try allocator.dupe(u8, current.key);
    errdefer allocator.free(cloned_key);
    const cloned_value = try allocator.dupe(u8, current.value);
    errdefer allocator.free(cloned_value);
    return try Node.createOwned(allocator, cloned_key, cloned_value, next);
}
