const std = @import("std");
const rw = @import("db_rwlock.zig");
const cow = @import("db_cow.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const Backend = enum {
    rwlock,
    cow,
};

pub fn parseBackend(name: []const u8) !Backend {
    if (std.mem.eql(u8, name, "rwlock")) return .rwlock;
    if (std.mem.eql(u8, name, "cow")) return .cow;
    return error.InvalidBackend;
}

pub fn backendName(backend: Backend) []const u8 {
    return @tagName(backend);
}

pub const Entry = rw.Entry;

pub const KvDb = struct {
    allocator: Allocator,
    backend: Backend,
    inner: union(Backend) {
        rwlock: *rw.KvDb,
        cow: *cow.KvDb,
    },

    pub fn openPath(allocator: Allocator, wal_path: []const u8, backend: Backend) !*KvDb {
        return openInDir(allocator, Io.Dir.cwd(), wal_path, backend);
    }

    pub fn openInDir(allocator: Allocator, dir: Io.Dir, wal_path: []const u8, backend: Backend) !*KvDb {
        const db = try allocator.create(KvDb);
        errdefer allocator.destroy(db);

        db.* = .{
            .allocator = allocator,
            .backend = backend,
            .inner = switch (backend) {
                .rwlock => .{ .rwlock = try rw.KvDb.openInDir(allocator, dir, wal_path) },
                .cow => .{ .cow = try cow.KvDb.openInDir(allocator, dir, wal_path) },
            },
        };
        return db;
    }

    pub fn close(self: *KvDb) void {
        switch (self.inner) {
            .rwlock => |impl| impl.close(),
            .cow => |impl| impl.close(),
        }
        self.allocator.destroy(self);
    }

    pub fn put(self: *KvDb, key: []const u8, value: []const u8) !void {
        switch (self.inner) {
            .rwlock => |impl| try impl.put(key, value),
            .cow => |impl| try impl.put(key, value),
        }
    }

    pub fn delete(self: *KvDb, key: []const u8) !void {
        switch (self.inner) {
            .rwlock => |impl| try impl.delete(key),
            .cow => |impl| try impl.delete(key),
        }
    }

    pub fn get(self: *KvDb, allocator: Allocator, key: []const u8) !?[]u8 {
        return switch (self.inner) {
            .rwlock => |impl| try impl.get(allocator, key),
            .cow => |impl| try impl.get(allocator, key),
        };
    }

    pub fn getInto(self: *KvDb, key: []const u8, buffer: []u8) !?usize {
        return switch (self.inner) {
            .rwlock => |impl| try impl.getInto(key, buffer),
            .cow => |impl| try impl.getInto(key, buffer),
        };
    }

    pub fn dump(self: *KvDb, writer: *Io.Writer) !void {
        switch (self.inner) {
            .rwlock => |impl| try impl.dump(writer),
            .cow => |impl| try impl.dump(writer),
        }
    }
};
