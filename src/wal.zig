const std = @import("std");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const io = std.Options.debug_io;

pub fn append(file: *Io.File, record: types.WalRecord) !void {
    var writer_buffer: [4096]u8 = undefined;
    var writer = file.writerStreaming(io, &writer_buffer);

    try writer.interface.writeInt(u32, types.wal_magic, .little);
    try writer.interface.writeByte(@intFromEnum(record.op));
    try writer.interface.writeInt(u32, @intCast(record.key.len), .little);
    try writer.interface.writeInt(u32, @intCast(record.value.len), .little);
    try writer.interface.writeInt(u32, checksum(record), .little);
    try writer.interface.writeAll(record.key);
    try writer.interface.writeAll(record.value);
    try writer.flush();
    try file.sync(io);
}

pub fn readRecord(allocator: Allocator, reader: *Io.Reader) !?types.WalRecord {
    const magic_bytes = reader.takeArray(4) catch |err| switch (err) {
        error.EndOfStream => return null,
        else => |e| return e,
    };
    const magic = std.mem.readInt(u32, magic_bytes, .little);
    if (magic != types.wal_magic) return error.CorruptWal;

    const op_byte = takeByteOrTruncated(reader) orelse return null;
    const op: types.Op = switch (op_byte) {
        @intFromEnum(types.Op.put) => .put,
        @intFromEnum(types.Op.delete) => .delete,
        else => return error.CorruptWal,
    };

    const key_len = try takeU32OrTruncated(reader) orelse return null;
    const value_len = try takeU32OrTruncated(reader) orelse return null;
    const expected_checksum = try takeU32OrTruncated(reader) orelse return null;

    const key = readAllocOrTruncated(allocator, reader, key_len) orelse return null;
    errdefer allocator.free(key);

    const value = readAllocOrTruncated(allocator, reader, value_len) orelse {
        allocator.free(key);
        return null;
    };
    errdefer allocator.free(value);

    const record = types.WalRecord{
        .op = op,
        .key = key,
        .value = value,
    };

    if (checksum(record) != expected_checksum) return error.CorruptWal;
    return record;
}

fn checksum(record: types.WalRecord) u32 {
    var crc: std.hash.Crc32 = .init();
    var op_buf = [1]u8{@intFromEnum(record.op)};
    crc.update(&op_buf);

    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(record.key.len), .little);
    crc.update(&len_buf);
    std.mem.writeInt(u32, &len_buf, @intCast(record.value.len), .little);
    crc.update(&len_buf);

    crc.update(record.key);
    crc.update(record.value);
    return crc.final();
}

fn takeByteOrTruncated(reader: *Io.Reader) ?u8 {
    const bytes = reader.takeArray(1) catch |err| switch (err) {
        error.EndOfStream => return null,
        else => return null,
    };
    return bytes[0];
}

fn takeU32OrTruncated(reader: *Io.Reader) !?u32 {
    const bytes = reader.takeArray(4) catch |err| switch (err) {
        error.EndOfStream => return null,
        else => |e| return e,
    };
    return std.mem.readInt(u32, bytes, .little);
}

fn readAllocOrTruncated(allocator: Allocator, reader: *Io.Reader, len: u32) ?[]u8 {
    return reader.readAlloc(allocator, len) catch |err| switch (err) {
        error.EndOfStream => null,
        else => null,
    };
}
