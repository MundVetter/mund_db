const std = @import("std");

pub const shard_count: usize = 64;
pub const wal_magic: u32 = 0x4d4b5631; // "MKV1"
pub const max_record_len = std.math.maxInt(u32);

pub const Op = enum(u8) {
    put = 1,
    delete = 2,
};

pub const WalRecord = struct {
    op: Op,
    key: []u8,
    value: []u8,
};
