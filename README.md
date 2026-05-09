# mund_db

`mund_db` is a small persistent key-value store with:

- a single dedicated writer thread
- concurrent readers through shard-level read locks
- an in-memory hashmap rebuilt from a WAL on startup
- a CLI, REPL, tests, and a simple benchmark

## Layout

- `src/main.zig`: thin entrypoint
- `src/cli.zig`: CLI commands and REPL
- `src/bench.zig`: benchmark runner
- `src/db.zig`: core database, sharded map, writer queue
- `src/wal.zig`: WAL encoding/decoding and checksums
- `src/types.zig`: shared record types and constants

## Architecture

```text
                           scaling view

readers scale out horizontally                     writes stay serialized

reader 1 ----\
reader 2 -----+--> hash(key) --> shard[0..63] --> shared read lock --> in-memory map
reader 3 -----+
...           +
reader N ----/

writer clients --> write queue --> 1 writer thread --> append + fsync WAL --> mutate shard
```

## WAL format

Each record is binary and little-endian:

```text
+------------+---------+-----------+-------------+------------+-------+---------+
| magic u32  | op u8   | key_len   | value_len   | crc32 u32  | key   | value   |
+------------+---------+-----------+-------------+------------+-------+---------+
```

Field details:

- `magic`: fixed record marker `0x4d4b5631` (`"MKV1"`)
- `op`: `1 = put`, `2 = delete`
- `key_len`: key byte length
- `value_len`: value byte length
- `crc32`: checksum over `op`, `key_len`, `value_len`, `key`, and `value`
- `key`: raw key bytes
- `value`: raw value bytes, empty for `delete`

Replay behavior:

- the WAL is scanned from the beginning on startup
- valid records rebuild the in-memory shards
- a truncated tail record is ignored
- a bad magic or checksum is treated as corruption

## Concurrency model

Readers never touch the WAL. They hash the key, take a shared lock on exactly one shard, and read from that shard's map.

Writes are serialized by a background writer thread. Each write:

1. enters the queue
2. gets appended to the WAL and synced
3. updates the target shard in memory
4. wakes the caller

That keeps acknowledged writes durable and consistent with crash recovery.

## Build and test

```bash
ZIG_GLOBAL_CACHE_DIR=/private/tmp/zig-cache ZIG_LOCAL_CACHE_DIR=.zig-cache zig build test
```

Tests live in [src/db_test.zig](src/db_test.zig).

## CLI

Set the WAL path once:

```bash
export MUND_DB_WAL=data.wal
```

Then use the CLI:

```bash
zig build run -- put hello world
zig build run -- get hello
zig build run -- delete hello
zig build run -- list
zig build run -- repl
```

## Benchmark

This starts one write-driving client and `n` read-driving clients against the database's single internal writer thread.

```bash
export MUND_DB_WAL=bench.wal
zig build run -- bench 5 8 4096
```

Arguments:

- `5`: seconds
- `8`: reader threads
- `4096`: keyspace size

Example result from a local run on this machine:

```text
seconds=3 readers=4 keyspace=1024 reads=13034931 writes=16754 reads_per_sec=4344977.00 writes_per_sec=5584.67
```

Benchmark machine:

```text
macOS 15.6.1
arm64
Darwin kernel RELEASE_ARM64_T6020
```
