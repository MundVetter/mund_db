pub fn main(init: @import("std").process.Init) !void {
    try @import("cli.zig").main(init);
}
