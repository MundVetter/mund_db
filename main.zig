const impl = @import("src/main.zig");

pub fn main(init: @import("std").process.Init) !void {
    try impl.main(init);
}
