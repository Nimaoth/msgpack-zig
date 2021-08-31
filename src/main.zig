const std = @import("std");
const time = @import("time");
const yaml = @import("yaml");

usingnamespace @import("writer.zig");
usingnamespace @import("reader.zig");

const String = struct {
    value: []const u8,

    pub fn msgPackWrite(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }

    pub fn msgPackRead(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }
};

const Foo = struct {
    a: i64,
    b: []const u8,
    c: []const bool,
    d: Bar,
    e: String,
};

const Baz = enum {
    Eins,
    Zwei,
    Drei,
};

const Bar = struct {
    lol: f32,
    uiae: bool,
    baz: Baz,
};

pub fn main() anyerror!void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var encodingBuffer = try gpa.allocator.alloc(u8, 1024 * 1024);
    defer gpa.allocator.free(encodingBuffer);
    std.mem.set(u8, encodingBuffer, 0);

    var stream = std.io.fixedBufferStream(encodingBuffer);

    var fileContentBuffer = std.ArrayList(u8).init(&gpa.allocator);
    defer fileContentBuffer.deinit();

    const testInputPath = @import("build_options").TEST_INPUT_PATH;
    std.log.info("{s}", .{testInputPath});

    var testDir = try std.fs.openDirAbsolute(testInputPath, .{ .iterate = true });
    defer testDir.close();

    // iterate over all files
    var iterator = testDir.iterate();
    var i: usize = 0;
    while (try iterator.next()) |entry| {
        defer i += 1;

        if (entry.kind != .File) continue;
        std.log.info("{s}", .{entry.name});

        var file = try testDir.openFile(entry.name, .{ .read = true });
        var reader = file.reader();

        // read entire file
        fileContentBuffer.clearRetainingCapacity();
        try reader.readAllArrayList(&fileContentBuffer, std.math.maxInt(usize));
        //std.debug.print("---\n{s}\n---\n", .{fileContentBuffer.items});

        var parser = std.json.Parser.init(&gpa.allocator, false);
        defer parser.deinit();

        var valueTree: std.json.ValueTree = try parser.parse(fileContentBuffer.items);
        defer valueTree.deinit();

        var arena = std.heap.ArenaAllocator.init(&gpa.allocator);
        defer arena.deinit();

        // iterate over all test cases in one file
        for (valueTree.root.Array.items) |testCaseJson| {
            const testObject = testCaseJson.Object;
            const msgpackEncodingsJson = testObject.get("msgpack").?.Array;

            // list of possible encodings for the value
            var possibleEncodings = try std.ArrayList([]const u8).initCapacity(&arena.allocator, msgpackEncodingsJson.items.len);
            for (msgpackEncodingsJson.items) |encodingJson| {
                var encoding = std.ArrayList(u8).init(&arena.allocator);
                var iter = std.mem.split(encodingJson.String, "-");
                while (iter.next()) |byte| {
                    try encoding.append(try std.fmt.parseInt(u8, byte, 16));
                }
                try possibleEncodings.append(encoding.toOwnedSlice());
            }

            // encode the value from the test
            stream.reset();
            var msgPack = msgPackWriter(stream.writer(), .{});
            if (testObject.get("nil")) |_| {
                try msgPack.writeNil();
            } else if (testObject.get("bool")) |boolJson| {
                try msgPack.writeBool(boolJson.Bool);
            } else if (testObject.get("string")) |stringJson| {
                try msgPack.writeString(stringJson.String);
            } else if (testObject.get("timestamp")) |timestampJson| {
                const timestampArray = timestampJson.Array.items;
                const sec = timestampArray[0].Integer;
                const nsec = timestampArray[1].Integer;
                try msgPack.writeTimestamp(sec, @intCast(u32, nsec));
            }

            const myEncodedData = encodingBuffer[0..try stream.getPos()];

            // check if our encoded data matches any of the encodings in the test case
            var foundMatchingEncoding = false;
            for (possibleEncodings.items) |encoding| {
                if (std.mem.eql(u8, myEncodedData, encoding)) {
                    foundMatchingEncoding = true;
                    break;
                }
            }

            if (foundMatchingEncoding) {} else {
                try testCaseJson.jsonStringify(.{}, std.io.getStdErr().writer());
                std.debug.print("\n", .{});

                //std.log.info("our: {s}", .{myEncodedData});
                //std.log.info("our: {}", .{std.fmt.fmtSliceHexLower(myEncodedData)});
                std.log.err("Encoding failed.", .{});
            }
        }

        //if (i == 1) break;
    }
}

pub fn testStuff(allocator: *std.mem.Allocator) anyerror!void {
    var local = time.Location.getLocal();
    var now = time.now(&local);
    std.log.info("now : {}", .{now});
    std.log.info("date: {}", .{now.date()});
    std.log.info("sec : {}", .{now.unix()});
    std.log.info("nsec: {}", .{now.nanosecond()});

    var encodingBuffer = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(encodingBuffer);
    std.mem.set(u8, encodingBuffer, 0);

    var stream = std.io.fixedBufferStream(encodingBuffer);

    var msgPack = msgPackWriter(stream.writer(), .{
        .writeBytesSliceAsString = true,
        .writeEnumAsString = false,
    });

    var foo = Foo{
        .a = 123,
        .b = "Hello",
        .c = &.{ true, false, false, true, true, false, true, true, false, false },
        .d = Bar{
            .lol = 123.456,
            .uiae = true,
            .baz = .Drei,
        },
        .e = String{ .value = "this is a String" },
    };

    try msgPack.writeTimestamp(now.unix(), @intCast(u32, now.nanosecond()));
    try msgPack.writeTimestamp(now.unix(), 0);

    try msgPack.writeAny(foo);
    try msgPack.writeExt(72, std.mem.asBytes(&foo));
    try msgPack.writeExt(1, &.{0x12});
    try msgPack.writeExt(2, &.{ 0x34, 0x56 });
    try msgPack.writeExt(-1, &.{ 0x78, 0x9a, 0xbc, 0xde });
    try msgPack.beginArray(5);

    try msgPack.beginMap(3);
    try msgPack.writeAny(@as(i64, 0));
    try msgPack.writeAny(@as(i64, 5));
    try msgPack.writeAny(@as(i64, 127));
    try msgPack.writeAny(@as(i64, -1));
    try msgPack.writeAny(@as(i64, -11));
    try msgPack.writeAny(@as(i64, -32));

    try msgPack.writeAny(@as(u8, 123));
    try msgPack.writeAny(@as(i8, -123));

    try msgPack.writeAny(@as(u16, 456));
    try msgPack.writeAny(@as(i16, -789));

    try msgPack.writeAny(@as(f32, 1.2345678987654321));
    try msgPack.writeAny(@as(f64, 1.2345678987654321));

    try msgPack.writeString("hello world");
    try msgPack.writeString("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeString(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBytes("hello world");
    try msgPack.writeBytes("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeBytes(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBool(true);
    try msgPack.writeBool(false);
    try msgPack.writeBool(true);
    try msgPack.writeBool(false);

    std.debug.print("\n", .{});
    const writtenBuffer = buffer[0..try stream.getPos()];
    var i: usize = 0;
    while (true) : (i += 16) {
        if (i >= writtenBuffer.len) break;

        const line = writtenBuffer[i..std.math.min(writtenBuffer.len, i + 16)];

        var k: usize = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{x:0>2} ", .{line[k]});
            } else {
                std.debug.print("   ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{:3} ", .{line[k]});
            } else {
                std.debug.print("    ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k >= line.len or line[k] == 0 or line[k] == 0xa or line[k] == 0xc) {
                std.debug.print(".", .{});
            } else {
                std.debug.print("{c}", .{line[k]});
            }
        }
        if (i > 0 and @mod(i, 16 * 4) == 0) std.debug.print("\n", .{});
        std.debug.print("\n", .{});
    }
    std.debug.print("\n", .{});

    var reader = msgPackReader(std.io.fixedBufferStream(writtenBuffer).reader());
    while (true) {
        reader.read(0) catch break;
    }
}
